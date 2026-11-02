//! A SQLite database file assembled page by page, and the zstd encoding of backups.

use std::io::SeekFrom;

use anyhow::{Context, Result};
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use async_compression::Level;
use bytes::Bytes;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufReader};

use crate::frame::{Frame, Pages, PAGE_SIZE};
use crate::store::Store;

pub struct DbFile {
    file: File,
    // keeps the temporary file alive, it is removed on drop
    _temp: tempfile::TempPath,
}

impl DbFile {
    /// A database in a temporary file, removed when dropped.
    pub fn temp() -> Result<Self> {
        let temp = tempfile::NamedTempFile::new().context("creating temporary file")?;
        let (file, path) = temp.into_parts();
        Ok(Self {
            file: File::from_std(file),
            _temp: path,
        })
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        let offset = (u64::from(frame.page_no()) - 1) * PAGE_SIZE as u64;
        self.file.seek(SeekFrom::Start(offset)).await?;
        self.file.write_all(frame.page()).await?;
        Ok(())
    }

    pub async fn write_pages(&mut self, pages: &Pages) -> Result<()> {
        for frame in pages.frames() {
            self.write_frame(frame).await?;
        }
        Ok(())
    }

    /// Cuts the database to `size_after` pages, as SQLite does when it commits.
    pub async fn truncate(&mut self, size_after: u32) -> Result<()> {
        self.file
            .set_len(u64::from(size_after) * PAGE_SIZE as u64)
            .await?;
        Ok(())
    }

    /// Replaces the content with the zstd-compressed object `key`.
    pub async fn load_compressed(&mut self, store: &Store, key: &str) -> Result<()> {
        self.file.set_len(0).await?;
        self.file.seek(SeekFrom::Start(0)).await?;
        let mut decoder = async_compression::tokio::write::ZstdDecoder::new(&mut self.file);
        store.download(key, &mut decoder).await?;
        decoder.shutdown().await?;
        Ok(())
    }

    /// Uploads the database compressed with zstd. Returns the compressed size.
    pub async fn upload_compressed(&mut self, store: &Store, key: &str, level: i32) -> Result<u64> {
        self.file.flush().await?;
        self.file.seek(SeekFrom::Start(0)).await?;
        let file = self.file.try_clone().await?;
        let encoder = ZstdEncoder::with_quality(BufReader::new(file), Level::Precise(level));
        let mut counted = CountingReader::new(encoder);
        store.upload(key, &mut counted).await?;
        Ok(counted.count)
    }
}

pub async fn compress(data: &[u8], level: i32) -> Result<Bytes> {
    let mut out = Vec::new();
    ZstdEncoder::with_quality(data, Level::Precise(level))
        .read_to_end(&mut out)
        .await?;
    Ok(out.into())
}

pub async fn decompress(data: &[u8]) -> Result<Bytes> {
    let mut out = Vec::new();
    ZstdDecoder::new(data)
        .read_to_end(&mut out)
        .await
        .context("invalid zstd data")?;
    Ok(out.into())
}

/// Counts the bytes read through it.
struct CountingReader<R> {
    inner: R,
    count: u64,
}

impl<R> CountingReader<R> {
    fn new(inner: R) -> Self {
        Self { inner, count: 0 }
    }
}

impl<R: tokio::io::AsyncRead + Unpin> tokio::io::AsyncRead for CountingReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        let res = std::pin::Pin::new(&mut self.inner).poll_read(cx, buf);
        self.count += (buf.filled().len() - before) as u64;
        res
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::frame::test::frame;

    #[tokio::test]
    async fn zstd_roundtrip() {
        let data = vec![7u8; 100_000];
        let compressed = compress(&data, 3).await.unwrap();
        assert!(compressed.len() < data.len() / 10);
        assert_eq!(decompress(&compressed).await.unwrap(), data);
    }

    #[tokio::test]
    async fn pages_land_at_their_offset() {
        let mut db = DbFile::temp().unwrap();
        db.write_frame(&frame(1, 3, 0, 3)).await.unwrap();
        db.write_frame(&frame(2, 1, 3, 1)).await.unwrap();
        db.truncate(2).await.unwrap();
        db.file.seek(SeekFrom::Start(0)).await.unwrap();
        let mut content = Vec::new();
        db.file.read_to_end(&mut content).await.unwrap();
        assert_eq!(content.len(), 2 * PAGE_SIZE);
        assert!(content[..PAGE_SIZE].iter().all(|b| *b == 1));
        assert!(content[PAGE_SIZE..].iter().all(|b| *b == 0));
    }
}
