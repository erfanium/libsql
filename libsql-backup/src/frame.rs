//! Replication frames as sqld sends them: a 24 byte little-endian header followed by one page.
//! Mirrors `libsql_replication::frame::FrameHeader`.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use bytes::Bytes;

pub const PAGE_SIZE: usize = 4096;
pub const FRAME_HEADER_SIZE: usize = 24;
pub const FRAME_SIZE: usize = FRAME_HEADER_SIZE + PAGE_SIZE;

#[derive(Clone)]
pub struct Frame {
    bytes: Bytes,
}

impl Frame {
    pub fn new(bytes: Bytes) -> Result<Self> {
        if bytes.len() != FRAME_SIZE {
            bail!(
                "invalid frame size: {} bytes, expected {FRAME_SIZE}",
                bytes.len()
            );
        }
        let frame = Self { bytes };
        if frame.page_no() == 0 {
            bail!("invalid frame: page number 0");
        }
        Ok(frame)
    }

    pub fn frame_no(&self) -> u64 {
        u64::from_le_bytes(self.bytes[0..8].try_into().unwrap())
    }

    pub fn page_no(&self) -> u32 {
        u32::from_le_bytes(self.bytes[16..20].try_into().unwrap())
    }

    /// Size of the database in pages after the transaction, non-zero only on commit frames.
    pub fn size_after(&self) -> u32 {
        u32::from_le_bytes(self.bytes[20..24].try_into().unwrap())
    }

    pub fn page(&self) -> &[u8] {
        &self.bytes[FRAME_HEADER_SIZE..]
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl std::fmt::Debug for Frame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Frame")
            .field("frame_no", &self.frame_no())
            .field("page_no", &self.page_no())
            .field("size_after", &self.size_after())
            .finish()
    }
}

/// The newest version of each page: a later insert of a page replaces the earlier one.
#[derive(Default, Debug, Clone)]
pub struct Pages {
    pages: BTreeMap<u32, Frame>,
}

impl Pages {
    pub fn insert(&mut self, frame: Frame) {
        self.pages.insert(frame.page_no(), frame);
    }

    /// Adds `newer` on top of these pages.
    pub fn merge(&mut self, newer: Pages) {
        self.pages.extend(newer.pages);
    }

    pub fn len(&self) -> usize {
        self.pages.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    /// Number of pages after merging `other` into these.
    pub fn union_len(&self, other: &Pages) -> usize {
        self.len()
            + other
                .pages
                .keys()
                .filter(|p| !self.pages.contains_key(p))
                .count()
    }

    pub fn frames(&self) -> impl Iterator<Item = &Frame> {
        self.pages.values()
    }

    /// Splits into chunks of at most `max` pages, in page order.
    pub fn split(self, max: usize) -> Vec<Pages> {
        assert!(max > 0);
        let mut out = Vec::new();
        let mut current = Pages::default();
        for (no, frame) in self.pages {
            if current.len() == max {
                out.push(std::mem::take(&mut current));
            }
            current.pages.insert(no, frame);
        }
        if !current.is_empty() {
            out.push(current);
        }
        out
    }

    /// Serializes as concatenated raw frames, in page order.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.len() * FRAME_SIZE);
        for frame in self.frames() {
            out.extend_from_slice(frame.as_bytes());
        }
        out
    }

    pub fn decode(bytes: Bytes) -> Result<Pages> {
        if bytes.len() % FRAME_SIZE != 0 {
            bail!(
                "invalid segment: {} bytes is not a multiple of the frame size",
                bytes.len()
            );
        }
        let mut pages = Pages::default();
        let mut offset = 0;
        while offset < bytes.len() {
            pages.insert(Frame::new(bytes.slice(offset..offset + FRAME_SIZE))?);
            offset += FRAME_SIZE;
        }
        Ok(pages)
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;

    pub fn frame(frame_no: u64, page_no: u32, size_after: u32, fill: u8) -> Frame {
        let mut bytes = Vec::with_capacity(FRAME_SIZE);
        bytes.extend_from_slice(&frame_no.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&page_no.to_le_bytes());
        bytes.extend_from_slice(&size_after.to_le_bytes());
        bytes.resize(FRAME_SIZE, fill);
        Frame::new(bytes.into()).unwrap()
    }

    #[test]
    fn header_fields() {
        let f = frame(42, 7, 9, 0xab);
        assert_eq!(f.frame_no(), 42);
        assert_eq!(f.page_no(), 7);
        assert_eq!(f.size_after(), 9);
        assert!(f.page().iter().all(|b| *b == 0xab));
    }

    #[test]
    fn matches_libsql_replication_layout() {
        use libsql_replication::frame::{FrameHeader, FrameMut};
        let f = frame(42, 7, 9, 1);
        let theirs = FrameMut::try_from(f.as_bytes()).unwrap();
        let header: &FrameHeader = theirs.header();
        assert_eq!(header.frame_no.get(), 42);
        assert_eq!(header.page_no.get(), 7);
        assert_eq!(header.size_after.get(), 9);
    }

    #[test]
    fn later_page_wins_and_roundtrips() {
        let mut pages = Pages::default();
        pages.insert(frame(1, 2, 0, 1));
        pages.insert(frame(2, 1, 0, 1));
        pages.insert(frame(3, 2, 3, 2));
        assert_eq!(pages.len(), 2);
        let decoded = Pages::decode(pages.encode().into()).unwrap();
        let frames: Vec<_> = decoded
            .frames()
            .map(|f| (f.page_no(), f.frame_no()))
            .collect();
        assert_eq!(frames, vec![(1, 2), (2, 3)]);
    }

    #[test]
    fn union_and_split() {
        let mut a = Pages::default();
        a.insert(frame(1, 1, 0, 0));
        a.insert(frame(2, 2, 0, 0));
        let mut b = Pages::default();
        b.insert(frame(3, 2, 0, 0));
        b.insert(frame(4, 3, 0, 0));
        assert_eq!(a.union_len(&b), 3);
        a.merge(b);
        let parts = a.split(2);
        assert_eq!(parts.iter().map(Pages::len).collect::<Vec<_>>(), vec![2, 1]);
    }
}
