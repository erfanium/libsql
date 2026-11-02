//! Test only: rebuilds the database a backup holds, to compare it with sqld's.

use std::path::Path;

use libsql_backup::dbfile::decompress;
use libsql_backup::frame::{Pages, PAGE_SIZE};
use libsql_backup::status;
use libsql_backup::store::Store;

pub async fn rebuild(store: &Store, namespace: &str, out: &Path) {
    let st = status::read(store, namespace)
        .await
        .unwrap()
        .expect("no backup")
        .status;
    let get = |key: String| async move {
        let (bytes, _) = store.get(&key).await.unwrap().expect("missing object");
        decompress(&bytes).await.unwrap()
    };

    let mut db = get(st.snapshot.key.clone()).await.to_vec();
    db.resize(st.snapshot.size_after as usize * PAGE_SIZE, 0);
    for entry in &st.wal {
        let pages = Pages::decode(get(entry.key.clone()).await).unwrap();
        for frame in pages.frames() {
            let offset = (frame.page_no() as usize - 1) * PAGE_SIZE;
            if db.len() < offset + PAGE_SIZE {
                db.resize(offset + PAGE_SIZE, 0);
            }
            db[offset..offset + PAGE_SIZE].copy_from_slice(frame.page());
        }
        db.resize(entry.size_after as usize * PAGE_SIZE, 0);
    }
    db.resize(st.size_after as usize * PAGE_SIZE, 0);
    std::fs::write(out, db).unwrap();

    let conn = rusqlite::Connection::open(out).unwrap();
    let check: String = conn
        .query_row("PRAGMA integrity_check", [], |row| row.get(0))
        .unwrap();
    assert_eq!(check, "ok");
}
