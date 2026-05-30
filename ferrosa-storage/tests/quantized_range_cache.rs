use std::sync::Arc;

use bytes::Bytes;
use ferrosa_storage::quantized_range_cache::{ObjectRangePageStore, QuantizedArtifactManifest};
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::ObjectStore;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

fn fixture_bytes(len: usize) -> Bytes {
    Bytes::from((0..len).map(|n| (n % 251) as u8).collect::<Vec<_>>())
}

#[test]
fn quantized_range_cache_smaller_than_index_reads_only_needed_object_ranges() {
    runtime().block_on(async {
        let object = ObjectPath::from("sstables/ks.tbl/gen-7-vec.qvec");
        let bytes = fixture_bytes(192);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store.put(&object, bytes.clone().into()).await.unwrap();

        let cache_dir = tempfile::tempdir().unwrap();
        let manifest = QuantizedArtifactManifest::new(object.clone(), bytes.len() as u64, 64);
        let page_store = ObjectRangePageStore::new(store, cache_dir.path().to_path_buf(), 64)
            .expect("create bounded page cache");

        let page0 = page_store.read_page(&manifest, 0).await.unwrap();
        let page2 = page_store.read_page(&manifest, 2).await.unwrap();

        assert_eq!(page0.as_ref(), &bytes[0..64]);
        assert_eq!(page2.as_ref(), &bytes[128..192]);
        assert_eq!(page_store.object_range_reads(), 2);
        assert_eq!(page_store.object_bytes_read(), 128);
        assert!(
            page_store.cache_bytes() <= 64,
            "bounded cache must stay smaller than the 192-byte index"
        );
    });
}

#[test]
fn quantized_range_cache_evicted_or_deleted_pages_rehydrate_from_object_ranges() {
    runtime().block_on(async {
        let object = ObjectPath::from("sstables/ks.tbl/gen-8-vec.qvec");
        let bytes = fixture_bytes(160);
        let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        store.put(&object, bytes.clone().into()).await.unwrap();

        let cache_dir = tempfile::tempdir().unwrap();
        let manifest = QuantizedArtifactManifest::new(object, bytes.len() as u64, 64);
        let page_store = ObjectRangePageStore::new(store, cache_dir.path().to_path_buf(), 96)
            .expect("create bounded page cache");

        let page1 = page_store.read_page(&manifest, 1).await.unwrap();
        assert_eq!(page1.as_ref(), &bytes[64..128]);
        assert_eq!(page_store.object_range_reads(), 1);

        page_store
            .delete_cached_page_for_test(&manifest, 1)
            .unwrap();

        let rehydrated = page_store.read_page(&manifest, 1).await.unwrap();
        assert_eq!(rehydrated.as_ref(), &bytes[64..128]);
        assert_eq!(page_store.object_range_reads(), 2);
        assert!(page_store.cache_bytes() <= 96);
    });
}
