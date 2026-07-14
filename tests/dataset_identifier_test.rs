use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use timslite::{Store, StoreConfig, TmslError};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_dir(name: &str) -> PathBuf {
    let id = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!(
        "timslite_dataset_identifier_{name}_{id}_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&dir);
    dir
}

fn store_config() -> StoreConfig {
    StoreConfig::builder()
        .enable_background_thread(false)
        .enable_journal(false)
        .build()
}

fn dataset_identifier_path(root: &Path, name: &str, dataset_type: &str) -> PathBuf {
    root.join(name).join(dataset_type).join("identifier")
}

#[test]
fn dataset_identifier_create_reopen_and_open_by_id() {
    let dir = temp_dir("open_by_id");
    {
        let mut store = Store::open(&dir, store_config()).unwrap();
        let first = store
            .create_dataset_with_config("alpha", "data", None)
            .unwrap();
        let second = store
            .create_dataset_with_config("beta", "data", None)
            .unwrap();

        assert_eq!(first.identifier(), 1);
        assert_eq!(second.identifier(), 2);
        assert_eq!(
            fs::read_to_string(dir.join("max_identifier"))
                .unwrap()
                .trim(),
            "2"
        );
        assert_eq!(
            fs::read_to_string(dataset_identifier_path(&dir, "alpha", "data"))
                .unwrap()
                .trim(),
            "1"
        );
        first.write(1, b"alpha").unwrap();
        second.write(1, b"beta").unwrap();
        store.close().unwrap();
    }

    {
        let mut store = Store::open(&dir, store_config()).unwrap();
        let alpha = store.open_dataset_by_identifier(1).unwrap();
        let beta = store.open_dataset_by_identifier(2).unwrap();

        assert_eq!(alpha.identifier(), 1);
        assert_eq!(beta.identifier(), 2);
        assert_eq!(alpha.read(1).unwrap().unwrap().1, b"alpha");
        assert_eq!(beta.read(1).unwrap().unwrap().1, b"beta");
    }
}

#[test]
fn dataset_identifier_rename_preserves_identifier_and_updates_cached_lookup() {
    let dir = temp_dir("rename_preserves_identifier");
    let mut store = Store::open(&dir, store_config()).unwrap();
    let dataset = store
        .create_dataset_with_config("alpha", "data", None)
        .unwrap();
    dataset.write(1, b"alpha").unwrap();
    let identifier = dataset.identifier();

    let cached = store.open_dataset_by_identifier(identifier).unwrap();
    assert_eq!(cached.read(1).unwrap().unwrap().1, b"alpha");

    store
        .rename_dataset("alpha", "data", "beta", "metrics")
        .unwrap();

    assert!(!dataset_identifier_path(&dir, "alpha", "data").exists());
    assert_eq!(
        fs::read_to_string(dataset_identifier_path(&dir, "beta", "metrics"))
            .unwrap()
            .trim(),
        identifier.to_string()
    );
    assert_eq!(
        fs::read_to_string(dir.join("max_identifier"))
            .unwrap()
            .trim(),
        "1"
    );
    assert!(store.open_dataset("alpha", "data").is_err());

    let reopened = store.open_dataset_by_identifier(identifier).unwrap();
    assert_eq!(reopened.identifier(), identifier);
    assert_eq!(reopened.read(1).unwrap().unwrap().1, b"alpha");
}

#[test]
fn dataset_identifier_treats_lagging_max_identifier_as_authoritative() {
    let dir = temp_dir("lagging_max_authoritative");
    {
        let mut store = Store::open(&dir, store_config()).unwrap();
        store
            .create_dataset_with_config("alpha", "data", None)
            .unwrap();
    }

    fs::write(dir.join("max_identifier"), "0").unwrap();

    let mut store = Store::open(&dir, store_config()).unwrap();
    assert!(
        store.open_dataset("alpha", "data").is_err(),
        "dataset identifier greater than authoritative max_identifier should be rejected"
    );
    assert_eq!(
        fs::read_to_string(dir.join("max_identifier"))
            .unwrap()
            .trim(),
        "0"
    );
}

#[test]
fn dataset_identifier_rejects_duplicate_identifier_when_opening_by_id() {
    let dir = temp_dir("duplicate");
    {
        let mut store = Store::open(&dir, store_config()).unwrap();
        store
            .create_dataset_with_config("alpha", "data", None)
            .unwrap();
        store
            .create_dataset_with_config("beta", "data", None)
            .unwrap();
    }

    fs::write(dataset_identifier_path(&dir, "beta", "data"), "1").unwrap();

    let mut store = Store::open(&dir, store_config()).unwrap();
    let result = store.open_dataset_by_identifier(1);
    assert!(matches!(result, Err(TmslError::InvalidData(_))));
}

#[test]
fn dataset_identifier_validates_lookup_and_missing_file() {
    let dir = temp_dir("invalid");
    {
        let mut store = Store::open(&dir, store_config()).unwrap();
        store
            .create_dataset_with_config("alpha", "data", None)
            .unwrap();
        assert!(matches!(
            store.open_dataset_by_identifier(0),
            Err(TmslError::InvalidData(_))
        ));
        assert!(matches!(
            store.open_dataset_by_identifier(999),
            Err(TmslError::NotFound(_))
        ));
    }

    fs::remove_file(dataset_identifier_path(&dir, "alpha", "data")).unwrap();
    let mut store = Store::open(&dir, store_config()).unwrap();
    assert!(matches!(
        store.open_dataset("alpha", "data"),
        Err(TmslError::NotFound(_))
    ));
}

#[test]
fn dataset_identifier_rejects_invalid_file_content() {
    let dir = temp_dir("bad_content");
    {
        let mut store = Store::open(&dir, store_config()).unwrap();
        store
            .create_dataset_with_config("alpha", "data", None)
            .unwrap();
    }

    fs::write(dataset_identifier_path(&dir, "alpha", "data"), "-1").unwrap();
    let mut store = Store::open(&dir, store_config()).unwrap();
    let result = store.open_dataset("alpha", "data");
    assert!(matches!(result, Err(TmslError::InvalidData(_))));
}

#[test]
fn dataset_identifier_rejects_zero_identifier_file_content() {
    let dir = temp_dir("zero_content");
    {
        let mut store = Store::open(&dir, store_config()).unwrap();
        store
            .create_dataset_with_config("alpha", "data", None)
            .unwrap();
    }

    fs::write(dataset_identifier_path(&dir, "alpha", "data"), "0").unwrap();
    let mut store = Store::open(&dir, store_config()).unwrap();
    let result = store.open_dataset("alpha", "data");
    assert!(matches!(result, Err(TmslError::InvalidData(_))));
}

#[test]
fn max_identifier_rejects_overflowing_file_content() {
    let dir = temp_dir("max_overflow");
    {
        let mut store = Store::open(&dir, store_config()).unwrap();
        store
            .create_dataset_with_config("alpha", "data", None)
            .unwrap();
    }

    fs::write(dir.join("max_identifier"), "18446744073709551616").unwrap();
    let result = Store::open(&dir, store_config());
    assert!(matches!(result, Err(TmslError::InvalidData(_))));
}
