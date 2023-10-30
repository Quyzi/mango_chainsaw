#![allow(unused_imports)]
use crate::internal::*;
use bytes::Bytes;
use log::LevelFilter;
use simplelog::{ColorChoice, CombinedLogger, Config as LogConfig, TermLogger, TerminalMode};
use std::{
    collections::hash_map::DefaultHasher,
    fs::File,
    hash::{Hash, Hasher},
};

#[test]
pub(crate) fn test_new_namespace() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let _ = CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Info,
        LogConfig::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )]);

    let db = DB::new(&temp.path().to_path_buf())?;
    let testing = db.open_namespace("testing")?;
    for n in 0..50 {
        let object = Bytes::from(format!("n={:#?}", n));
        let mut labels = vec![
            Label::new("test", &format!("{}", n)),
            Label::new("datatype", "sample"),
        ];
        if n % 3 == 0 {
            labels.push(Label::new("animal", "dog"));
        }
        if n % 10 == 0 && n > 0 {
            labels.push(Label::new("test", "13"));
            labels.push(Label::new("name", "Doggo"));
        }
        if n == 42 {
            labels.push(Label::new("name", "Pugsly"));
            labels.push(Label::new("name", "Doggo"));
            let mut hasher = DefaultHasher::new();
            object.hash(&mut hasher);
            log::info!(target: "mango_chainsaw", "Pugsly is hash {}", hasher.finish());
        }
        testing.insert(object, labels)?;
    }

    let ids = testing.query(vec![
        Label::new("animal", "dog"),
        // Label::new("test", "13"),
        Label::new("name", "Doggo"),
    ])?;
    log::info!(target: "mango_chainsaw", "found ids: {ids:#?}");

    Ok(())
}
