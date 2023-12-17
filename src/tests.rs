use crate::delete::DeleteRequest;
use crate::{common::Label, db::Db, insert::InsertRequest, query::QueryRequest};
use anyhow::Result;
use bytes::Bytes;
use log::LevelFilter;

use simplelog::Config as LogConfig;
use simplelog::{ColorChoice, CombinedLogger, TermLogger, TerminalMode};

use walkdir::WalkDir;

use std::path::PathBuf;
use std::{fs, io::Read};

fn create_payloads(db: Db) -> Result<Vec<InsertRequest>> {
    let mut payloads = vec![];

    let mut cwd = std::env::current_dir()?;
    cwd.push("src");
    for entry in WalkDir::new(cwd) {
        let entry = entry?;
        if entry.file_type().is_dir() {
            continue;
        }

        let meta = entry.metadata()?;

        let size_bytes = meta.len();
        let filepath = match entry.path().to_str() {
            Some(path) => path,
            None => continue,
        };

        if filepath.contains(".git") || filepath.contains("temp") {
            continue;
        }

        let filename = match entry.file_name().to_str() {
            Some(name) => name,
            None => continue,
        };
        let fileext = match entry.path().extension() {
            Some(e) => e.to_str().unwrap(),
            None => "none",
        };

        let ctype = match filename {
            "common.rs" => "code-common",
            "delete.rs" => "code-mutable",
            "insert.rs" => "code-mutable",
            _ => "code-misc",
        };

        let contents = {
            let mut buf = Vec::with_capacity(meta.len() as usize);
            let mut file = fs::OpenOptions::new()
                .read(true)
                .write(false)
                .open(entry.path())?;
            file.read_to_end(&mut buf)?;
            buf
        };

        let req = InsertRequest::new_using_db(&db, Bytes::from(contents))?;
        req.add_label(Label::new(&format!(
            "mango_chainsaw.test/full_path={filepath}"
        )))?;
        req.add_label(Label::new(&format!(
            "mango_chainsaw.test/filename={filename}"
        )))?;
        req.add_label(Label::new(&format!(
            "mango_chainsaw.test/filetype={fileext}"
        )))?;
        req.add_label(Label::new(&format!(
            "mango_chainsaw.test/content_type={ctype}"
        )))?;
        req.add_label(Label::new(&format!(
            "mango_chainsaw.test/filesize={size_bytes}"
        )))?;

        payloads.push(req);
    }

    Ok(payloads)
}

/// build a test db using the current source code as data.
fn e2e_build() -> Result<()> {
    let path = PathBuf::from("./temp");
    let db = Db::open(path.as_path())?;

    let ns = db.open_namespace("files")?;

    let payloads = create_payloads(db.clone())?;
    let _num = payloads.len();
    let mut inserted_ids = vec![];
    for payload in payloads {
        let objectid = payload.execute(&ns)?;
        inserted_ids.push(objectid);
        log::info!("added object with id {objectid}");
    }
    db.flush_sync()?;

    Ok(())
}

/// Get just the "code-mutable" objects using an exact include label
fn e2e_query_include() -> Result<()> {
    let path = PathBuf::from("./temp");
    let db = Db::open(path.as_path())?;

    let ns = db.open_namespace("files")?;

    let req = QueryRequest::new();
    req.include(Label::new("mango_chainsaw.test/content_type=code-mutable"))?;

    let object_ids = req.execute(&ns)?;
    let objects = ns.get_all(object_ids)?;

    for (id, labels, contents) in objects {
        let labels = labels.unwrap();
        let contents: String = flexbuffers::from_slice(&contents.unwrap())?;
        log::info!("id={id}; labels={labels:?} :: {}", contents.len() as u64);
    }

    db.flush_sync()?;
    Ok(())
}

/// Get just the "code-mutable" objects using a prefix and excludes
fn e2e_query_prefix_exclude() -> Result<()> {
    let path = PathBuf::from("./temp");
    let db = Db::open(path.as_path())?;

    let ns = db.open_namespace("files")?;

    let req = QueryRequest::new();
    req.include_prefix(Label::new("mango_chainsaw.test/content_type=code"))?;
    req.exclude(Label::new("mango_chainsaw.test/content_type=code-common"))?;
    req.exclude(Label::new("mango_chainsaw.test/content_type=code-misc"))?;

    let object_ids = req.execute(&ns)?;
    let objects = ns.get_all(object_ids)?;

    for (id, labels, contents) in objects {
        let labels = labels.unwrap();
        // let contents: String = flexbuffers::from_slice(&contents.unwrap().to_vec())?;
        log::info!(
            "id={id}; labels={labels:?} :: {}",
            contents.unwrap().len() as u64
        );
    }

    db.flush_sync()?;
    Ok(())
}

/// Delete the code-mutable objects
fn e2e_delete() -> Result<()> {
    let path = PathBuf::from("./temp");
    let db = Db::open(path.as_path())?;

    let ns = db.open_namespace("files")?;
    let req = QueryRequest::new();
    req.include_prefix(Label::new("mango_chainsaw.test/content_type=code"))?;
    req.exclude(Label::new("mango_chainsaw.test/content_type=code-common"))?;
    req.exclude(Label::new("mango_chainsaw.test/content_type=code-misc"))?;

    let results = req.execute(&ns)?;
    let req = DeleteRequest::new();
    for object_id in results {
        req.add_object(object_id)?;
    }

    req.execute(&ns)?;

    Ok(())
}

#[test]
fn e2e_test() -> Result<()> {
    let _ = CombinedLogger::init(vec![TermLogger::new(
        LevelFilter::Info,
        LogConfig::default(),
        TerminalMode::Mixed,
        ColorChoice::Auto,
    )]);

    e2e_build()?;
    e2e_query_include()?;
    e2e_query_prefix_exclude()?;
    e2e_delete()?;
    e2e_query_prefix_exclude()?;
    Ok(())
}
