pub mod bucket;
pub mod label;
pub mod mango;
pub mod object;
pub mod query;

#[cfg(test)]
#[allow(unused)]
mod tests {
    use std::env;

    use anyhow::{anyhow, Result};
    use bytes::Bytes;
    use flexbuffers::FlexbufferSerializer;
    use log::LevelFilter;
    use simplelog::{CombinedLogger, TermLogger, TerminalMode};
    use walkdir::WalkDir;

    use crate::{
        label::Label,
        label::SEPARATOR as LabelSep,
        mango::Mango,
        object::Object,
        query::{
            find::FindRequest,
            get::GetRequest,
            insert::InsertRequest,
            transaction::{Request, Transaction},
        },
    };

    fn ser<T: serde::Serialize>(item: T) -> Result<Bytes> {
        let mut s = FlexbufferSerializer::new();
        item.serialize(&mut s)?;
        Ok(s.take_buffer().into())
    }

    fn de<T: serde::de::DeserializeOwned>(bytes: Bytes) -> Result<T> {
        Ok(flexbuffers::from_slice(&bytes)?)
    }

    #[test]
    fn test_full() -> Result<()> {
        CombinedLogger::init(vec![TermLogger::new(
            LevelFilter::Trace,
            simplelog::ConfigBuilder::new()
                .set_thread_level(LevelFilter::Trace)
                .set_thread_mode(simplelog::ThreadLogMode::Both)
                .add_filter_ignore_str("sled")
                .build(),
            TerminalMode::Mixed,
            simplelog::ColorChoice::Auto,
        )])?;

        let mango = Mango::new_temp()?;
        let bucket = mango.get_bucket("testing")?;
        let cwd = env::current_dir()?.join("src");

        let tx: Transaction = (&bucket).into();

        // walk the current directory and add all files
        for entry in WalkDir::new(cwd) {
            let entry = entry?;
            let _meta = entry.metadata()?;
            if entry.file_type().is_dir() {
                continue;
            }
            let file_path = entry
                .path()
                .to_str()
                .expect("error getting file path for {entry:?}");
            if file_path.contains(".git") || file_path.contains("temp") {
                continue;
            }

            let filename = entry
                .path()
                .file_name()
                .expect("error getting filename for {entry:?}")
                .to_str()
                .expect("error getting filename for {entry:?}");
            let file_ext = match entry.path().extension() {
                Some(e) => e.to_str().unwrap(),
                None => "none",
            };

            let contents = std::fs::read_to_string(entry.path())?;
            let content_bytes = String::as_bytes(&contents).to_vec();

            let content_type = match filename {
                "insert.rs" | "delete.rs" | "transaction.rs" | "find.rs" | "cswap.rs" => {
                    "transaction"
                }

                "lib.rs" | "label.rs" | "mango.rs" | "object.rs" => "library",

                "mod.rs" => "module",

                "Cargo.lock" | "Cargo.toml" => "cargo",

                _ => "none",
            };

            let labels: Vec<Label> = vec![
                format!("mango_chainsaw/full_path{LabelSep}{file_path}").try_into()?,
                format!("mango_chainsaw/filename{LabelSep}{filename}").try_into()?,
                format!("mango_chainsaw/filetype{LabelSep}{file_ext}").try_into()?,
                format!("mango_chainsaw/content_type{LabelSep}{content_type}").try_into()?,
            ];
            assert_eq!(
                labels,
                vec![
                    Label::new("mango_chainsaw/full_path", file_path),
                    Label::new("mango_chainsaw/filename", filename),
                    Label::new("mango_chainsaw/filetype", file_ext),
                    Label::new("mango_chainsaw/content_type", content_type),
                ]
            );

            let req = InsertRequest::new_monotonic_id(&mango, content_bytes.into())?;
            req.add_labels(labels)?;
            tx.append_request(Request::Insert(req))?;
        }
        tx.execute()?;

        let find_tx: Transaction = (&bucket).into();

        // Use labels to get the lib.rs file
        let req = FindRequest::new()?;
        req.add_include_group(vec![Label::new("mango_chainsaw/content_type", "library")])?;
        req.add_exclude_group(vec![
            Label::new("mango_chainsaw/filename", "label.rs"),
            Label::new("mango_chainsaw/filename", "object.rs"),
            Label::new("mango_chainsaw/filename", "mango.rs"),
        ])?;

        find_tx.append_request(req.into())?;
        find_tx.execute()?;
        let results = find_tx.results()?;
        let first = results.first().unwrap();
        let id = match first {
            crate::query::transaction::RequestResult::Find(_, Ok(res)) => res.first().unwrap().0,
            _ => return Err(anyhow!("shit")),
        };

        // Get the object out and compare
        let original = std::fs::read_to_string("src/lib.rs")?;

        let tx: Transaction = (&bucket).into();
        tx.append_request(GetRequest::new(vec![id])?.into())?;
        tx.execute()?;
        let results = tx.results()?;
        let first = results.first().unwrap();
        match first {
            crate::query::transaction::RequestResult::Get(_, Ok(res)) => {
                let bytes = &res.first().unwrap().1;
                let got = String::from_utf8(bytes.to_vec())?;
                assert_eq!(original, got);
                Ok(())
            }
            _ => Err(anyhow!("shit 2")),
        }
    }
}
