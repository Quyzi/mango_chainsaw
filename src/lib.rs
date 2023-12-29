pub mod bucket;
pub mod label;
pub mod mango;
pub mod object;
pub mod query;

#[cfg(test)]
mod tests {
    use std::{env, fs::OpenOptions, io::Read, path::PathBuf};

    use anyhow::Result;
    use log::LevelFilter;
    use simplelog::{CombinedLogger, TermLogger, TerminalMode};
    use walkdir::WalkDir;

    use crate::{
        label::Label,
        label::SEPARATOR as LabelSep,
        mango::Mango,
        query::{
            insert::InsertRequest,
            transaction::{Request, Transaction},
        },
    };

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

        let mango_path = PathBuf::from("temp");
        let mango: Mango = mango_path.try_into()?;
        let bucket = mango.get_bucket("testing")?;
        let cwd = env::current_dir()?.join("src");

        let tx: Transaction = (&bucket).into();

        // walk the current directory and add all files
        for entry in WalkDir::new(cwd) {
            let entry = entry?;
            let meta = entry.metadata()?;
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

            let contents = {
                let mut buf = Vec::with_capacity(meta.len() as usize);
                let mut file = OpenOptions::new()
                    .read(true)
                    .write(false)
                    .open(entry.path())?;
                file.read_to_end(&mut buf)?;
                buf
            };

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

            let req = InsertRequest::new_monotonic_id(&mango, contents.into())?;
            req.add_labels(labels)?;
            tx.append_request(Request::Insert(req))?;
        }
        tx.execute()?;

        // TODO: Get the objects back out by label

        Ok(())
    }
}
