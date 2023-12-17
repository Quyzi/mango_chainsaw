use crate::{common::Label, db::Db, insert::InsertRequest, query::QueryRequest};
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::executor;
use serde_json::json;
use std::{
    fmt::Write,
    time::{SystemTime, UNIX_EPOCH},
};

fn create_db() -> Result<Db> {
    Db::open_temp()
}

fn make_payload() -> Result<Bytes> {
    let now = {
        let now = SystemTime::now();
        now.duration_since(UNIX_EPOCH)?.as_secs()
    };
    let mut buf = BytesMut::new();
    write!(
        &mut buf,
        "{}",
        json!({
            "thing": "longer",
            "numbers": [
                4, 2, 0, 6, 9,
                8675309,
                4, 8, 15, 16, 23, 42
            ],
            "now": now,
            "living": false,
        })
    )?;

    Ok(buf.freeze())
}

#[test]
fn test_insert_query() -> Result<()> {
    let db = create_db()?;
    let ns = db.open_namespace("testing")?;

    let now = {
        let now = SystemTime::now();
        now.duration_since(UNIX_EPOCH)?.as_secs()
    };

    let req = InsertRequest::new_using_db(
        &db,
        make_payload()?
    )?;
    let labels = vec![
        Label::new("mango.chainsaw/testing=true"),
        Label::new("mango.chainsaw/prod=true"),
        Label::new("mango.chainsaw/dev=true"),
        Label::new("mango.chainsaw/staging=true"),
        Label::new("mango.chainsaw/service=dummy"),
        Label::new(&format!("mango.chainsaw/updated={now}")),
    ];
    for label in labels {
        req.add_label(label)?;
    }
    let id = req.execute(&ns)?;

    let query = QueryRequest::new();
    query.include(Label::new("mango.chainsaw/dev=true"))?;
    let ids = executor::block_on(query.execute(&ns))?;
    assert!(ids.contains(&id));
    log::info!(
        target: "mango_chainsaw::tests/query",
        "got ids {ids:#?}",
    );
    Ok(())
}
