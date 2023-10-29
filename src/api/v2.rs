use actix_web::{HttpServer, App, Responder, web::{self, Json}, HttpRequest, middleware::Logger, HttpResponse, get, put, post, delete};
use bytes::BytesMut;
use rayon::prelude::*;
use std::future::Future;
use futures_util::StreamExt as _;
use serde_derive::{Deserialize, Serialize};
use crate::{internal::*, namespace::{NamespaceStats, self}};
type Result<T> = actix_web::Result<T>;


pub async fn start_server(bind: (String, u16), db: DB) -> Result<()> {
    let appdata = db.clone();

    HttpServer::new(move || {
        let db = appdata.clone();
        App::new()
        .wrap(Logger::default())
        .app_data(web::Data::new(db))
        .service(index)

        .service(list_namespaces)
        .service(delete_namespace)
        .service(list_trees)

        .service(put_blob)
        .service(get_blob)
        .service(delete_blob)
        .service(query_blobs)
    })
    .bind(bind)?
    .run()
    .await?;

    Ok(())
}

/// Hello
#[get("/api/v2/")]
async fn index() -> Result<impl Responder> {
    Ok(HttpResponse::Ok().json("Hi"))
}

/// List namespaces
#[get("/api/v2/namespaces")]
async fn list_namespaces(data: web::Data<DB>) -> Result<impl Responder> {
    match data.list_namespaces() {
        Ok(ns) => Ok(HttpResponse::Ok().json(ns)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e))
    }
}

/// Delete a namespace by name
#[delete("/api/v2/namespaces/{namespace}")]
async fn delete_namespace(data: web::Data<DB>, path: web::Path<String>) -> Result<impl Responder> {
    let name = path.into_inner();
    let namespace = match data.open_namespace(&name) {
        Ok(ns) => ns, 
        Err(e) => return Err(actix_web::error::ErrorNotFound(e))
    };
    match data.drop_namespace(namespace) {
        Ok(_) => Ok(HttpResponse::Ok().body(format!("{name} deleted"))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e))
    }
}

/// List data storage trees
#[get("/api/v2/trees")]
async fn list_trees(data: web::Data<DB>) -> Result<impl Responder> {
    match data.list_trees() {
        Ok(trees) => Ok(HttpResponse::Ok().json(trees)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}

/// Insert an object into the namespace
#[put("/api/v2/{namespace}")]
async fn put_blob(data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<Label>>, mut body: web::Payload) -> Result<impl Responder> {
    let namespace = match data.open_namespace(&path.into_inner()) {
        Ok(ns) => ns,
        Err(e) => return Err(actix_web::error::ErrorInternalServerError(e)),
    };
    let mut blob = BytesMut::new();
    while let Some(bytes) = body.next().await {
        blob.extend_from_slice(&bytes.map_err(|e| actix_web::error::ErrorInternalServerError(e))?)
    }
    match namespace.insert(blob.freeze(), query.into_inner()) {
        Ok(id) => Ok(HttpResponse::Ok().json(id)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}

/// Get an object by id
#[get("/api/v2/{namespace}/{id}")]
async fn get_blob(data: web::Data<DB>, path: web::Path<(String, u64)>) -> Result<impl Responder> {
    let (name, id) = path.into_inner();
    let namespace = match data.open_namespace(&name) {
        Ok(ns) => ns,
        Err(e) => return Err(actix_web::error::ErrorInternalServerError(e)),
    };
    match namespace.get(id) {
        Ok(Some(blob)) => Ok(HttpResponse::Ok().body(blob)),
        Ok(None) => Ok(HttpResponse::NotFound().json(format!("{id} not found"))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}

/// Delete an object by id
#[delete("/api/v2/{namespace}/{id}")]
async fn delete_blob(data: web::Data<DB>, path: web::Path<(String, u64)>) -> Result<impl Responder> {
    let (name, id) = path.into_inner();
    let namespace = match data.open_namespace(&name) {
        Ok(ns) => ns,
        Err(e) => return Err(actix_web::error::ErrorInternalServerError(e)),
    };
    match namespace.delete_objects(vec![id]) {
        Ok(_) => Ok(HttpResponse::Ok().json(format!("{id} deleted"))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}

/// Query a namespace for objects with the given labels
#[get("/api/v2/{namespace}")]
async fn query_blobs(data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<Label>>) -> Result<impl Responder> {
    let name = path.into_inner();
    let labels = query.into_inner();
    let namespace = match data.open_namespace(&name) {
        Ok(ns) => ns,
        Err(e) => return Err(actix_web::error::ErrorInternalServerError(e)),
    };
    match namespace.query(labels.clone()) {
        Ok(ids) => {
            if ids.is_empty() {
                Ok(HttpResponse::NotFound().json(labels))
            } else {
                Ok(HttpResponse::Ok().json(ids))
            }
        },
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}