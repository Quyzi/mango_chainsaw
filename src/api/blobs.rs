use crate::internal::*;
use actix_web::{
    delete, get, options, put,
    web::{self},
    HttpResponse, Responder, HttpRequest,
};
use bytes::BytesMut;
use futures_util::StreamExt;
use serde_derive::{Deserialize, Serialize};
use utoipa::{OpenApi, ToResponse, ToSchema};

type Result<T> = actix_web::Result<T>;

const TAG: &str = "Blob Management";

#[derive(Clone, Serialize, Deserialize, Debug, ToSchema, ToResponse)]
pub(crate) struct BlobResponse {
}



/// Get a blob stored in the given namespace with the given id
#[utoipa::path(
    get, path = "/blobs/{namespace_name}/{id}",
    tag = TAG,
)]
#[get("/blobs/{namespace_name}/{id}")]
pub(crate) async fn get_blob(req: HttpRequest, data: web::Data<DB>, path: web::Path<(String, u64)>) -> Result<impl Responder> {
    Ok("")
}

/// Delete a blob stored in the given namespace with the given id
#[utoipa::path(
    delete, path = "/blobs/{namespace_name}/{id}",
    tag = TAG,
)]
#[delete("/blobs/{namespace_name}/{id}")]
pub(crate) async fn delete_blob(req: HttpRequest, data: web::Data<DB>, path: web::Path<(String, u64)>) -> Result<impl Responder> {
    Ok("")
}

/// Search for blobs in a namespace with the given labels
#[utoipa::path(
    get, path = "/blobs/{namespace_name}",
    tag = TAG,
)]
#[get("/blobs/{namespace_name}")]
pub(crate) async fn search_blobs(req: HttpRequest, data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<Label>>) -> Result<impl Responder> {
    Ok("")
}