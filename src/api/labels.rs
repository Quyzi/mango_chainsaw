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

const TAG: &str = "Label Management";

#[derive(Clone, Serialize, Deserialize, Debug, ToSchema, ToResponse)]
pub(crate) struct LabelResponse {
}


/// Get all labels describing the given blob
#[utoipa::path(
    get, path = "/labels/{namespace_name}/{id}",
    tag = TAG,
)]
#[get("/labels/{namespace_name}/{id}")]
pub(crate) async fn get_blob_labels(req: HttpRequest, data: web::Data<DB>, path: web::Path<(String, u64)>) -> Result<impl Responder> {
    Ok("")
}


/// Get a label from a namespace by name
#[utoipa::path(
    get, path = "/labels/{namespace_name}",
    tag = TAG,
    params(
        ("namespace_name" = String, Path, description = "namespace name", example = "namespace_name"),
        ("label" = String, Query, description = "label", example = "name=pugsly")
    )
)]
#[get("/labels/{namespace_name}")]
pub(crate) async fn get_label(req: HttpRequest, data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<String>>) -> Result<impl Responder> {
    Ok("")
}


/// Delete a label from a namespace by name
#[utoipa::path(
    delete, path = "/labels/{namespace_name}",
    tag=TAG
)]
#[delete("/labels/{namespace_name}")]
pub(crate) async fn delete_label(req: HttpRequest, data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<String>>) -> Result<impl Responder> {
    Ok("")
}