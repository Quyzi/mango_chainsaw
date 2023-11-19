use std::time::{SystemTime, UNIX_EPOCH};

use crate::internal::*;
use actix_web::{
    delete, get, options, put,
    web::{self},
    HttpResponse, Responder,
};
use bytes::BytesMut;
use futures_util::StreamExt;
use serde_derive::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::{OpenApi, ToResponse, ToSchema};
use utoipa_rapidoc::RapiDoc;
use utoipa_swagger_ui::SwaggerUi;
type Result<T> = actix_web::Result<T>;

#[derive(OpenApi)]
#[openapi(
    info(title = "Mango Chainsaw API v3", description = "Store stuff in style",),
    paths(
        crate::api::v3::index,
        crate::api::v3::insert,
        crate::api::v3::list,
        crate::api::v3::get,
        crate::api::v3::search,
        crate::api::v3::deleteblob,
        crate::api::v3::deletenamespace,
    ),
    components(
        schemas(Label, ApiError, InsertResponse, QueryResponse),
        responses(ApiError, Label, InsertResponse, QueryResponse),
    )
)]
pub struct V3Documentation;

pub fn configure(cfg: &mut web::ServiceConfig) {
    let openapi = V3Documentation::openapi();

    cfg.service(crate::api::v3::index)
        .service(crate::api::v3::insert)
        .service(crate::api::v3::list)
        .service(crate::api::v3::get)
        .service(crate::api::v3::search)
        .service(crate::api::v3::deleteblob)
        .service(crate::api::v3::deletenamespace)
        .service(
            SwaggerUi::new("/api/swagger-ui/{_:.*}").url("/api-docs/openapi.json", openapi.clone()),
        )
        .service(RapiDoc::new("/api-docs/openapi.json").path("/api/rapidoc"));
}

#[derive(Error, Debug, Serialize, Deserialize, ToResponse, ToSchema)]
pub enum ApiError {
    #[error("failed to open namespace {0}. error={1}")]
    OpenNamespace(String, String),

    #[error("failed to insert blob size={1} into namespace {0}. error={2}")]
    InsertError(String, usize, String),

    #[error("failed to list namespaces. error={0}")]
    ListError(String),

    #[error("blob with id {1} not found in namespace {0}.")]
    BlobNotFound(String, u64),

    #[error("failed to get blob with id {1} from namespace {0}. error={2}")]
    GetError(String, u64, String),

    #[error("failed to query namespace {0}. error={1}")]
    QueryError(String, String),

    #[error("failed to delete blob {1} from namespace {0}")]
    DeleteBlobError(String, u64),

    #[error("failed to delete namespace {0}. error={1}")]
    DeleteNamespace(String, String),

    #[error("failed to serialize namespace name into bytes. error={0}")]
    SerializerError(String),
}

/// API V3 Index
///
/// Hello!
#[utoipa::path(
    get, path = "/api/v3",
    tag = "Mango Chainsaw API",
    responses(
        (status = 200, description = "Hello, world", body = String, example = json!("Hello")),
        (status = 418, description = "I'm a teapot", body = String, example = json!("I'm a teapot")),
    )
)]
#[get("/api/v3")]
pub async fn index(_data: web::Data<DB>) -> Result<impl Responder> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time travel is banned.")
        .as_millis();
    if now % 12345 == 0 {
        Ok(HttpResponse::ImATeapot().body("I'm a teapot"))
    } else {
        Ok(HttpResponse::Ok().body("Hello"))
    }
}

/// Get a list of Namespaces
///
/// Returns a list of namespace names
#[utoipa::path(
    options, path = "/api/v3",
    tag = "Mango Chainsaw API",
    responses(
        (status = 200, description = "Successfully retrieved list of namespaces", body = Vec<String>, example = json!(vec!["namespace_name", "cool_animals", "pictures_of_cats"])),
        (status = 500, description = "Failed to retrieve a list of namespaces", body = ApiError, example = json!(ApiError::ListError("something broke".to_string()))),
    )
)]
#[options("/api/v3")]
pub async fn list(data: web::Data<DB>) -> Result<impl Responder> {
    let db = data.clone();
    match db.list_namespaces() {
        Ok(names) => Ok(HttpResponse::Ok().json(names)),
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to list namespaces, error={e}");
            Ok(HttpResponse::InternalServerError().json(ApiError::ListError(e.to_string())))
        }
    }
}

/// Delete a namespace from the database by name
///
/// This deletes all of the data stored in the namespace.
#[utoipa::path(
    delete, path = "/api/v3/{namespace}",
    tag = "Mango Chainsaw API",
    params(
        ("namespace" = String, Path, description = "Name of the namespace to delete", example = json!("namespace_name")),
    ),
    responses(
        (status = 200, description = "Namespace deleted successfully", body = String, example = json!("namespace_name deleted")),
        (status = 500, description = "Failed to delete namespace, namespace did not open", body = ApiError, example = json!(ApiError::OpenNamespace("namespace_name".to_string(), "something broke".to_string()))),
        (status = 500, description = "Failed to delete namespace", body = ApiError, example = json!(ApiError::DeleteNamespace("namespace_name".to_string(), "something broke".to_string()))),
    )
)]
#[delete("/api/v3/{namespace}")]
pub async fn deletenamespace(
    data: web::Data<DB>,
    path: web::Path<String>,
) -> Result<impl Responder> {
    let db = data.clone();
    let namespace_name = path.into_inner();
    let namespace = match db.open_namespace(&namespace_name) {
        Ok(ns) => ns,
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to open namespace {namespace_name}, error={e}");
            return Ok(HttpResponse::InternalServerError()
                .json(ApiError::OpenNamespace(namespace_name, e.to_string())));
        }
    };
    match db.drop_namespace(namespace) {
        Ok(_) => Ok(HttpResponse::Ok().json(format!("{namespace_name} deleted"))),
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to delete namespace {namespace_name}, error={e}");
            Ok(HttpResponse::InternalServerError()
                .json(ApiError::DeleteNamespace(namespace_name, e.to_string())))
        }
    }
}

/// Insert request response
#[derive(Serialize, Deserialize, Clone, ToSchema, ToResponse)]
pub struct InsertResponse {
    /// `true` if insert was successful
    success: bool,

    /// Response message
    message: String,

    /// The `id` of the newly inserted blob
    id: String,

    /// The size of the blob
    size: String,

    /// The labels describing this blob
    labels: Vec<Label>,
}

/// Insert a blob into a namespace by name
///
/// The namespace is provided in the Path
/// The labels are provided by the Query
/// The body of the request is the Payload
#[utoipa::path(
    put, path = "/api/v3/{namespace}",
    tag = "Mango Chainsaw API",
    params(
        ("namespace" = String, Path, description = "The namespace to put the blob into.", example = "namespace_name"),
        ("label" = Vec<Label>, Query, style = Form, description = "Labels describing this blob.", example = json!(vec![Label{name: "animal".to_string(), value: "dog".to_string()}])),
    ),
    request_body = inline(String),
    responses(
        (status = 200, description = "Blob successfully inserted into the namespace", body = InsertResponse, example = json!(InsertResponse { success: true, message: "Ok".to_string(), id: "42069".to_string(), size: "12345".to_string(), labels: vec![Label{name: "animal".to_string(), value: "dog".to_string()}]})),
        (status = 500, description = "Failed to open namespace", body = ApiError, example = json!(ApiError::OpenNamespace("namespace_name".to_string(), "something broke".to_string()))),
        (status = 500, description = "Failed to insert blob into namespace", body = ApiError, example = json!(ApiError::InsertError("namespace_name".to_string(), 420, "something broke".to_string()))),
    )
)]
#[put("/api/v3/{namespace}")]
pub async fn insert(
    data: web::Data<DB>,
    path: web::Path<String>,
    query: web::Query<Vec<Label>>,
    mut body: web::Payload,
) -> Result<impl Responder> {
    let db = data.clone();
    let namespace_name = path.into_inner();
    let labels = query.into_inner();
    let blob = {
        let mut blob = BytesMut::new();
        while let Some(Ok(bytes)) = body.next().await {
            blob.extend(bytes);
        }
        blob.freeze()
    };
    let size = blob.len();

    let namespace = match db.open_namespace(&namespace_name) {
        Ok(ns) => ns,
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to open namespace {namespace_name} to insert blob with size {size}, error={e}");
            return Ok(HttpResponse::InternalServerError()
                .json(ApiError::OpenNamespace(namespace_name, e.to_string())));
        }
    };

    let id = match namespace.insert(blob, labels.to_owned()) {
        Ok(id) => id,
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to insert blob into namespace {namespace_name} with size {size}, error={e}");
            return Ok(
                HttpResponse::InternalServerError().json(ApiError::InsertError(
                    namespace_name,
                    size,
                    e.to_string(),
                )),
            );
        }
    };

    log::info!(target: "mango_chainsaw", "inserted blob with id={id} into namespace={namespace_name}");
    Ok(HttpResponse::Ok().json(InsertResponse {
        success: true,
        message: "Blob inserted into namespace".to_string(),
        id: format!("{id}"),
        size: format!("{size}"),
        labels,
    }))
}

/// Get a blob from a namespace by id
///
/// If a blob exists in the namespace with the given id, it will be returned as Bytes.
#[utoipa::path(
    get, path = "/api/v3/{namespace}/{id}",
    tag = "Mango Chainsaw API",
    params(
        ("namespace" = String, description = "Name of the namespace to get the blob from", example = json!("cool_animals")),
        ("id" = u64, description = "ID of the blob to get from the given namespace", example = json!(133742069)),
    ),
    responses(
        (status = 200, description = "Successfully got the blob from the given namespace", body = String),
        (status = 404, description = "Requested blob not found in the given namespace", body = ApiError, example = json!(ApiError::BlobNotFound("cool_animals".to_string(), 133742069))),
        (status = 500, description = "Failed to retrieve the requested blob from the given namespace", body = ApiError, example = json!(ApiError::GetError("cool_animals".to_string(), 133742069, "something broke".to_string())))
    )
)]
#[get("/api/v3/{namespace}/{id}")]
pub async fn get(data: web::Data<DB>, path: web::Path<(String, u64)>) -> Result<impl Responder> {
    let db = data.clone();
    let (namespace_name, id) = path.into_inner();

    let namespace = match db.open_namespace(&namespace_name) {
        Ok(ns) => ns,
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to open namespace {namespace_name} to get blob with id {id}, error={e}");
            return Ok(HttpResponse::InternalServerError()
                .json(ApiError::OpenNamespace(namespace_name, e.to_string())));
        }
    };
    match namespace.get(id) {
        Ok(Some(blob)) => {
            log::info!(target: "mango_chainsaw", "found blob with id={id} in namespace={namespace_name} with size={}", blob.len());
            Ok(HttpResponse::Ok().body(blob))
        }
        Ok(None) => {
            log::info!(target: "mango_chainsaw", "failed to find blob with id={id} in namespace={namespace_name}");
            Ok(HttpResponse::NotFound().json(ApiError::BlobNotFound(namespace_name, id)))
        }
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to get blob from namespace {namespace_name} with id {id}, error={e}");
            Ok(HttpResponse::InternalServerError().json(ApiError::GetError(
                namespace_name,
                id,
                e.to_string(),
            )))
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema, ToResponse)]
pub struct QueryResponse {
    /// Was the query successful?
    pub success: bool,

    /// Any message returned during the query
    pub message: String,

    /// Namespace that was queried
    pub namespace: String,

    /// Labels used in this query
    pub labels: Vec<Label>,

    /// Results of the query
    pub results: Option<Vec<String>>,
}

/// Search a namespace for blobs matching all of the given label pairs
///
/// Returns a list of blob ids for any blobs that match all of the given labels
#[utoipa::path(
    get, path = "/api/v3/{namespace}",
    tag = "Mango Chainsaw API",
    params(
        ("namespace" = String, Path, description = "Name of the namespace to run this query on", example = "namespace_name"),
        ("label" = Vec<Label>, Query, style = Form, description = "Get blobs matching ALL of the given labels", example = json!(vec![Label{name: "animal".to_string(), value: "dog".to_string()}]))
    ),
    responses(
        (status = 200, description = "database query successful", body = QueryResponse, example = json!(QueryResponse {success: true, message: "success".to_string(), namespace: "namespace_name".to_string(), labels: vec![Label{name: "animal".to_string(), value: "dog".to_string()}], results: Some(vec![format!("133742069")])})),
        (status = 400, description = "database query failed, no labels given", body = ApiError, example = json!(ApiError::QueryError("namespace_name".to_string(), "Query must have at least one label".to_string()))),
        (status = 404, description = "database query successful, but no events found", body = QueryResponse, example = json!(QueryResponse { success: true, message: "no blobs found".to_string(), namespace: "namespace_name".to_string(), labels: vec![Label{name: "animal".to_string(), value: "cat".to_string()}], results: None})),
        (status = 500, description = "database query failed", body = ApiError, example = json!(ApiError::QueryError("namespace_name".to_string(), "something broke".to_string())))
    )
)]
#[get("/api/v3/{namespace}")]
pub async fn search(
    data: web::Data<DB>,
    path: web::Path<String>,
    query: web::Query<Vec<Label>>,
) -> Result<impl Responder> {
    let db = data.clone();
    let namespace_name = path.into_inner();
    let labels = query.into_inner();
    if labels.is_empty() {
        return Ok(HttpResponse::BadRequest().json(ApiError::QueryError(
            namespace_name,
            "Query must have at least one label".to_string(),
        )));
    }
    let namespace = match db.open_namespace(&namespace_name) {
        Ok(ns) => ns,
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to open namespace {namespace_name} for querying");
            return Ok(HttpResponse::InternalServerError()
                .json(ApiError::QueryError(namespace_name, e.to_string())));
        }
    };
    let results = match namespace.query(labels.to_owned()) {
        Ok(ids) => ids,
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to query namespace {namespace_name}, error={e}");
            return Ok(HttpResponse::InternalServerError()
                .json(ApiError::QueryError(namespace_name, e.to_string())));
        }
    };
    if results.is_empty() {
        Ok(HttpResponse::Ok().json(QueryResponse {
            success: true,
            message: "Query successful, but no blobs matched the given labels.".to_string(),
            namespace: namespace_name,
            labels,
            results: None,
        }))
    } else {
        Ok(HttpResponse::Ok().json(QueryResponse {
            success: true,
            message: format!(
                "Query successful, found {} blobs for the given labels",
                results.len()
            ),
            namespace: namespace_name,
            labels,
            results: Some(results.into_iter().map(|it| format!("{it}")).collect()),
        }))
    }
}

/// Delete a blob from a namespace
///
/// Deletes a blob with the given id from the given namespace
#[utoipa::path(
    delete, path = "/api/v3/{namespace}/{id}",
    tag = "Mango Chainsaw API",
    params(
        ("namespace" = String, Path, description = "The name of the namespace to delete a blob from", example = "namespace_name"),
        ("id" = u64, Path, description = "The id of the blob to delete", example = 133742069),
    ),
    responses(
        (status = 200, description = "Successfully deleted blob", body = String, example = json!("Blob with id 133742069 deleted from namespace=namespace_name")),
        (status = 500, description = "Failed to delete blob", body = ApiError, example = json!(ApiError::DeleteBlobError("namespace_name".to_string(), 133742069))),
        (status = 500, description = "Failed to delete blob, namespace did not open", body = ApiError, example = json!(ApiError::OpenNamespace("namespace_name".to_string(), "something broke".to_string()))),
    ),
)]
#[delete("/api/v3/{namespace}/{id}")]
pub async fn deleteblob(
    data: web::Data<DB>,
    path: web::Path<(String, u64)>,
) -> Result<impl Responder> {
    let db = data.clone();
    let (namespace_name, id) = path.into_inner();
    let namespace = match db.open_namespace(&namespace_name) {
        Ok(ns) => ns,
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to open namespace {namespace_name} to get blob with id {id}, error={e}");
            return Ok(HttpResponse::InternalServerError()
                .json(ApiError::OpenNamespace(namespace_name, e.to_string())));
        }
    };
    match namespace.delete_blob(id) {
        Ok(_) => Ok(HttpResponse::Ok().body(format!(
            "Blob with id={id} deleted from namespace={namespace_name}"
        ))),
        Err(e) => {
            log::error!(target: "mango_chainsaw", "failed to delete blob from namespace {namespace_name} with id {id}, error={e}");
            Ok(HttpResponse::InternalServerError()
                .json(ApiError::DeleteBlobError(namespace_name, id)))
        }
    }
}
