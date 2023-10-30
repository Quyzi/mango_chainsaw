use actix_web::{
    delete, get,
    middleware::{Logger, Compress},
    put,
    web::{self},
    App, HttpResponse, HttpServer, Responder,
};
use bytes::BytesMut;
use futures_util::StreamExt as _;
use utoipa::OpenApi;
use utoipa_rapidoc::RapiDoc;
use utoipa_redoc::{Redoc, Servable};
use utoipa_swagger_ui::SwaggerUi;

use crate::internal::*;
type Result<T> = actix_web::Result<T>;

pub async fn start_server(bind: (String, u16), db: DB) -> Result<()> {
    #[derive(OpenApi)]
    #[openapi(
        info(
            title = "Mango Chainsaw",
            description = "In-memory blobstore using namespace + label indexing",
        ),
        paths(
            crate::api::v2::index,
            crate::api::v2::list_namespaces,
            crate::api::v2::delete_namespace,
            crate::api::v2::list_trees,
            crate::api::v2::put_blob,
            crate::api::v2::get_blob,
            crate::api::v2::delete_blob,
            crate::api::v2::query_blobs,
        ),
        components(
            schemas(Label, NamespaceStats),
            responses(Label, NamespaceStats)
        ),
        tags(
            (name = "Mango Chainsaw", description = "In-memory label indexed blobstore")
        )
    )]
    struct ApiDoc;
    
    let appdata = db.clone();
    let openapi = ApiDoc::openapi();

    HttpServer::new(move || {
        let db = appdata.clone();
        App::new()
            .wrap(Logger::default())
            .wrap(Compress::default())
            .app_data(web::Data::new(db))
            .service(index)
            .service(list_namespaces)
            .service(delete_namespace)
            .service(namespace_stats)
            .service(list_trees)
            .service(put_blob)
            .service(get_blob)
            .service(delete_blob)
            .service(query_blobs)
            .service(Redoc::with_url("/redoc", openapi.clone()))
            .service(SwaggerUi::new("/swagger-ui/{_:.*}")
                .url("/api-docs/openapi.json", openapi.clone())
            )
            .service(RapiDoc::new("/api-docs/openapi.json").path("/rapidoc"))
    })
    .bind(bind)?
    .run()
    .await?;

    Ok(())
}

#[utoipa::path(
    get,
    path = "/api/v2",
    responses(
        (status = 200, description = "Hello, world")
    )
)]
#[get("/api/v2")]
pub async fn index() -> Result<impl Responder> {
    Ok(HttpResponse::Ok().json("Hi"))
}

#[utoipa::path(
    get, 
    path = "/api/v2/namespaces",
    responses(
        (status = 200, description = "Successfully got list of namespaces", body = Vec<String>),
        (status = 500, description = "Database error, failed to get namespaces list", body = String)
    )
)]
#[get("/api/v2/namespaces")]
pub async fn list_namespaces(data: web::Data<DB>) -> Result<impl Responder> {
    match data.list_namespaces() {
        Ok(ns) => Ok(HttpResponse::Ok().json(ns)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}

#[utoipa::path(
    delete,
    path = "/api/v2/{namespace}",
    responses(
        (status = 200, description = "Namespace successfully deleted", body = String),
        (status = 404, description = "Namespace does not exist to delete", body = String),
        (status = 500, description = "Database error, failed to delete namespace", body = String)
    ),
    params(
        ("namespace" = String, Path, description = "Name of the namespace to delete")
    )
)]
#[delete("/api/v2/{namespace}")]
pub async fn delete_namespace(data: web::Data<DB>, path: web::Path<String>) -> Result<impl Responder> {
    let name = path.into_inner();
    let namespace = match data.open_namespace(&name) {
        Ok(ns) => ns,
        Err(e) => return Err(actix_web::error::ErrorNotFound(e)),
    };
    match data.drop_namespace(namespace) {
        Ok(_) => Ok(HttpResponse::Ok().body(format!("{name} deleted"))),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/v2/trees",
    responses(
        (status = 200, description = "Successfully listed database storage trees", body = Vec<String>),
        (status = 500, description = "Database error, failed to list trees", body = String)
    )
)]
#[get("/api/v2/trees")]
pub async fn list_trees(data: web::Data<DB>) -> Result<impl Responder> {
    match data.list_trees() {
        Ok(trees) => Ok(HttpResponse::Ok().json(trees)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}

#[utoipa::path(
    put,
    path = "/api/v2/{namespace}",
    responses(
        (status = 200, description = "Blob successfully added to namespace", body = u64),
        (status = 500, description = "Database error, failed to insert blob", body = String),
    ),
    params(
        ("namespace" = String, Path, description = "Name of the namespace to insert the blob into"),
        ("labels" = Vec<Label>, Query, description = "A list of labels defining this object"),
    )
)]
#[put("/api/v2/{namespace}")]
pub async fn put_blob(
    data: web::Data<DB>,
    path: web::Path<String>,
    query: web::Query<Vec<Label>>,
    mut body: web::Payload,
) -> Result<impl Responder> {
    let namespace = match data.open_namespace(&path.into_inner()) {
        Ok(ns) => ns,
        Err(e) => return Err(actix_web::error::ErrorInternalServerError(e)),
    };
    let mut blob = BytesMut::new();
    while let Some(bytes) = body.next().await {
        blob.extend_from_slice(&bytes.map_err(actix_web::error::ErrorInternalServerError)?)
    }
    match namespace.insert(blob.freeze(), query.into_inner()) {
        Ok(id) => Ok(HttpResponse::Ok().json(id)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/v2/{namespace}/{id}",
    responses(
        (status = 200, description = "Successfully retrieved blob from namespace", body = Bytes),
        (status = 404, description = "Blob not found in namespace", body = String),
        (status = 500, description = "Database error, failed to get blob", body = String)
    ),
    params(
        ("namespace" = String, Path, description = "Name of the namespace to get the blob from"),
        ("id" = u64, Path, description = "The id number of the blob to get"),
    )
)]
#[get("/api/v2/{namespace}/{id}")]
pub async fn get_blob(data: web::Data<DB>, path: web::Path<(String, u64)>) -> Result<impl Responder> {
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

#[utoipa::path(
    delete,
    path = "/api/v2/{namespace}/{id}",
    responses(
        (status = 200, description = "Successfully deleted blob", body = String),
        (status = 500, description = "Database error, failed to delete blob", body = String),
    ),
    params(
        ("namespace" = String, Path, description = "Name of the namespace to get the blob from"),
        ("id" = u64, Path, description = "The id number of the blob to get"),
    )
)]
#[delete("/api/v2/{namespace}/{id}")]
pub async fn delete_blob(
    data: web::Data<DB>,
    path: web::Path<(String, u64)>,
) -> Result<impl Responder> {
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

#[utoipa::path(
    get,
    path = "/api/v2/{namespace}",
    responses(
        (status = 200, description = "Namespace query completed. Returning IDs", body = Vec<u64>),
        (status = 404, description = "Namespace query completed. No blobs found", body = Vec<Label>),
        (status = 500, description = "Database error, failed to query", body = String),
    ),
    params(
        ("namespace" = String, Path, description = "Name of the namespace to query"),
        ("labels" = Vec<Label>, Query, description = "Labels to query"),
    )
)]
#[get("/api/v2/{namespace}")]
pub async fn query_blobs(
    data: web::Data<DB>,
    path: web::Path<String>,
    query: web::Query<Vec<Label>>,
) -> Result<impl Responder> {
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
        }
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}

#[utoipa::path(
    get,
    path = "/api/v2/{namespace}/stats",
    responses(
        (status = 200, description = "Successfully collected namespace stats", body = NamespaceStats),
        (status = 500, description = "Database error, failed to get stats", body = String)
    ),
    params(
        ("namespace" = String, Path, description = "Name of the namespace to query"),
    )
)]
#[get("/api/v2/{namespace}/stats")]
pub async fn namespace_stats(data: web::Data<DB>, path: web::Path<String>) -> Result<impl Responder> {
    let name = path.into_inner();
    let namespace = match data.open_namespace(&name) {
        Ok(ns) => ns,
        Err(e) => return Err(actix_web::error::ErrorInternalServerError(e)),
    };
    match namespace.stats() {
        Ok(stats) => Ok(HttpResponse::Ok().json(stats)),
        Err(e) => Err(actix_web::error::ErrorInternalServerError(e)),
    }
}
