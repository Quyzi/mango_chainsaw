use super::response::RequestInfo;
use crate::internal::*;
use actix_web::{
    delete, get, options,
    web::{self},
    HttpResponse, Responder, HttpRequest,
};
use serde_derive::{Deserialize, Serialize};
use utoipa::{ToResponse, ToSchema, schema};

type Result<T> = actix_web::Result<T>;

const TAG: &str = "Namespace Management";

/// Namespace request Response
#[derive(Clone, Serialize, Deserialize, Debug, ToSchema, ToResponse)]
#[schema(
    example = json!(Response::example())
)]
pub(crate) struct Response {
    /// true if the request was successful
    pub success: bool,

    /// Response message
    pub message: String,

    /// Request details
    pub request: RequestInfo,

    /// Namespace name
    pub namespace: String,

    /// Response Data
    pub data: ResponseData,
}

/// Namespace response data
#[derive(Clone, Serialize, Deserialize, Debug, ToSchema, ToResponse)]
pub(crate) enum ResponseData {
    /// No data
    /// 
    /// 0 = Ok \
    /// 1+ = ?
    Default(u8),
    
    /// A list of Namespace names
    List(Vec<String>),
    
    /// Namespace stats
    Stats(NamespaceStats),
    
    /// Name of namespace deleted
    Delete(String),
}

impl Response {
    pub fn new(req: &HttpRequest) -> Self {
        Self {
            success: true,
            message: format!("message"),
            request: RequestInfo::from(req),
            namespace: format!("namespace"),
            data: ResponseData::Default(42),
        }
    }

    pub(self) fn example() -> Self {
        Self {
            success: true,
            message: format!("message"),
            request: RequestInfo::example(),
            namespace: format!("namespace"),
            data: ResponseData::List(vec![format!("cool_dogs"), format!("less_cool_cats")])
        }
    }

    pub(self) fn example_error() -> Self {
        let mut this = Self::example();
        this.success = false;
        this.message = format!("fatal exception: the extraplanar quantum wonton burrito failed to inject signal to the dynamic resonance bus in quasi-newtonian time. See the rhomboextrapulator documentation, page Π");
        this.data = ResponseData::Default(1);
        this
    }

    pub(self) fn example_stats() -> Self {
        let mut this = Self::example();
        this.message = format!("success");
        this.data = ResponseData::Stats(NamespaceStats { name: format!("namespace_name"), relations_checksum: 42069, blob_checksum: 69, labels_checksum: 420, blobs_count: 65536, labels_count: 8775309, relations_count: 42 });
        this
    }

    pub(self) fn example_delete() -> Self {
        let mut this = Self::example();
        this.message = format!("{}", this.namespace);
        this.data = ResponseData::Delete(this.namespace.to_owned());
        this
    }
}

/// Get a list of all current namespace names
/// 
#[utoipa::path(
    options, path = "/namespaces",
    tag = TAG,
    params(),
    responses(
        (status = 200, description = "success", body = Response, example = json!(Response::example())),
        (status = 500, description = "failure", body = Response, example = json!(Response::example_error()))
    )
)]
#[options("/namespaces")]
pub(crate) async fn list_namespaces(req: HttpRequest, data: web::Data<DB>) -> Result<impl Responder> {
    let mut response = Response::new(&req);
    match data.list_namespaces() {
        Ok(names) => {
            log::trace!(target: "mango_chainsaw", "API :: List namespaces got {} namespaces", names.len());
            response.message = format!("OK");
            response.namespace = format!("_");
            response.data = ResponseData::List(names);
            Ok(HttpResponse::Ok().json(response))
        },
        Err(e) => {
            log::error!(target: "mango_chainsaw", "API :: List namespaces got error {e}");
            response.success = false;
            response.message = e.to_string();
            response.data = ResponseData::Default(1);
            Ok(HttpResponse::InternalServerError().json(response))
        }
    }
}


/// Get a specific namespace stats
/// 
#[utoipa::path(
    get, path = "/namespaces/{namespace_name}",
    tag = TAG,
    params(
        ("namespace_name" = String, Path, description = "name of namespace", example = "namespace_name"),
    ),
    responses(
        (status = 200, description = "success", body = Response, example = json!(Response::example_stats())),
        (status = 500, description = "failure", body = Response, example = json!(Response::example_error()))
    )
)]
#[get("/namespaces/{namespace_name}")]
pub(crate) async fn get_namespace(req: HttpRequest, data: web::Data<DB>, path: web::Path<String>) -> Result<impl Responder> {
    let mut response = Response::new(&req);
    response.namespace = path.into_inner();
    let namespace = match data.open_namespace(&response.namespace) {
        Ok(ns) => ns,
        Err(e) => {
            response.success = false;
            response.message = format!("{e}");
            response.data = ResponseData::Default(1);
            log::error!(target: "mango_chainsaw", "API :: Get namespace {} stats error {e}", &response.namespace);
            return Ok(HttpResponse::InternalServerError().json(response))
        }
    };
    match namespace.stats() {
        Ok(stats) => {
            log::trace!(target: "mango_chainsaw", "API :: Get namespace stats {} OK", &response.namespace);
            response.message = format!("success");
            response.data = ResponseData::Stats(stats);
            Ok(HttpResponse::Ok().json(response))
        },
        Err(e) => {
            log::error!(target: "mango_chainsaw", "API :: Get namespace stats {} failed {e}", &response.namespace);
            response.success = false;
            response.message = e.to_string();
            response.data = ResponseData::Default(1);
            Ok(HttpResponse::InternalServerError().json(response))
        }
    }

}


/// Delete a specific namespace
/// 
#[utoipa::path(
    delete, path = "/namespaces/{namespace_name}",
    tag = TAG,
    params(
        ("namespace_name" = String, Path, description = "name of namespace", example = "namespace_name"),
    ),
    responses(
        (status = 200, description = "success", body = Response, example = json!(Response::example_delete())),
        (status = 500, description = "failure", body = Response, example = json!(Response::example_error()))
    )
)]
#[delete("/namespaces/{namespace_name}")]
pub(crate) async fn delete_namespace(req: HttpRequest, data: web::Data<DB>, path: web::Path<String>) -> Result<impl Responder> {
    let mut response = Response::new(&req);
    response.namespace = path.into_inner();
    let namespace = match data.open_namespace(&response.namespace) {
        Ok(ns) => ns,
        Err(e) => {
            log::error!(target: "mango_chainsaw", "API :: Delete namespace {} failed to open namespace {e}", &response.namespace);
            response.success = false;
            response.message = e.to_string();
            response.data = ResponseData::Default(1);
            return Ok(HttpResponse::InternalServerError().json(response))
        }
    };
    match data.drop_namespace(namespace) {
        Ok(_) => {
            response.message = format!("{} deleted", &response.namespace);
            response.data = ResponseData::Delete(response.namespace.to_owned());
            Ok(HttpResponse::Ok().json(response))
        },
        Err(e) => {
            log::error!(target: "mango_chainsaw", "API :: Delete namespace {} failed {e}", &response.namespace);
            response.success = false;
            response.message = e.to_string();
            response.data = ResponseData::Default(1);
            Ok(HttpResponse::InternalServerError().json(response))
        }
    }
}