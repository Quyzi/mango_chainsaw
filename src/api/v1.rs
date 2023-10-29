use actix_web::{HttpServer, App, Responder, web, HttpRequest, middleware::Logger};
use bytes::BytesMut;
use futures_util::stream::StreamExt;
use crate::internal::*;

pub struct ApiServerV1 {
    address: String, 
    port: u16,
    db: DB,
}

#[deprecated]
impl ApiServerV1 {
    pub fn new(db: DB, address: String, port: u16) -> Self {
        Self {
            address, db, port, 
        }
    }

    pub(crate) async fn run(&self) -> std::io::Result<()> {
        let this = self.db.clone();
        HttpServer::new(move || {
            let db = this.clone();
            App::new()
            .wrap(Logger::default())
            .app_data(web::Data::new(db))
            .route("/", web::to(Self::index))
            .route("/api/v1/{namespace}/insert", web::to(Self::insert))
            .route("/api/v1/{namespace}/query", web::to(Self::query))
            .route("/api/v1/{namespace}/get/{id}", web::to(Self::get))
            .route("/api/v1/{namespace}/delete/{id}", web::to(Self::delete))
        })
        .workers(8)
        .bind((self.address.to_string(), self.port))?
        .run()
        .await
    }
    
    async fn index(req: HttpRequest, _data: web::Data<DB>) -> impl Responder {
        let info = req.connection_info();
        let method = req.method().as_str();
        let reqpath = req.path();
        let host = info.host();
        log::debug!(target: "mango_chainsaw", "[{method}] {host}{reqpath}");
        "Hello"
    }

    async fn insert(req: HttpRequest, data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<Label>>, mut payload: web::Payload) -> actix_web::Result<impl Responder> {
        let info = req.connection_info();
        let method = req.method().as_str();
        let reqpath = req.path();
        let host = info.host();
        log::debug!(target: "mango_chainsaw", "[{method}] {host}{reqpath}");

        let namespace = path.into_inner();
        let labels = query.into_inner();

        let mut blob = BytesMut::new();
        while let Some(item) = payload.next().await {
            blob.extend_from_slice(&item?);
        }

        let namespace = data.open_namespace(&namespace).map_err(|e| actix_web::error::ErrorBadRequest(e))?;
        let id = namespace.insert(blob.freeze(), labels).map_err(|e| actix_web::error::ErrorServiceUnavailable(e))?;

        Ok(format!("{id}"))
    }

    async fn get(req: HttpRequest, data: web::Data<DB>, path: web::Path<(String, u64)>) -> actix_web::Result<impl Responder> {
        let info = req.connection_info();
        let method = req.method().as_str();
        let reqpath = req.path();
        let host = info.host();
        log::debug!(target: "mango_chainsaw", "[{method}] {host}{reqpath}");

        let (namespace, id) = path.into_inner();
        let namespace = data.open_namespace(&namespace).map_err(|e| actix_web::error::ErrorBadRequest(e))?;
        let blob = namespace.get(id).map_err(|e| actix_web::error::ErrorServiceUnavailable(e))?;
        match blob {
            Some(b) => Ok(b),
            None => Err(actix_web::error::ErrorNotFound(format!("{id} not found"))),
        }
    }

    async fn query(req: HttpRequest, data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<Label>>) -> actix_web::Result<impl Responder> {
        let info = req.connection_info();
        let method = req.method().as_str();
        let reqpath = req.path();
        let host = info.host();
        log::debug!(target: "mango_chainsaw", "[{method}] {host}{reqpath}");

        let namespace = path.into_inner();
        let labels = query.into_inner();
        let namespace = data.open_namespace(&namespace).map_err(|e| actix_web::error::ErrorBadRequest(e))?;

        let results = namespace.query(labels).map_err(|e| actix_web::error::ErrorBadGateway(e))?;
        Ok(web::Json(results))
    }

    async fn delete(req: HttpRequest, data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<u64>>) -> actix_web::Result<impl Responder> {
        let info = req.connection_info();
        let method = req.method().as_str();
        let reqpath = req.path();
        let host = info.host();
        log::debug!(target: "mango_chainsaw", "[{method}] {host}{reqpath}");

        let namespace = path.into_inner();
        let ids = query.into_inner();
        let namespace = data.open_namespace(&namespace).map_err(|e| actix_web::error::ErrorBadRequest(e))?;
        let _ = namespace.delete_objects(ids).map_err(|e| actix_web::error::ErrorServiceUnavailable(e))?;
        Ok("success")
    }
}