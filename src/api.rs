use actix_web::{HttpServer, App, Responder, web};
use bytes::BytesMut;
use futures_util::stream::StreamExt;
use crate::internal::*;

pub struct ApiServer {
    address: String, 
    port: u16,
    db: DB,
}

impl ApiServer {
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
            .app_data(web::Data::new(db))
            .route("/", web::to(Self::index))
            .route("/api/v1/{namespace}/insert", web::to(Self::insert))
            .route("/api/v1/{namespace}/query", web::to(Self::query))
            .route("/api/v1/{namespace}/get/{id}", web::to(Self::get))
            .route("/api/v1/{namespace}/delete/{id}", web::to(Self::delete))
        })
        .bind((self.address.to_string(), self.port))?
        .run()
        .await
    }
    
    async fn index(_data: web::Data<DB>) -> impl Responder {
        "Hello"
    }

    async fn insert(data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<Label>>, mut payload: web::Payload) -> actix_web::Result<impl Responder> {
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

    async fn get(data: web::Data<DB>, path: web::Path<(String, u64)>) -> actix_web::Result<impl Responder> {
        let (namespace, id) = path.into_inner();
        let namespace = data.open_namespace(&namespace).map_err(|e| actix_web::error::ErrorBadRequest(e))?;
        let blob = namespace.get(id).map_err(|e| actix_web::error::ErrorServiceUnavailable(e))?;
        match blob {
            Some(b) => Ok(b),
            None => Err(actix_web::error::ErrorNotFound(format!("{id} not found"))),
        }
    }

    async fn query(data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<Label>>) -> actix_web::Result<impl Responder> {
        let namespace = path.into_inner();
        let labels = query.into_inner();
        let namespace = data.open_namespace(&namespace).map_err(|e| actix_web::error::ErrorBadRequest(e))?;

        let results = namespace.query(labels).map_err(|e| actix_web::error::ErrorBadGateway(e))?;
        Ok(web::Json(results))
    }

    async fn delete(data: web::Data<DB>, path: web::Path<String>, query: web::Query<Vec<u64>>) -> actix_web::Result<impl Responder> {
        let namespace = path.into_inner();
        let ids = query.into_inner();
        let namespace = data.open_namespace(&namespace).map_err(|e| actix_web::error::ErrorBadRequest(e))?;
        let _ = namespace.delete_objects(ids).map_err(|e| actix_web::error::ErrorServiceUnavailable(e))?;
        Ok("success")
    }
}