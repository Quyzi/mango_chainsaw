use actix_web::web;
use utoipa::OpenApi;
use utoipa_rapidoc::RapiDoc;
use utoipa_redoc::Redoc;
use utoipa_swagger_ui::SwaggerUi;

/// Configure actix server to use mango_chainsaw api
pub fn configure(cfg: &mut web::ServiceConfig) {
    let openapi = super::documentation::ApiDocumentation::openapi();

    cfg.service(super::namespaces::get_namespace)
        .service(super::namespaces::list_namespaces)
        .service(super::namespaces::delete_namespace)
        .service(super::blobs::get_blob)
        .service(super::blobs::delete_blob)
        .service(super::blobs::search_blobs)
        .service(super::labels::get_blob_labels)
        .service(super::labels::get_label)
        .service(super::labels::delete_label)
        .service(
            SwaggerUi::new("/swagger-ui/{_:.*}")
                .url("/api-docs/openapi.json", openapi.clone())
        )
        .service(Redoc::new(openapi.clone()))
        .service(RapiDoc::new("/api-docs/openapi.json").path("/rapidoc"));

}