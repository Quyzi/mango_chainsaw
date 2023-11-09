use crate::internal::*;
use utoipa::{OpenApi, ToResponse, ToSchema};
use utoipa_rapidoc::RapiDoc;
use utoipa_redoc::Redoc;
use utoipa_swagger_ui::SwaggerUi;


#[derive(OpenApi)]
#[openapi(
    info(
        title = "Mango Chainsaw",
        description = "Blob storage for people with weird eyebrows"
    ),
    paths(
        super::namespaces::list_namespaces,
        super::namespaces::get_namespace,
        super::namespaces::delete_namespace,

        super::blobs::get_blob,
        super::blobs::delete_blob,
        super::blobs::search_blobs,

        super::labels::get_label,
        super::labels::delete_label,
        super::labels::get_blob_labels,
    ),
    components(
        schemas(

        ),
        responses(
            super::namespaces::Response,
            super::blobs::BlobResponse,
            super::labels::LabelResponse,
        ),
    )
)]
pub(crate) struct ApiDocumentation;