use actix_web::HttpRequest;
use serde_derive::{Serialize, Deserialize};
use utoipa::{ToResponse, ToSchema, schema};

/// Mango Chainsaw API request information
#[derive(Clone, Debug, Serialize, Deserialize, ToResponse, ToSchema)]
#[schema(
    example = json!(RequestInfo::example())
)]
pub(crate) struct RequestInfo {
    /// Request headers
    /// 
    /// Headers that are sensitive will be sanitized
    pub headers: Vec<String>,

    /// Request path
    pub path: String,

    /// Request method
    pub method: String,

    /// Request query parameters
    pub query: String,
}

impl RequestInfo {
    /// Example for json! in schema
    pub(super) fn example() -> Self {
        Self {
            headers: vec![ format!("X-Custom-Header = some-value") ],
            path: format!("/"),
            method: format!("GET"),
            query: format!("?"),
        }
    }
}

impl From<&HttpRequest> for RequestInfo {
    fn from(value: &HttpRequest) -> Self {
        let path = value.path().to_string();
        let method = value.method().to_string();
        let query = value.query_string().to_string();
        let headers = value.headers().into_iter().map(|(k, v)| {
            let name = k.to_string();
            let value;
            if v.is_sensitive() {
                value = format!("SENSITIVE");
            } else {
                value = match v.to_str() {
                    Ok(s) => format!("{s}"),
                    Err(e) => format!("{e}"),
                }
            }
            format!("{name} = {value}")
        }).collect();
        Self {
            headers, path, method, query
        }
    }
}