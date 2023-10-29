pub mod db;
pub mod errors;
pub mod label;
pub mod namespace;
pub mod api;

#[cfg(test)]
mod tests;

pub(crate) mod internal {
    pub use crate::errors::MangoChainsawError;
    pub use crate::errors::Result;

    pub use crate::db::DB;
    pub use crate::label::Label;
    pub use crate::namespace::Namespace;
    pub use crate::api::*;
}

pub mod prelude {
    pub use crate::db::DB;
    pub use crate::errors::MangoChainsawError;
}