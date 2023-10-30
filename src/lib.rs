pub mod api;
pub mod db;
pub mod errors;
pub mod label;
pub mod namespace;

#[cfg(test)]
mod tests;

pub(crate) mod internal {
    pub use crate::errors::MangoChainsawError;
    pub use crate::errors::Result;

    pub use crate::api::*;
    pub use crate::db::DB;
    pub use crate::label::Label;
    pub use crate::namespace::Namespace;
}

pub mod prelude {
    pub use crate::db::DB;
    pub use crate::errors::MangoChainsawError;
}
