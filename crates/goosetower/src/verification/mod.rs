//! Source-controlled verification infrastructure. This module is never wired into
//! the production gateway or runtime server.

pub mod fake_source;
#[cfg(feature = "p02-verification")]
pub mod tower_observer;
