// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Generated protobuf modules for the Frequenz API.

// Including the generated protobuf code.
#![allow(
    clippy::doc_lazy_continuation,
    clippy::module_inception,
    dead_code,
    clippy::enum_variant_names
)]
mod pb {
    tonic::include_proto!("proto_v1_alpha18");
}

// Only export what we need
pub use pb::frequenz::api::common::v1alpha8 as common;
pub use pb::frequenz::api::microgrid::v1alpha18 as microgrid;
#[cfg(any(test, feature = "test-utils"))]
pub use pb::google;

mod electrical_component;
mod graph;
