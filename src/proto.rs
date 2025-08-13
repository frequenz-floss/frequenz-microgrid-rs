// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Generated protobuf modules for the Frequenz API.

mod graph;

#[allow(clippy::doc_lazy_continuation, clippy::doc_overindented_list_items)]
pub mod common {
    pub mod v1alpha8 {
        pub mod grid {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1alpha8.grid");
        }

        pub mod microgrid {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1alpha8.microgrid");
            pub mod electrical_components {
                #![allow(clippy::derive_partial_eq_without_eq)]
                tonic::include_proto!(
                    "frequenz.api.common.v1alpha8.microgrid.electrical_components"
                );
            }
            pub mod sensors {
                #![allow(clippy::derive_partial_eq_without_eq)]
                tonic::include_proto!("frequenz.api.common.v1alpha8.microgrid.sensors");
            }
        }

        pub mod metrics {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1alpha8.metrics");
        }

        pub mod types {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1alpha8.types");
        }
    }
}

#[allow(clippy::doc_lazy_continuation, clippy::doc_overindented_list_items)]
pub mod microgrid {
    pub mod v1alpha18 {
        #![allow(clippy::derive_partial_eq_without_eq)]
        tonic::include_proto!("frequenz.api.microgrid.v1alpha18");
    }
}
