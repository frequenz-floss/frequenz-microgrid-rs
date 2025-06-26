// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Generated protobuf modules for the Frequenz API.

#[allow(clippy::doc_lazy_continuation)]
pub mod common {
    pub mod v1 {
        #![allow(clippy::derive_partial_eq_without_eq)]
        tonic::include_proto!("frequenz.api.common.v1");

        pub mod grid {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1.grid");
        }

        pub mod microgrid {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1.microgrid");
            pub mod components {
                #![allow(clippy::derive_partial_eq_without_eq)]
                tonic::include_proto!("frequenz.api.common.v1.microgrid.components");
            }
            pub mod sensors {
                #![allow(clippy::derive_partial_eq_without_eq)]
                tonic::include_proto!("frequenz.api.common.v1.microgrid.sensors");
            }
        }

        pub mod metrics {
            #![allow(clippy::derive_partial_eq_without_eq)]
            tonic::include_proto!("frequenz.api.common.v1.metrics");
        }
    }
}

#[allow(clippy::doc_lazy_continuation)]
pub mod microgrid {
    pub mod v1 {
        #![allow(clippy::derive_partial_eq_without_eq)]
        tonic::include_proto!("frequenz.api.microgrid.v1");
    }
}
