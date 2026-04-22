// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the `Error` struct and the `ErrorKind` enum, which are
//! used to represent errors that can occur in the library.

/// A macro for defining the `ErrorKind` enum, the `Display` implementation for
/// it, and the constructors for the `Error` struct.
macro_rules! ErrorKind {
    ($(
        ($kind:ident, $ctor:ident)
    ),* $(,)?) => {
        /// The kind of error that occurred.
        #[derive(Debug, Clone, PartialEq)]
        pub enum ErrorKind {
            $(
                $kind,
            )*
        }

        impl std::fmt::Display for ErrorKind {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        Self::$kind => write!(f, "{}", stringify!($kind)),
                    )*
                }
            }
        }

        /// Constructors for [`Error`].
        impl Error {
            $(
                #[doc = concat!(
                    "Creates a new [`Error`] with the `",
                    stringify!($kind),
                    "` kind and the given description."
                )]
                pub(crate) fn $ctor(desc: impl Into<String>) -> crate::error::Error {
                    Self {
                        kind: ErrorKind::$kind,
                        desc: desc.into(),
                    }
                }
            )*

            /// Returns the kind of error that occurred.
            pub fn kind(&self) -> ErrorKind {
                self.kind.clone()
            }
        }
    };
}

ErrorKind!(
    (ComponentGraphError, component_graph_error),
    (ComponentDataError, component_data_error),
    (ConnectionFailure, connection_failure),
    (ChronoError, chrono_error),
    (DroppedUnusedFormulas, dropped_unused_formulas),
    (FormulaEngineError, formula_engine_error),
    (InvalidComponent, invalid_component),
    (Internal, internal),
    (APIServerError, api_server_error),
);

/// An error that can occur during the creation or traversal of a
/// [ComponentGraph][crate::ComponentGraph].
#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    kind: ErrorKind,
    desc: String,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.kind, self.desc)
    }
}

impl std::error::Error for Error {}

impl From<frequenz_microgrid_component_graph::Error> for Error {
    fn from(error: frequenz_microgrid_component_graph::Error) -> Self {
        Self::component_graph_error(error.to_string())
    }
}
