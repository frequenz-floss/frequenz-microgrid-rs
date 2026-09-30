// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Status updates for set-power requests.

use chrono::{DateTime, Utc};

/// A status update for an accepted set-power request, as streamed back by
/// the Microgrid API.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SetPowerUpdate {
    /// The setpoint was issued to the component. This doesn't mean the
    /// component has reached the target power yet.
    Success { valid_until: Option<DateTime<Utc>> },
    /// Applying the setpoint failed; the component is not at the target
    /// power.
    Failed,
    /// The request was overridden by a newer one for the same component.
    Overridden,
    /// A status not expected after acceptance, e.g. one added in a newer
    /// API version, as its raw proto value.
    Other(i32),
}

impl SetPowerUpdate {
    /// Whether this update ends the request.
    pub fn is_final(&self) -> bool {
        matches!(self, Self::Success { .. } | Self::Failed | Self::Overridden)
    }
}
