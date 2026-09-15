// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Health of an electrical component, judged from one telemetry message.

use std::collections::HashSet;

use crate::client::proto::common::microgrid::electrical_components::{
    ElectricalComponentStateCode, ElectricalComponentTelemetry,
};

/// A component's health, judged from one telemetry message.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Health {
    /// No state snapshot carries an error and every state code is in the
    /// healthy set.
    Healthy,
    /// A state snapshot carries an error or a state code outside the
    /// healthy set.
    Unhealthy,
    /// A state code this crate does not know; the component counts as
    /// unhealthy.
    UnknownStateCode(i32),
}

/// The health `telemetry` reports: `Healthy` while no state snapshot
/// carries an error and every state code is in `healthy_state_codes`.
pub(crate) fn health(
    telemetry: &ElectricalComponentTelemetry,
    healthy_state_codes: &HashSet<ElectricalComponentStateCode>,
) -> Health {
    for snapshot in &telemetry.state_snapshots {
        if !snapshot.errors.is_empty() {
            return Health::Unhealthy;
        }
        for &code in &snapshot.states {
            match ElectricalComponentStateCode::try_from(code) {
                Ok(state) => {
                    if !healthy_state_codes.contains(&state) {
                        return Health::Unhealthy;
                    }
                }
                Err(_) => return Health::UnknownStateCode(code),
            }
        }
    }
    Health::Healthy
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::client::proto::common::microgrid::electrical_components::ElectricalComponentStateSnapshot;

    fn telemetry(states: Vec<i32>, errors: usize) -> ElectricalComponentTelemetry {
        ElectricalComponentTelemetry {
            electrical_component_id: 7,
            state_snapshots: vec![ElectricalComponentStateSnapshot {
                states,
                errors: (0..errors).map(|_| Default::default()).collect(),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn healthy_codes() -> HashSet<ElectricalComponentStateCode> {
        HashSet::from([ElectricalComponentStateCode::Ready])
    }

    #[test]
    fn listed_state_without_errors_is_healthy() {
        let data = telemetry(vec![ElectricalComponentStateCode::Ready as i32], 0);
        assert_eq!(health(&data, &healthy_codes()), Health::Healthy);
    }

    #[test]
    fn no_state_snapshot_is_healthy() {
        let data = ElectricalComponentTelemetry {
            electrical_component_id: 7,
            ..Default::default()
        };
        assert_eq!(health(&data, &healthy_codes()), Health::Healthy);
    }

    #[test]
    fn error_makes_a_listed_state_unhealthy() {
        let data = telemetry(vec![ElectricalComponentStateCode::Ready as i32], 1);
        assert_eq!(health(&data, &healthy_codes()), Health::Unhealthy);
    }

    #[test]
    fn unlisted_state_is_unhealthy() {
        let data = telemetry(vec![ElectricalComponentStateCode::Error as i32], 0);
        assert_eq!(health(&data, &healthy_codes()), Health::Unhealthy);
    }

    #[test]
    fn unknown_state_code_is_reported_with_its_code() {
        let data = telemetry(vec![9999], 0);
        assert_eq!(
            health(&data, &healthy_codes()),
            Health::UnknownStateCode(9999)
        );
    }
}
