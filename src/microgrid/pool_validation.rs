// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Shared validation for pool component-ID selections.

use std::collections::BTreeSet;

use crate::Error;

/// Validates the `component_ids` a pool was constructed with against the set of
/// `all_matching` IDs of the right kind in the component graph.
///
/// `Some(ids)` must be a non-empty subset of `all_matching`; `None` ("all
/// components of this kind") is accepted as-is. `noun` names the component kind
/// in error messages (e.g. `"batteries"`).
pub(super) fn validate_pool_ids(
    component_ids: &Option<BTreeSet<u64>>,
    all_matching: &BTreeSet<u64>,
    noun: &str,
) -> Result<(), Error> {
    if let Some(ids) = component_ids {
        if ids.is_empty() {
            let e = "component_ids cannot be an empty set".to_string();
            tracing::error!("{e}");
            return Err(Error::invalid_component(e));
        }
        if !ids.is_subset(all_matching) {
            let e = format!("All component_ids {ids:?} must be {noun}.");
            tracing::error!("{e}");
            return Err(Error::invalid_component(e));
        }
    }
    Ok(())
}
