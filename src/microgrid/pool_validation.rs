// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Shared validation for pool component-ID selections.

use std::collections::BTreeSet;

use crate::Error;

/// Validates the `component_ids` a pool was constructed with against the set of
/// `all_matching` IDs of the right kind in the component graph.
///
/// When `Some(ids)`, every ID must be one of `all_matching`. An empty selection
/// — either `Some({})` or `None` over a microgrid with no components of this
/// kind — is allowed and yields an empty pool (empty bounds, zero power).
/// `noun` names the component kind in error messages (e.g. `"batteries"`).
pub(super) fn validate_pool_ids(
    component_ids: &Option<BTreeSet<u64>>,
    all_matching: &BTreeSet<u64>,
    noun: &str,
) -> Result<(), Error> {
    if let Some(ids) = component_ids
        && !ids.is_subset(all_matching)
    {
        let e = format!("All component_ids {ids:?} must be {noun}.");
        return Err(Error::invalid_component(e));
    }
    Ok(())
}
