# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- This updates the component-graph version to 0.5, which treats unhandled component categories as pass-through.

- The component-graph configuration is no longer hardcoded. `LogicalMeterConfig` now carries a `ComponentGraphConfig` (set via `with_component_graph_config`), defaulting to `ComponentGraphConfig::default()` from the graph crate. This is a behavior change for callers that relied on the previously hardcoded values: the old defaults were `allow_component_validation_failures = true`, `allow_unconnected_components = true`, plus the pre-0.5 formula generators (which behave like `prefer_meters_in_component_formulas = false` and `include_phantom_loads_in_consumer_formula = true`). To preserve the old behavior, pass a `ComponentGraphConfig` built with those four flags via `LogicalMeterConfig::with_component_graph_config`.

- `MicrogridClientHandle::try_new`, `LogicalMeterHandle::try_new`, and `Microgrid::try_new` no longer return an error when the microgrid API server is unreachable at startup or when the server returns data that doesn't yet form a valid component graph; instead they wait for the server to recover. Callers that relied on a quick failure to detect a misconfigured or unavailable endpoint should wrap the call in `tokio::time::timeout` (or equivalent) to bound the wait. URL validation still fails fast: a malformed endpoint URL is still surfaced as `ConnectionFailure` from `MicrogridClientHandle::try_new`, and an invalid `LogicalMeterConfig` still surfaces synchronously from `LogicalMeterHandle::try_new`.

## New Features

- The microgrid client now tolerates the API server being absent or returning incomplete data at startup. `MicrogridClientHandle::try_new` establishes the gRPC connection lazily, so it succeeds regardless of whether the server is reachable; transient stream errors are then handled by the existing per-stream retry loop. `LogicalMeterHandle::try_new` (and therefore `Microgrid::try_new`) wraps the entire component-graph setup — listing components, listing connections, and building the graph — in a single retry loop that sleeps 3 seconds between attempts, so applications block waiting for the server and a valid graph instead of exiting with an error.

- `Bounds::combine_parallel`, `Bounds::intersect`, and `Bounds::merge_if_overlapping` are now public, allowing external callers to combine bounds without going through higher-level types.
- Put test utils under a feature gate.
- Added `MockMicrogridApiClient::augment_electrical_component_bounds`: It captures requests so that these can be used in test cases. Obtain the list of captured requests using `MockMicrogridApiClient::augment_bounds_calls_handle` (also new).
- Added `MockComponent.add_component_bounds`: It allows to add metric bounds to a mock component.

- `LogicalMeterConfig::with_component_graph_config` lets callers pass a custom `ComponentGraphConfig` to the underlying graph builder (e.g. to enable phantom loads in the consumer formula or to flip the meter-vs-device preference for per-category formulas).

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
