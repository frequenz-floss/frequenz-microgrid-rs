# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `MicrogridClientHandle::try_new`, `LogicalMeterHandle::try_new`, and `Microgrid::try_new` no longer return an error when the microgrid API server is unreachable at startup or when the server returns data that doesn't yet form a valid component graph; instead they wait for the server to recover. Callers that relied on a quick failure to detect a misconfigured or unavailable endpoint should wrap the call in `tokio::time::timeout` (or equivalent) to bound the wait. URL validation still fails fast: a malformed endpoint URL is still surfaced as `ConnectionFailure` from `MicrogridClientHandle::try_new`, and an invalid `LogicalMeterConfig` still surfaces synchronously from `LogicalMeterHandle::try_new`.

## New Features

- The microgrid client now tolerates the API server being absent or returning incomplete data at startup. `MicrogridClientHandle::try_new` establishes the gRPC connection lazily, so it succeeds regardless of whether the server is reachable; transient stream errors are then handled by the existing per-stream retry loop. `LogicalMeterHandle::try_new` (and therefore `Microgrid::try_new`) wraps the entire component-graph setup — listing components, listing connections, and building the graph — in a single retry loop that sleeps 3 seconds between attempts, so applications block waiting for the server and a valid graph instead of exiting with an error.

- `Bounds::combine_parallel`, `Bounds::intersect`, and `Bounds::merge_if_overlapping` are now public, allowing external callers to combine bounds without going through higher-level types.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
