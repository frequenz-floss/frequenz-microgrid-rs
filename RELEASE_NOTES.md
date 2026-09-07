# Frequenz Microgrid Release Notes

## Summary

The logical meter now evaluates formulas once per resampling tick, against one snapshot of component data. Composed formulas no longer subscribe to each operand separately, and only the components a formula actually reads are subscribed.

## Upgrading

- `Formula<Q>` is now a struct with one type parameter instead of the `Formula<QOut, QIn1, QIn2>` enum. The `FormulaSubscriber` trait is gone: `subscribe()` is a method on `Formula`, so remove `use frequenz_microgrid::FormulaSubscriber`. Composition (`coalesce`, `min`, `max`, `avg`) no longer returns `Result`: drop the `?`.
- `FormulaOperand<Q>`, the operand of the composition methods, is now public. It holds a formula or a constant of the same quantity. An external `broadcast::Receiver` can no longer be used as an operand, and two formulas can no longer be multiplied or divided. `avg` takes `Vec<impl Into<FormulaOperand<Q>>>`, so an empty list needs its type spelled out, e.g. `Vec::<Formula<Power>>::new()`.
- Formulas from different logical meters (separately created `LogicalMeterHandle`s; clones of one handle share a meter) can no longer be combined. `subscribe()` on such a formula fails with `ErrorKind::FormulaEngineError`. Subscribe to each formula on its own and combine the streams yourself.
- `Metric::FormulaType` is removed.
- `Formula`'s `Display` output changed. The `METRIC_X::(...)` wrapper around each formula from the component graph is gone, and every component carries its metric instead, e.g. `#2:AC_POWER_ACTIVE`. Parentheses appear only where precedence or left associativity needs them. Constants render as plain numbers in the quantity's base unit: `5 W` is now `5`, `Power::from_kilowatts(100.0)` is `100000`, and `0.0` is `0`.
- `ErrorKind::DroppedUnusedFormulas` is removed.
- The first samples of a new subscription may be `None`. `subscribe()` returns as soon as the request is queued, and the component subscriptions complete later. A formula emits `None` while one of its components is still being subscribed, and on the first tick after that if the component has not sent anything yet. Do not treat the first sample as final.
- A `COALESCE` emits `None` for a short time when its primary stops sending data, because its fallback is not subscribed while the primary sends data. The fallback is then subscribed and is used once it sends data, usually one or two ticks later. If it sends nothing by the second tick after its subscription completes, the `COALESCE` moves on to its next operand.
- `subscribe()` no longer reports telemetry-subscription failures. They are logged and retried on the next tick. `subscribe()` fails only when the logical meter is gone, or when the formula combines several logical meters.
- `Quantity` can no longer be implemented outside this crate.

## New Features

- All operands of a composed formula, including operands of different metrics, are evaluated against the same tick's snapshot instead of being lined up by timestamp.
- The logical meter subscribes to component telemetry on demand. Only the components a formula reads are subscribed, and a `COALESCE` fallback stays unsubscribed while the primary sends data. A component is unsubscribed on the third consecutive tick in which no formula reads it; change this with `LogicalMeterConfig::with_unsubscribe_after_intervals`.
- `Formula<Q> * Percentage` scales a formula by a percentage.
- `test-utils`: `MockMicrogridApiClient::open_telemetry_streams()` reports which components have an open telemetry stream, and `wait_for_open_streams()` waits until that set matches an expected one. A `with_silence_after_metrics` component's stream closes when its last receiver is dropped while it still has samples to send; a receiver dropped after the component went silent leaves the stream open.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
