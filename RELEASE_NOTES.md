# Frequenz Microgrid Release Notes

## Summary

The logical meter now evaluates formulas once per resampling tick, against one snapshot of component data. Composed formulas no longer subscribe to each operand separately, and only the components a formula actually reads are subscribed.

## Upgrading

- `Formula<Q>` is now a struct with one type parameter instead of the `Formula<QOut, QIn1, QIn2>` enum.
  - The `FormulaSubscriber` trait is gone: `subscribe()` is a method on `Formula`, so remove `use frequenz_microgrid::FormulaSubscriber`. Use `Formula<Q>` where you used `FormulaSubscriber` bounds or trait objects. A type that implemented `FormulaSubscriber` can no longer be used in a formula: subscribe to it on its own and combine the streams yourself.
  - The enum variants are gone, `Formula::Subscriber`, `Formula::Multiply` and `Formula::Divide` included. Build formulas with the operators and methods instead: a formula can be multiplied or divided by an `f32`, or multiplied by a `Percentage`.
  - `coalesce`, `min`, `max` and `avg` no longer return `Result`: drop the `?`.
- `FormulaOperand<Q>`, the operand of `+`, `-`, `coalesce`, `min`, `max` and `avg`, is now public. It holds a formula or a constant of the same quantity.
  - An external `broadcast::Receiver` can no longer be used as an operand. Subscribe to the formula and combine the streams yourself.
  - `avg` takes `Vec<impl Into<FormulaOperand<Q>>>`, so an empty list needs its type spelled out, e.g. `Vec::<Formula<Power>>::new()`.
- A formula that combines formulas from different logical meters can still be built, but its `subscribe()` now fails with `ErrorKind::FormulaEngineError`. Each `Microgrid::try_new` and each `LogicalMeterHandle::try_new` call creates its own logical meter. Clones of a handle share it, and so does a `Microgrid` built from that handle with `new_from_handles`. Subscribe to each formula on its own and combine the streams yourself.
- `Metric::FormulaType` is removed. No public API returned it; use the `Formula<M::QuantityType>` that the `LogicalMeterHandle` methods return.
- `Formula`'s `Display` output changed.
  - The `METRIC_X::(...)` wrapper around each formula from the component graph is gone, and every component carries its metric instead, e.g. `#2:AC_POWER_ACTIVE`.
  - Parentheses appear only where precedence or left associativity needs them.
  - Constants render as plain numbers in the quantity's base unit: `5 W` is now `5`, `Power::from_kilowatts(100.0)` is `100000`, and `0.0` is `0`.
- `ErrorKind::DroppedUnusedFormulas` is removed. No public API returned it, so remove any match arm or comparison on it.
- A NaN or infinite component value, constant or intermediate result is now treated as missing data. `COALESCE` moves past it to its next operand, `AVG` skips it, and the other operators give `None`.
- A component-graph formula with more than about 1000 `+` and `-` operators is now rejected: the `LogicalMeterHandle` method that builds it returns `ErrorKind::FormulaEngineError`. This can happen at large sites, for example:
  - for the consumer formula with phantom loads included, at a few hundred metered branches;
  - for a battery or PV formula over a meter with more than about 510 inverters.
- The formulas from `LogicalMeterHandle::consumer` and `LogicalMeterHandle::producer` are no longer clamped at zero, because component graph 0.6.3 removed the clamps: they replaced valid values of metrics other than active power, such as reactive power, with zero.
  - The consumer formula can now be negative. For active power, combine it with `max` and a zero `Power` to get the old result. With phantom loads included, the consumer formula is unchanged.
  - A producer that draws power now adds a positive value instead of zero, so the producer total can be positive. For example, with PV and a CHP behind separate meters, PV producing 10 kW and the CHP drawing 2 kW used to give -10 kW and now give -8 kW.
  - Combining the producer formula with `min` and a zero `Power` gets close to the old result, but differs from it when one producer draws power while another produces.
- The first samples of a new subscription may be `None`. `subscribe()` returns as soon as the request is queued, and the component subscriptions complete later. A formula emits `None` while a component it reads is still being subscribed, and on the first tick after that if the component has not sent anything yet. A `None` in the first samples does not always mean the data is missing.
- A `COALESCE` fallback is subscribed only once the primary's value is missing, for example because the primary stopped sending data, does not send the metric, or sends NaN. When the primary's value is missing, a `COALESCE` can emit `None` for a few ticks while its fallback is subscribed. The fallback is used once it sends data. If it sends nothing by the second tick after its subscription completes, the `COALESCE` moves on to its next operand. When the primary has a value again, the fallback stays subscribed until no formula has read it for three ticks in a row, by default.
- If the microgrid client stops running, components that are not subscribed yet can no longer be subscribed. A formula sends `None` on every tick on which it reads such a component, and its stream stays open.
- `Quantity` can no longer be implemented outside this crate. Use the quantity types this crate provides.

## New Features

- All operands of a composed formula, including operands of different metrics, are evaluated against the same tick's snapshot instead of being lined up by timestamp.
- The logical meter subscribes to component telemetry on demand. Only the components a formula reads are subscribed, and a `COALESCE` fallback is subscribed only once the primary's value is missing. A component is unsubscribed on the third tick in a row in which no formula reads it; change this with `LogicalMeterConfig::with_unsubscribe_after_idle_intervals`.
- `Formula<Q> * Percentage` scales a formula by a percentage.
- `Formula<Q>` implements `Clone`, and `avg` accepts constants as well as formulas.
- `test-utils`: `MockMicrogridApiClient::open_telemetry_streams()` reports which components have an open telemetry stream, and `wait_for_open_streams()` waits until that set matches an expected one. A `with_silence_after_metrics` component's stream closes when its last receiver is dropped while it still has samples to send; a receiver dropped after the component went silent leaves the stream open.

## Bug Fixes

- On the tick after a wall-clock jump, a formula that reads a component now sends `None`. Before, it was computed from resamplers with no data, so `COALESCE(#1, 0)` sent a made-up 0.
