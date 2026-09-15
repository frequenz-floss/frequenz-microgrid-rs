# Frequenz Microgrid Release Notes

## Summary

Formulas are now evaluated by the logical-meter actor against resampled snapshots, so composition no longer subscribes eagerly and can freely mix metrics.

## Upgrading

- `Formula<Q>` is now a struct wrapping a formula-engine expression, with one type parameter instead of `Formula<QOut, QIn1, QIn2>`; its enum variants and the `FormulaSubscriber` trait are gone. Composition (`coalesce`, `min`, `max`, `avg`) no longer returns `Result`: drop the `?`.
- `FormulaOperand` is replaced by `Operand<Q>`: a formula or a constant of the same quantity. An external `broadcast::Receiver` can no longer be used as an operand, and two formulas can no longer be multiplied or divided. `avg` takes `Vec<impl Into<Operand<Q>>>`, so an empty list needs its type spelled out, e.g. `Vec::<Formula<Power>>::new()`.
- `Metric::FormulaType` is replaced by `Metric::KIND: FormulaKind`.
- `Formula`'s `Display` output changed: every component leaf now carries its metric, e.g. `#2:AC_POWER_ACTIVE`, an operand is parenthesised only where precedence requires it and `0.0` renders as `0`.
- `ErrorKind::DroppedUnusedFormulas` is removed; the actor no longer uses an error to drive cleanup.
- `frequenz-microgrid-formula-engine` 0.2 is required.
- The first samples of a new subscription may be `None`. `subscribe()` returns as soon as the request reaches the logical meter; the component subscriptions complete later, and a formula emits `None` while one of its components is still pending. Do not treat the first sample as authoritative.
- A `COALESCE` emits `None` for about one tick when its primary stops delivering and its fallback had been dropped as unread. The fallback is subscribed again and delivers from the next tick.
- `subscribe()` no longer reports telemetry-subscription failures: they are logged and retried on the next tick; `subscribe()` fails only when the logical meter is gone.
- `Quantity` is now sealed (it has a crate-private supertrait), so downstream implementations of `Quantity` no longer compile.
- Every formula reads `None` for a component whose latest telemetry reports an error, or a state of Error, Unavailable, Off, SwitchingOff or SwitchingOn. Every other state the crate knows counts as healthy; a state code it does not know counts as unhealthy. Change the healthy set with `LogicalMeterConfig::with_healthy_state_codes`; a component that reports no state at all is never gated. The battery and PV pool telemetry trackers keep their own, narrower healthy sets; this setting does not change them.

## New Features

- Formulas are evaluated by the logical-meter actor against one resampled snapshot per tick, so composed formulas never need timestamp synchronisation and can mix metrics.
- The logical meter subscribes to component telemetry on demand: only components an evaluation reads are subscribed, `COALESCE` fallbacks stay unsubscribed while the primary delivers, and unread components are dropped after `LogicalMeterConfig::with_unsubscribe_after_intervals` ticks (default 3).
- `quantity::ApparentPower` and `metric::AcPowerApparent`.
- `Key`, `Source` and `FormulaExpr` expose a formula's expression. A `Key` is a component id plus a `Source`: the value of a metric (`Source::Value`), a limit of the sample's first bounds entry (`Source::LowerBound`, `Source::UpperBound`), or the component's health (`Source::Health`, 1 while healthy). They render as `#5:AC_POWER_ACTIVE`, `#5:BATTERY_SOC_PCT.lower` and `#5:HEALTH`. Bound and health leaves resample with the last value seen, not an average; the default resampling function and its per-metric overrides apply to metric values only. `Formula::expr()` returns the expression, and `Expr`, `Function` and `Op` are re-exported so callers can name the type behind it and match on its variant payloads.
- `test-utils`: `MockMicrogridApiClient::open_telemetry_streams()` reports which components have an open telemetry stream, and `wait_for_open_streams()` polls it. A `with_silence_after_metrics` component's stream now ends when its last receiver is dropped instead of staying open forever.
- `test-utils`: `MockComponent::with_soc(values, lower, upper)` emits battery SoC samples in percent carrying `(lower, upper)` as the sample's first bounds entry. `MockComponent::with_capacity(values)` emits capacity samples in watt-hours.
- `BatteryPool::soc()` returns a formula for the pool's capacity-weighted state of charge. `BatteryPool::capacity()` returns a formula for the pool's usable capacity in watt-hours. Each battery is weighted by its usable capacity, `capacity * max(upper - lower, 0) / 100`, using its SoC bounds. Each battery's SoC is first rescaled to its SoC bounds and clamped to 0-100 %. A battery contributes to neither formula while its capacity or SoC bounds are missing, while it is unhealthy, or while an inverter of its group is unhealthy or has sent nothing for the configured maximum sample age. A battery with no SoC reading drops out of `soc()` as well. `capacity()` and `soc()` read `None` when no battery contributes, and while any component of the pool is still being subscribed. They differ from the Python SDK in two ways: a result within floating-point noise of 100 % is not snapped to exactly 100 %, and `soc()` reads `None` where the SDK reports 0 when batteries contribute but their total usable capacity is zero.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
