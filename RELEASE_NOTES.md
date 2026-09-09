# Frequenz Microgrid Release Notes

## Summary

Formulas are now evaluated by the logical-meter actor against resampled snapshots, so composition no longer subscribes eagerly and can freely mix metrics.

## Upgrading

- `Formula<Q>` is now a struct wrapping a formula-engine expression, with one type parameter instead of `Formula<QOut, QIn1, QIn2>`; its enum variants and the `FormulaSubscriber` trait are gone. Composition (`coalesce`, `min`, `max`, `avg`) no longer returns `Result`: drop the `?`.
- `FormulaOperand` is replaced by `Operand<Q>`: a formula or a constant of the same quantity. An external `broadcast::Receiver` can no longer be used as an operand, and two formulas can no longer be multiplied or divided. `avg` takes `Vec<impl Into<Operand<Q>>>`, so an empty list needs its type spelled out, e.g. `Vec::<Formula<Power>>::new()`.
- `avg` now yields `None` when any operand is `None`. Before, it averaged the operands that had a value. `min`, `max`, `+` and `-` already yielded `None` in that case.
- `Metric::FormulaType` is replaced by `Metric::KIND: FormulaKind`.
- `Formula`'s `Display` output changed: every component leaf now carries its metric, e.g. `#2:AC_POWER_ACTIVE`, nested operators are parenthesised and `0.0` renders as `0`.
- `ErrorKind::DroppedUnusedFormulas` is removed; the actor no longer uses an error to drive cleanup.
- `frequenz-microgrid-formula-engine` 0.2 is required.
- The first samples of a new subscription may be `None`. `subscribe()` returns as soon as the request reaches the logical meter; the component subscriptions complete later, and a formula emits `None` while one of its components is still pending. Do not treat the first sample as authoritative.
- A `COALESCE` emits `None` for about one tick when its primary stops delivering and its fallback had been dropped as unread. The fallback is subscribed again and delivers from the next tick.
- `subscribe()` no longer reports telemetry-subscription failures: they are logged and retried on the next tick; `subscribe()` fails only when the logical meter is gone.
- `Quantity` is now sealed (it has a crate-private supertrait), so downstream implementations of `Quantity` no longer compile.

## New Features

- Formulas are evaluated by the logical-meter actor against one resampled snapshot per tick, so composed formulas never need timestamp synchronisation and can mix metrics.
- The logical meter subscribes to component telemetry on demand: only components an evaluation reads are subscribed, `COALESCE` fallbacks stay unsubscribed while the primary delivers, and unread components are dropped after `LogicalMeterConfig::with_unsubscribe_after_intervals` ticks (default 3).
- `quantity::ApparentPower` and `metric::AcPowerApparent`.
- `Key` and `FormulaExpr` expose a formula's expression; `Formula::expr()` returns it, and `Expr`, `Function` and `Op` are re-exported so callers can name the type behind it and match on its variant payloads.
- `test-utils`: `MockMicrogridApiClient::open_telemetry_streams()` reports which components have an open telemetry stream, and `wait_for_open_streams()` polls it. A `with_silence_after_metrics` component's stream now ends when its last receiver is dropped instead of staying open forever.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
