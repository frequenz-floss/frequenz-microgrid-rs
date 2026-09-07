# Frequenz Microgrid Release Notes

## Summary

Formulas are now evaluated by the logical-meter actor against resampled snapshots, so composition no longer subscribes eagerly and can freely mix metrics.

## Upgrading

- `Formula<Q>` is now a struct wrapping a formula-engine expression; its enum variants, `FormulaOperand`, `FormulaSubscriber`, `GraphFormula`, `AggregationFormula`, `CoalesceFormula` and `GraphFormulaProvider` are gone. Composition (`coalesce`, `min`, `max`, `avg`) no longer returns `Result`: drop the `?`.
- `Metric::FormulaType` is replaced by `Metric::KIND: FormulaKind`.
- `Formula`'s `Display` output changed: every component leaf now carries its metric, e.g. `#2:AC_POWER_ACTIVE`, nested operators are parenthesised and `0.0` renders as `0`.
- `ErrorKind::DroppedUnusedFormulas` is removed; the actor no longer uses an error to drive cleanup.
- `frequenz-microgrid-formula-engine` 0.2 is required.

## New Features

- Formulas are evaluated by the logical-meter actor against one resampled snapshot per tick, so composed formulas never need timestamp synchronisation and can mix metrics.
- The logical meter subscribes to component telemetry on demand: only components an evaluation reads are subscribed, `COALESCE` fallbacks stay unsubscribed while the primary delivers, and unread components are dropped after `LogicalMeterConfig::with_unsubscribe_after_intervals` ticks (default 3).
- `quantity::ApparentPower` and `metric::AcPowerApparent`.
- `Key` and `FormulaExpr` expose a formula's expression; `Formula::expr()` returns it, and `Expr` is re-exported so callers can name the type behind them.
- `test-utils`: `MockMicrogridApiClient::open_telemetry_streams()` reports which components have an open telemetry stream.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
