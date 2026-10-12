# Frequenz Microgrid Release Notes

## Summary

The logical meter now evaluates formulas once per resampling tick, against one snapshot of component data. Composed formulas no longer subscribe to each operand separately.

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
- `Quantity` can no longer be implemented outside this crate. Use the quantity types this crate provides.

## New Features

- All operands of a composed formula, including operands of different metrics, are evaluated against the same tick's snapshot instead of being lined up by timestamp.
- `Formula<Q> * Percentage` scales a formula by a percentage.
- `Formula<Q>` implements `Clone`, and `avg` accepts constants as well as formulas.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
