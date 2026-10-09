# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `Metric::FormulaType` is removed. No public API returned it; use the `Formula<M::QuantityType>` that the `LogicalMeterHandle` methods return.
- `ErrorKind::DroppedUnusedFormulas` is removed. No public API returned it, so remove any match arm or comparison on it.
- In a formula from a `LogicalMeterHandle` method, a NaN or infinite component value or intermediate result is now treated as missing data: `COALESCE` moves past it to its next operand, and `+`, `-`, `MIN` and `MAX` give `None`.
- A component-graph formula with more than about 1000 `+` and `-` operators is now rejected: the `LogicalMeterHandle` method that builds it returns `ErrorKind::FormulaEngineError`. This can happen at large sites, for example:
  - for the consumer formula with phantom loads included, at a few hundred metered branches;
  - for a battery or PV formula over a meter with more than about 510 inverters.
- The formulas from `LogicalMeterHandle::consumer` and `LogicalMeterHandle::producer` are no longer clamped at zero, because component graph 0.6.3 removed the clamps: they replaced valid values of metrics other than active power, such as reactive power, with zero.
  - The consumer formula can now be negative. For active power, combine it with `max` and a zero `Power` to get the old result. With phantom loads included, the consumer formula is unchanged.
  - A producer that draws power now adds a positive value instead of zero, so the producer total can be positive. For example, with PV and a CHP behind separate meters, PV producing 10 kW and the CHP drawing 2 kW used to give -10 kW and now give -8 kW.
  - Combining the producer formula with `min` and a zero `Power` gets close to the old result, but differs from it when one producer draws power while another produces.
- `Quantity` can no longer be implemented outside this crate. Use the quantity types this crate provides.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
