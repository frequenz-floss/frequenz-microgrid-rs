# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `LogicalMeterConfig` instances can't be created directly anymore, and need to be created using the `LogicalMeterConfig::new` method.  This helps avoid future breaking changes, as we add more config parameters.

- Formula streaming methods in the `LogicalMeterHandle` no longer take metric as a function parameter, but expect a generic argument.  For example:

  *Old syntax:*
   ```rust
   let formula_grid = logical_meter.grid(metric::AcPowerActive)?;
   let formula_pv = logical_meter.pv(None, metric::AcVoltage)?;
   ```

  *New syntax:*
   ```rust
   let formula_grid = logical_meter.grid::<metric::AcPowerActive>()?;
   let formula_pv = logical_meter.pv::<metric::AcVoltage>(None)?;
   ```

## New Features

- It is now possible to change the default resampling function, and to override the resampling function for specific metrics.

- The resampler's `max_age_in_intervals` has also become configurable, through `LogicalMeterConfig`.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
