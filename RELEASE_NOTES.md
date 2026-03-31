# Frequenz Microgrid Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- `LogicalMeterConfig` instances can't be created directly anymore, and need to be created using the `LogicalMeterConfig::new` method.  This helps avoid future breaking changes, as we add more config parameters.

## New Features

- It is now possible to change the default resampling function, and to override the resampling function for specific metrics.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
