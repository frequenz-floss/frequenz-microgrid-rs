# Frequenz Microgrid Release Notes

## Summary

This release makes incremental improvements to the client and the quantity types.

## Upgrading

- The `MicrogridClientHandle::list_electrical_components` method now expects `ElectricalComponentCategory` enum values instead of `i32`, to filter by component category.

## New Features

- The new `MicrogridClientHandle::augment_electrical_component_bounds` method can be used to augment the bounds for specific metrics of electrical components.

- All methods on `Quantity` types are now `const`.

- `Quantity` types have two new methods `min` and `max`, similar to the `min` and `max` methods on fundamental numerical types.
