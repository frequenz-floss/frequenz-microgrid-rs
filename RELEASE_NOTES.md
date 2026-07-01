# Frequenz Microgrid Release Notes

## Bug Fixes

- Fixed streaming of the `AcFrequency` metric (e.g. `Microgrid`'s grid frequency). Subscribing to a `Frequency` formula previously failed because the logical meter actor didn't route the `Frequency` quantity.
