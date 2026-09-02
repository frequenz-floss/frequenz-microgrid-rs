# Frequenz Microgrid Release Notes

## Upgrading

- The bundled Microgrid API protos moved to `frequenz-api-microgrid` v0.19.0 (`frequenz-api-common` v0.8.4). The re-exported `client::ElectricalComponentCategory` gained the `SteamBoiler` variant; an exhaustive `match` on it needs a new arm.
- `client::ElectricalComponent` gained the fields `operational_mode` and `model` (`manufacturer` and `model_name` are deprecated in favour of `model`); struct literals need `..Default::default()`.

## New Features

- Component operational modes are passed to the component graph. Formulas no longer read components whose mode is `Inactive` or `ControlOnly`; for such a component, `LogicalMeterHandle::component()` returns a formula with no reading.
- The crate now requires `frequenz-microgrid-component-graph` 0.6.2 (was 0.6.0), which brings the operational-mode support and formula fixes; see its release notes.
- `test-utils`: `MockComponent::with_operational_mode()` sets a mock component's operational mode.
