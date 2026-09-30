# Frequenz Microgrid Release Notes

## Summary

This release introduces the SteamBoilerPool, with support for streaming telemetry, bounds and health status.

## New Features

- `LogicalMeterHandle::steam_boiler::<M>()` streams a metric for a set of steam boilers.
- `Microgrid::steam_boiler_pool()` returns a `SteamBoilerPool` with the pool's active power, aggregated active-power bounds and health-partitioned telemetry snapshots.
- `test-utils`: `MockComponent::steam_boiler()` builds a steam boiler, and `MockComponent::add_sample_power_bounds()` attaches bounds to a component's streamed active-power samples.
- `set_power_active` and `set_power_reactive` are added to `MicrogridClientHandle`, exposing `SetElectricalComponentPower` from the Microgrid API. They return once the request is accepted, with a receiver for the `SetPowerUpdate`s the API streams after that.
- `test-utils`: `MockComponent::with_set_power_statuses()` sets the statuses streamed back for set-power requests, and `MockMicrogridApiClient::set_power_calls_handle()` exposes the recorded requests.
