# frequenz-microgrid-rs

[<img alt="docs.rs" src="https://img.shields.io/docsrs/frequenz-microgrid">](https://docs.rs/frequenz-microgrid)
[<img alt="Crates.io" src="https://img.shields.io/crates/v/frequenz-microgrid">](https://crates.io/crates/frequenz-microgrid)

High-level Rust interface for the Frequenz Microgrid API.

The crate connects to a Microgrid API server, builds a [`ComponentGraph`]
from the live topology, and exposes typed, formula-driven streams of
microgrid metrics — grid power, battery state-of-charge, PV reactive
power, consumer current, and so on — without requiring callers to write
the per-component formulas by hand.

Support for controlling components is coming soon.

## Quick start

```toml
[dependencies]
frequenz-microgrid = "0.4"
chrono = "0.4"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Stream the grid's active power once per second:

```rust , ignore
use chrono::TimeDelta;
use frequenz_microgrid::{Error, LogicalMeterConfig, Microgrid, metric};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let microgrid = Microgrid::try_new(
        "http://[::1]:8800",
        LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()),
    )
    .await?;

    let mut grid = microgrid
        .logical_meter()
        .grid::<metric::AcPowerActive>()?
        .subscribe()
        .await?;

    while let Ok(sample) = grid.recv().await {
        println!("{:?}: {:?}", sample.timestamp(), sample.value());
    }
    Ok(())
}
```

[`Microgrid::try_new`] blocks (with retries) until the server is
reachable and returns a graph that builds successfully, so applications
can start before their backing service is ready.

## Testing with the in-crate mock

[`frequenz_microgrid::test_utils`][`client::test_utils`] ships a
[`MockMicrogridApiClient`] (plus [`MockComponent`] and
[`TokioSyncedClock`] helpers) for downstream tests.  Enable it as a
dev-dependency feature:

```toml
[dev-dependencies]
frequenz-microgrid = { version = "0.4", features = ["test-utils"] }
```

## What's included

- [`Microgrid`] / [`LogicalMeterHandle`]: typed formulas for [`grid`],
  [`battery`], [`pv`], [`chp`], [`ev_charger`], [`consumer`],
  [`producer`], and individual [`component`]s, parametrised over a
  metric in [`metric`].
- [`BatteryPool`]: aggregated active-power bounds and state-of-charge
  for one or more batteries.
- [`MicrogridClientHandle`]: cloneable low-level gRPC handle with
  per-stream automatic reconnect.
- [`quantity`]: [`Power`], [`Current`], [`Voltage`], [`ReactivePower`],
  [`Energy`], [`Frequency`], [`Percentage`]. Unit conversions are
  explicit at every API surface.

## Configuring the underlying graph

[`LogicalMeterConfig::with_component_graph_config`] forwards a
[`ComponentGraphConfig`] to the
[`frequenz-microgrid-component-graph`](https://docs.rs/frequenz-microgrid-component-graph)
builder, exposing knobs like
[`prefer_meters_in_component_formulas`],
[`include_phantom_loads_in_consumer_formula`], and per-formula
overrides. If not set, the graph crate's `Default::default()` is used.

## Contributing

See the [Contributing Guide](https://github.com/frequenz-floss/frequenz-microgrid-rs/blob/HEAD/CONTRIBUTING.md).

[`ComponentGraph`]: https://docs.rs/frequenz-microgrid-component-graph/latest/frequenz_microgrid_component_graph/struct.ComponentGraph.html
[`ComponentGraphConfig`]: https://docs.rs/frequenz-microgrid-component-graph/latest/frequenz_microgrid_component_graph/struct.ComponentGraphConfig.html
[`prefer_meters_in_component_formulas`]: https://docs.rs/frequenz-microgrid-component-graph/latest/frequenz_microgrid_component_graph/struct.ComponentGraphConfigBuilder.html#method.prefer_meters_in_component_formulas
[`include_phantom_loads_in_consumer_formula`]: https://docs.rs/frequenz-microgrid-component-graph/latest/frequenz_microgrid_component_graph/struct.ComponentGraphConfigBuilder.html#method.include_phantom_loads_in_consumer_formula
[`Microgrid`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.Microgrid.html
[`Microgrid::try_new`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.Microgrid.html#method.try_new
[`LogicalMeterHandle`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterHandle.html
[`LogicalMeterConfig`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterConfig.html
[`LogicalMeterConfig::with_component_graph_config`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterConfig.html#method.with_component_graph_config
[`BatteryPool`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.BatteryPool.html
[`MicrogridClientHandle`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.MicrogridClientHandle.html
[`grid`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterHandle.html#method.grid
[`battery`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterHandle.html#method.battery
[`pv`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterHandle.html#method.pv
[`chp`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterHandle.html#method.chp
[`ev_charger`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterHandle.html#method.ev_charger
[`consumer`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterHandle.html#method.consumer
[`producer`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterHandle.html#method.producer
[`component`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/struct.LogicalMeterHandle.html#method.component
[`metric`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/metric/index.html
[`quantity`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/quantity/index.html
[`Power`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/quantity/struct.Power.html
[`Current`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/quantity/struct.Current.html
[`Voltage`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/quantity/struct.Voltage.html
[`ReactivePower`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/quantity/struct.ReactivePower.html
[`Energy`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/quantity/struct.Energy.html
[`Frequency`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/quantity/struct.Frequency.html
[`Percentage`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/quantity/struct.Percentage.html
[`client::test_utils`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/client/test_utils/index.html
[`MockMicrogridApiClient`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/client/test_utils/struct.MockMicrogridApiClient.html
[`MockComponent`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/client/test_utils/struct.MockComponent.html
[`TokioSyncedClock`]: https://docs.rs/frequenz-microgrid/latest/frequenz_microgrid/client/test_utils/struct.TokioSyncedClock.html
