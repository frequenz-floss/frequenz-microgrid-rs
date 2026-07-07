# frequenz-microgrid-rs

[![CI](https://github.com/frequenz-floss/frequenz-microgrid-rs/actions/workflows/ci.yaml/badge.svg)](https://github.com/frequenz-floss/frequenz-microgrid-rs/actions/workflows/ci.yaml) [![docs.rs](https://img.shields.io/docsrs/frequenz-microgrid)](https://docs.rs/frequenz-microgrid) [![Crates.io](https://img.shields.io/crates/v/frequenz-microgrid)](https://crates.io/crates/frequenz-microgrid)

High-level Rust interface for the Frequenz Microgrid API.

The crate connects to a Microgrid API server, builds a component graph from the live topology, and exposes typed, formula-driven streams of microgrid metrics — grid power, battery state-of-charge, PV reactive power, consumer current, and so on — without requiring callers to write the per-component formulas by hand.

Support for controlling components is coming soon.

## Quick start

```sh
cargo add frequenz-microgrid chrono tokio --features tokio/macros,tokio/rt-multi-thread
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

`Microgrid::try_new` blocks (with retries) until the server is reachable and returns a graph that builds successfully, so applications can start before their backing service is ready.

## Testing with the in-crate mock

The `test-utils` feature ships a `MockMicrogridApiClient` (plus `MockComponent` and `TokioSyncedClock` helpers) for downstream tests. Enable it as a dev-dependency:

```sh
cargo add --dev frequenz-microgrid --features test-utils
```

## What's included

- `Microgrid` / `LogicalMeterHandle`: typed formulas for grid, battery, pv, chp, ev_charger, consumer, producer, and individual components, parametrised over a metric.
- `BatteryPool` and `PvPool`: aggregated active-power bounds and health-partitioned telemetry for a set of batteries or PV inverters.
- `MicrogridClientHandle`: cloneable low-level gRPC handle with per-stream automatic reconnect.
- Typed quantities — `Power`, `Current`, `Voltage`, `ReactivePower`, `Energy`, `Frequency`, `Percentage` — with unit conversions explicit at every API surface.

See the [API documentation](https://docs.rs/frequenz-microgrid) for the full surface.

## Configuring the underlying graph

`LogicalMeterConfig::with_component_graph_config` forwards a `ComponentGraphConfig` to the [frequenz-microgrid-component-graph](https://docs.rs/frequenz-microgrid-component-graph) builder, exposing knobs like `prefer_meters_in_component_formulas`, `include_phantom_loads_in_consumer_formula`, and per-formula overrides. If not set, the graph crate's `Default::default()` is used.

## Contributing

See the [Contributing Guide](https://github.com/frequenz-floss/frequenz-microgrid-rs/blob/HEAD/CONTRIBUTING.md).

## License

Licensed under the [MIT License](LICENSE).
