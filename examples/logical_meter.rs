// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use chrono::TimeDelta;
use frequenz_microgrid::{
    Error, FormulaOps, LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle, metric,
};
use tracing_subscriber::{
    EnvFilter,
    fmt::{self},
    prelude::*,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info,frequenz_microgrid=debug"))
        .with(fmt::layer().with_file(true).with_line_number(true))
        .init();

    let client = MicrogridClientHandle::new("http://[::1]:8800");
    let mut logical_meter = LogicalMeterHandle::try_new(
        client,
        LogicalMeterConfig {
            resampling_interval: TimeDelta::try_seconds(1).unwrap(),
        },
    )
    .await?;

    // Create a formula that calculates `grid_power - battery_power`.
    let formula_grid = logical_meter.grid(metric::AcPowerActive)?;
    let formula_battery = logical_meter.battery(None, metric::AcPowerActive)?;
    let formula_consumer = logical_meter.consumer(metric::AcPowerActive)?;

    let formula = (logical_meter.grid(metric::AcPowerActive)?
        - logical_meter.battery(None, metric::AcPowerActive)?
        + logical_meter.consumer(metric::AcPowerActive)?)?;

    let mut rx = formula.subscribe().await?;
    let mut grid_rx = formula_grid.subscribe().await?;
    let mut battery_rx = formula_battery.subscribe().await?;
    let mut consumer_rx = formula_consumer.subscribe().await?;

    for _ in 0..3 {
        let sample = rx.recv().await.unwrap();
        let grid_sample = grid_rx.recv().await.unwrap();
        let battery_sample = battery_rx.recv().await.unwrap();
        let consumer_sample = consumer_rx.recv().await.unwrap();
        tracing::info!(
            "grid({}) - battery({}) + consumer({}) = {}",
            grid_sample.value().unwrap(),
            battery_sample.value().unwrap(),
            consumer_sample.value().unwrap(),
            sample.value().unwrap()
        );
    }
    let formula_grid_voltage = logical_meter
        .battery(None, metric::AcVoltagePhase1N)?
        .coalesce(logical_meter.pv(None, metric::AcVoltagePhase1N)?)?;

    tracing::info!("formula_grid_voltage: {}", formula_grid_voltage);
    let mut grid_voltage_rx = formula_grid_voltage.subscribe().await?;
    for _ in 0..3 {
        let sample = grid_voltage_rx.recv().await.unwrap();
        tracing::info!("grid voltage: {}", sample.value().unwrap());
    }

    drop(rx);
    drop(grid_rx);
    drop(battery_rx);
    drop(consumer_rx);

    loop {
        let sample = grid_voltage_rx.recv().await.unwrap();
        tracing::info!("grid voltage: {}", sample.value().unwrap());
    }
}
