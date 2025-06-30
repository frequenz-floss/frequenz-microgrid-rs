// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use chrono::TimeDelta;
use frequenz_microgrid::{
    Error, LogicalMeterConfig, LogicalMeterHandle, Metric, MicrogridClientHandle,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt::fmt()
        .with_file(true)
        .with_line_number(true)
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
    let formula_grid = logical_meter.grid(Metric::AcActivePower)?;
    let formula_battery = logical_meter.battery(None, Metric::AcActivePower)?;
    let formula_consumer = logical_meter.consumer(Metric::AcActivePower)?;

    let formula = (logical_meter.grid(Metric::AcActivePower)?
        - logical_meter.battery(None, Metric::AcActivePower)?
        + logical_meter.consumer(Metric::AcActivePower)?)?;

    let mut rx = formula.subscribe().await?;
    let mut grid_rx = formula_grid.subscribe().await?;
    let mut battery_rx = formula_battery.subscribe().await?;
    let mut consumer_rx = formula_consumer.subscribe().await?;

    loop {
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
}
