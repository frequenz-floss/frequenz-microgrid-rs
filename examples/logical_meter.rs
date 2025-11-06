// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

use chrono::TimeDelta;
use frequenz_microgrid::{
    Error, LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle, metric, quantity::Power,
};
use tracing_subscriber::{
    EnvFilter,
    fmt::{self},
    prelude::*,
};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::registry()
        .with(EnvFilter::new("info,frequenz_microgrid=warn"))
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
    let formula_pv = logical_meter.pv(None, metric::AcPowerActive)?;
    let formula_consumer = logical_meter.consumer(metric::AcPowerActive)?;

    let formula = logical_meter.grid(metric::AcPowerActive)?
        - logical_meter.pv(None, metric::AcPowerActive)?
        + logical_meter.consumer(metric::AcPowerActive)?
        + Power::from_kilowatts(100.0);

    let mut rx = formula.subscribe().await?;
    let mut grid_rx = formula_grid.subscribe().await?;
    let mut pv_rx = formula_pv.subscribe().await?;
    let mut consumer_rx = formula_consumer.subscribe().await?;

    for _ in 0..3 {
        let sample = rx.recv().await.unwrap();
        let grid_sample = grid_rx.recv().await.unwrap();
        let pv_sample = pv_rx.recv().await.unwrap();
        let consumer_sample = consumer_rx.recv().await.unwrap();
        tracing::info!(
            "grid({}) - pv({}) + consumer({}) + 100kW = {}",
            grid_sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string()),
            pv_sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string()),
            consumer_sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string()),
            sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string())
        );
    }

    let formula_grid_voltage = logical_meter.grid(metric::AcVoltage)?.coalesce(
        logical_meter.grid(metric::AcVoltagePhase1N)?.avg(vec![
            logical_meter.grid(metric::AcVoltagePhase2N)?,
            logical_meter.grid(metric::AcVoltagePhase3N)?,
        ])? * 3.0_f32.sqrt(),
    )?;

    tracing::info!("formula_grid_voltage: {}", formula_grid_voltage);
    let mut grid_voltage_rx = formula_grid_voltage.subscribe().await?;

    for _ in 0..3 {
        let sample = grid_voltage_rx.recv().await.unwrap();
        tracing::info!(
            "grid voltage: {}",
            sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string())
        );
    }

    drop(rx);
    drop(grid_rx);
    drop(pv_rx);
    drop(consumer_rx);

    let mut p1 = logical_meter
        .grid(metric::AcVoltagePhase1N)?
        .subscribe()
        .await?;
    let mut p2 = logical_meter
        .grid(metric::AcVoltagePhase2N)?
        .subscribe()
        .await?;
    let mut p3 = logical_meter
        .grid(metric::AcVoltagePhase3N)?
        .subscribe()
        .await?;
    let mut three_phase = logical_meter.grid(metric::AcVoltage)?.subscribe().await?;

    loop {
        let sample = grid_voltage_rx.recv().await.unwrap();
        let p1_sample = p1.recv().await.unwrap();
        let p2_sample = p2.recv().await.unwrap();
        let p3_sample = p3.recv().await.unwrap();
        let three_phase_sample = three_phase.recv().await.unwrap();
        tracing::info!(
            "grid voltage: Coalesce({}, Avg({}, {}, {}) * sqrt(3)) = {}",
            three_phase_sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string()),
            p1_sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string()),
            p2_sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string()),
            p3_sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string()),
            sample
                .value()
                .map(|v| v.to_string())
                .unwrap_or_else(|| "None".to_string())
        );
    }
}
