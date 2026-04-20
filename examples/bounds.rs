// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

use chrono::TimeDelta;
use frequenz_microgrid::Microgrid;
use frequenz_microgrid::{Error, LogicalMeterConfig};
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

    let microgrid = Microgrid::try_new(
        "http://[::1]:8800",
        LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()),
    )
    .await?;

    let mut battery_pool = microgrid.battery_pool(None);
    let mut bounds_rx = battery_pool.power_bounds();

    while let Ok(bounds) = bounds_rx.recv().await {
        tracing::info!("Battery pool active-power bounds: {:?}", bounds);
    }

    Ok(())
}
