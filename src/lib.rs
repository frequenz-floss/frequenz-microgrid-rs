// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

#![doc = include_str!("../README.md")]
#![cfg_attr(
    not(test),
    deny(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::unimplemented,
        clippy::todo,
        clippy::unreachable,
    )
)]

mod bounds;
pub use bounds::Bounds;

pub mod client;
pub use client::MicrogridClientHandle;

mod error;
pub use error::{Error, ErrorKind};

pub mod quantity;

mod sample;
pub use sample::Sample;

mod logical_meter;
pub use logical_meter::{Formula, FormulaSubscriber, LogicalMeterConfig, LogicalMeterHandle};

pub mod metric;

pub(crate) mod wall_clock_timer;

mod microgrid;
pub use microgrid::{BatteryPool, Microgrid, PvPool};

#[cfg(any(test, feature = "test-utils"))]
pub use client::test_utils;
