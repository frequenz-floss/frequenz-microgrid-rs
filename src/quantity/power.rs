// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the `Power` quantity and its operations.

use super::{Current, Energy, Voltage};

qty_ctor! {
    #[doc = "A physical quantity representing active power."]
    Power => {
        (from_milliwatts, as_milliwatts, "mW", 1e-3),
        (from_watts, as_watts, "W", 1e0),
        (from_kilowatts, as_kilowatts, "kW", 1e3),
        (from_megawatts, as_megawatts, "MW", 1e6),
        (from_gigawatts, as_gigawatts, "GW", 1e9),
    }
}

impl std::ops::Div<Voltage> for Power {
    type Output = Current;

    fn div(self, voltage: Voltage) -> Self::Output {
        Current::from_amperes(self.as_watts() / voltage.as_volts())
    }
}

impl std::ops::Div<Current> for Power {
    type Output = Voltage;

    fn div(self, current: Current) -> Self::Output {
        Voltage::from_volts(self.as_watts() / current.as_amperes())
    }
}

impl std::ops::Mul<std::time::Duration> for Power {
    type Output = Energy;

    fn mul(self, duration: std::time::Duration) -> Self::Output {
        Energy::from_watthours(self.as_watts() * duration.as_secs_f32() / 3600.0)
    }
}
