// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the `Energy` quantity and its operations.

use super::Power;

qty_ctor! {
    #[doc = "A physical quantity representing energy."]
    Energy => {
        (from_milliwatthours, as_milliwatthours, "mWh", 1e-3),
        (from_watthours, as_watthours, "Wh", 1e0),
        (from_kilowatthours, as_kilowatthours, "kWh", 1e3),
        (from_megawatthours, as_megawatthours, "MWh", 1e6),
        (from_gigawatthours, as_gigawatthours, "GWh", 1e9),
    }
}

impl std::ops::Div<Power> for Energy {
    type Output = std::time::Duration;

    fn div(self, power: Power) -> Self::Output {
        let seconds = (self.as_watthours() / power.as_watts()) * 3600.0;
        std::time::Duration::from_secs_f32(seconds)
    }
}

impl std::ops::Div<std::time::Duration> for Energy {
    type Output = Power;

    fn div(self, duration: std::time::Duration) -> Self::Output {
        Power::from_watts(self.as_watthours() / duration.as_secs_f32() / 3600.0)
    }
}
