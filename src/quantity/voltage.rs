// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the `Voltage` quantity and its operations.

use super::{Current, Power};

qty_ctor! {
    #[doc = "A physical quantity representing voltage."]
    Voltage => {
        (from_millivolts, as_millivolts, "mV", 1e-3),
        (from_volts, as_volts, "V", 1e0),
        (from_kilovolts, as_kilovolts, "kV", 1e3),
    }
}

impl std::ops::Mul<Current> for Voltage {
    type Output = Power;

    fn mul(self, current: Current) -> Self::Output {
        Power::from_watts(self.as_volts() * current.as_amperes())
    }
}
