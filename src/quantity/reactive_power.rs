// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the `ReactivePower` quantity and its operations.

use super::{Current, Voltage};

qty_ctor! {
    #[doc = "A physical quantity representing reactive power."]
    ReactivePower => {
        (from_millivolt_amperes_reactive, as_millivolt_amperes_reactive, "mVAR", 1e-3),
        (from_volt_amperes_reactive, as_volt_amperes_reactive, "VAR", 1e0),
        (from_kilovolt_amperes_reactive, as_kilovolt_amperes_reactive, "kVAR", 1e3),
        (from_megavolt_amperes_reactive, as_megavolt_amperes_reactive, "MVAR", 1e6),
        (from_gigavolt_amperes_reactive, as_gigavolt_amperes_reactive, "GVAR", 1e9),
    }
}

impl std::ops::Div<Voltage> for ReactivePower {
    type Output = Current;

    fn div(self, voltage: Voltage) -> Self::Output {
        Current::from_amperes(self.as_volt_amperes_reactive() / voltage.as_volts())
    }
}

impl std::ops::Div<Current> for ReactivePower {
    type Output = Voltage;

    fn div(self, current: Current) -> Self::Output {
        Voltage::from_volts(self.as_volt_amperes_reactive() / current.as_amperes())
    }
}
