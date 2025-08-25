// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the `Current` quantity and its operations.

use super::{Power, Voltage};

qty_ctor! {
    #[doc = "A physical quantity representing electric current."]
    Current => {
        (from_milliamperes, as_milliamperes, "mA", 1e-3),
        (from_amperes, as_amperes, "A", 1e0),
    }
}

impl std::ops::Mul<Voltage> for Current {
    type Output = Power;

    fn mul(self, voltage: Voltage) -> Self::Output {
        Power::from_watts(self.as_amperes() * voltage.as_volts())
    }
}
