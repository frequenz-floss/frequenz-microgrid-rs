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

#[cfg(test)]
mod tests {
    use crate::quantity::{Percentage, Quantity as _, Voltage, test_utils::assert_f32_eq};

    use super::Current;

    #[test]
    fn test_current() {
        let current_1 = Current::from_milliamperes(1000.0);
        let current_2 = Current::from_amperes(1.2);
        assert_f32_eq(current_1.as_amperes(), 1.0);
        assert_f32_eq(current_2.as_milliamperes(), 1200.0);

        assert!(current_1 < current_2);
        assert!(current_2 > current_1);

        assert_f32_eq((current_1 + current_2).as_amperes(), 2.2);
        assert_f32_eq((current_2 - current_1).as_amperes(), 0.2);
        assert_f32_eq((current_2 * 2.0).as_amperes(), 2.4);
        assert_f32_eq(
            (current_2 * Percentage::from_percentage(50.0)).as_amperes(),
            0.6,
        );
        assert_f32_eq((current_2 / 3.0).as_amperes(), 0.4);
        assert_f32_eq(current_2 / current_1, 1.2);

        assert_f32_eq(Current::zero().as_amperes(), 0.0);
    }

    #[test]
    fn test_current_power_voltage() {
        let current = Current::from_amperes(2.0);
        let voltage = Voltage::from_volts(230.0);
        let power = current * voltage;
        assert_f32_eq(power.as_watts(), 460.0);
    }

    #[test]
    fn test_current_formatting() {
        let s = |value| Current::from_amperes(value).to_string();
        let p = |value, prec| format!("{:.prec$}", Current::from_amperes(value), prec = prec);
        assert_eq!(s(0.0), "0 mA");

        assert_eq!(s(1.558), "1.558 A");
        assert_eq!(p(1.558, 1), "1.6 A");

        assert_eq!(s(0.001558), "1.558 mA");
        assert_eq!(p(0.001558, 1), "1.6 mA");

        assert_eq!(s(1.5508), "1.551 A");
        assert_eq!(p(1.5508, 5), "1.5508 A");

        assert_eq!(s(0.0015508), "1.551 mA");
        assert_eq!(p(0.0015508, 5), "1.5508 mA");

        assert_eq!(s(-1.558), "-1.558 A");
        assert_eq!(p(-1.558, 1), "-1.6 A");

        assert_eq!(s(-0.001558), "-1.558 mA");
        assert_eq!(p(-0.001558, 1), "-1.6 mA");

        assert_eq!(s(-2030.04487), "-2030.045 A");
        assert_eq!(p(-2030.04487, 1), "-2030 A");
        assert_eq!(p(-2030.04487, 2), "-2030.04 A");
    }
}
