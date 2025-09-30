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

#[cfg(test)]
mod tests {
    use crate::quantity::{
        Current, Percentage, Power, Quantity as _, Voltage, test_utils::assert_f32_eq,
    };

    #[test]
    fn test_voltage() {
        let voltage_1 = Voltage::from_volts(1000.0);

        assert_f32_eq(voltage_1.as_millivolts(), 1_000_000.0);
        assert_f32_eq(voltage_1.as_volts(), 1000.0);
        assert_f32_eq(voltage_1.as_kilovolts(), 1.0);

        let voltage_2 = Voltage::from_millivolts(1_200_000.0);
        assert_f32_eq(voltage_2.as_volts(), 1200.0);

        let voltage_2 = Voltage::from_kilovolts(1.2);
        assert_f32_eq(voltage_2.as_volts(), 1200.0);

        assert!(voltage_1 < voltage_2);
        assert!(voltage_2 > voltage_1);

        assert_f32_eq((voltage_1 + voltage_2).as_volts(), 2200.0);
        assert_f32_eq((voltage_2 - voltage_1).as_volts(), 200.0);
        assert_f32_eq((voltage_1 * 2.0).as_volts(), 2000.0);
        assert_f32_eq((voltage_2 / 2.0).as_volts(), 600.0);
        assert_f32_eq(
            (voltage_2 * Percentage::from_percentage(50.0)).as_volts(),
            600.0,
        );
        assert_f32_eq(voltage_2 / voltage_1, 1.2);

        assert_f32_eq(Voltage::zero().as_volts(), 0.0);
    }

    #[test]
    fn test_voltage_current_power() {
        let voltage = Voltage::from_volts(230.0);
        let current = Current::from_amperes(10.0);
        let power = Power::from_watts(2300.0);

        assert_f32_eq((voltage * current).as_watts(), 2300.0);
        assert_f32_eq((power / voltage).as_amperes(), 10.0);
        assert_f32_eq((power / current).as_volts(), 230.0);
    }

    #[test]
    fn test_voltage_formatting() {
        let s = |value| Voltage::from_volts(value).to_string();
        let p = |value, prec| format!("{:.prec$}", Voltage::from_volts(value), prec = prec);
        assert_eq!(s(0.0), "0 mV");

        assert_eq!(s(0.000_5), "0.5 mV");
        assert_eq!(p(0.000_5, 1), "0.5 mV");
        assert_eq!(s(0.5), "500 mV");

        assert_eq!(s(1.558), "1.558 V");
        assert_eq!(p(1.558, 1), "1.6 V");
        assert_eq!(s(1_558.0), "1.558 kV");
        assert_eq!(p(1_558.0, 1), "1.6 kV");

        assert_eq!(s(1.5508), "1.551 V");
        assert_eq!(p(1.5508, 5), "1.5508 V");

        assert_eq!(s(2_030_000.0), "2030 kV");

        assert_eq!(s(-0.000_5), "-0.5 mV");
        assert_eq!(p(-0.000_5, 1), "-0.5 mV");
        assert_eq!(s(-0.5), "-500 mV");

        assert_eq!(s(-1.558), "-1.558 V");
        assert_eq!(p(-1.558, 1), "-1.6 V");
        assert_eq!(s(-1_558.0), "-1.558 kV");
        assert_eq!(p(-1_558.0, 1), "-1.6 kV");
        assert_eq!(s(-2_030_000.0), "-2030 kV");
    }
}
