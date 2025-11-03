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

#[cfg(test)]
mod tests {
    use crate::quantity::{
        Percentage, Quantity as _, ReactivePower, Voltage, test_utils::assert_f32_eq,
    };

    #[test]
    fn test_reactive_power() {
        let reactive_power_1 = ReactivePower::from_volt_amperes_reactive(1000.0);
        assert_f32_eq(
            reactive_power_1.as_millivolt_amperes_reactive(),
            1_000_000.0,
        );
        assert_f32_eq(reactive_power_1.as_volt_amperes_reactive(), 1000.0);
        assert_f32_eq(reactive_power_1.as_kilovolt_amperes_reactive(), 1.0);
        assert_f32_eq(reactive_power_1.as_megavolt_amperes_reactive(), 0.001);
        assert_f32_eq(reactive_power_1.as_gigavolt_amperes_reactive(), 0.000_001);

        let reactive_power_2 = ReactivePower::from_millivolt_amperes_reactive(1_200_000.0);
        assert_f32_eq(reactive_power_2.as_volt_amperes_reactive(), 1200.0);
        let reactive_power_2 = ReactivePower::from_kilovolt_amperes_reactive(1.2);
        assert_f32_eq(reactive_power_2.as_volt_amperes_reactive(), 1200.0);
        let reactive_power_2 = ReactivePower::from_megavolt_amperes_reactive(0.0012);
        assert_f32_eq(reactive_power_2.as_volt_amperes_reactive(), 1200.0);
        let reactive_power_2 = ReactivePower::from_gigavolt_amperes_reactive(0.000_001_2);
        assert_f32_eq(reactive_power_2.as_volt_amperes_reactive(), 1200.0);

        assert!(reactive_power_1 < reactive_power_2);
        assert!(reactive_power_2 > reactive_power_1);

        assert_f32_eq(
            (reactive_power_1 + reactive_power_2).as_volt_amperes_reactive(),
            2200.0,
        );
        assert_f32_eq(
            (reactive_power_2 - reactive_power_1).as_volt_amperes_reactive(),
            200.0,
        );
        assert_f32_eq((reactive_power_1 * 2.0).as_volt_amperes_reactive(), 2000.0);
        assert_f32_eq((reactive_power_2 / 2.0).as_volt_amperes_reactive(), 600.0);
        assert_f32_eq(
            (reactive_power_2 * Percentage::from_percentage(50.0)).as_volt_amperes_reactive(),
            600.0,
        );
        assert_f32_eq(reactive_power_2 / reactive_power_1, 1.2);
        assert_f32_eq(ReactivePower::zero().as_volt_amperes_reactive(), 0.0);
    }

    #[test]
    fn test_reactive_power_voltage_current() {
        let reactive_power = ReactivePower::from_kilovolt_amperes_reactive(1.0);
        let voltage = Voltage::from_volts(1000.0);

        let current = reactive_power / voltage;
        assert_f32_eq(current.as_amperes(), 1.0);

        let voltage_calculated = reactive_power / current;
        assert_f32_eq(voltage_calculated.as_volts(), 1000.0);
    }

    #[test]
    fn test_reactive_power_formatting() {
        let s = |value| ReactivePower::from_volt_amperes_reactive(value).to_string();
        let p = |value, prec| {
            format!(
                "{:.prec$}",
                ReactivePower::from_volt_amperes_reactive(value),
                prec = prec
            )
        };
        assert_eq!(s(0.0), "0 mVAR");

        assert_eq!(s(1.558), "1.558 VAR");
        assert_eq!(p(1.558, 1), "1.6 VAR");

        assert_eq!(s(1.5508), "1.551 VAR");
        assert_eq!(p(1.5508, 5), "1.5508 VAR");

        assert_eq!(s(2030.0), "2.03 kVAR");

        assert_eq!(s(2_030_022.0), "2.03 MVAR");
        assert_eq!(s(2_030_022_123.0), "2.03 GVAR");
        assert_eq!(p(2_030_022_123.0, 6), "2.030022 GVAR");

        assert_eq!(s(-1.558), "-1.558 VAR");
        assert_eq!(p(-1.558, 1), "-1.6 VAR");

        assert_eq!(s(-2030.0), "-2.03 kVAR");
        assert_eq!(p(-2030.0, 1), "-2 kVAR");

        assert_eq!(s(-2_030_022.0), "-2.03 MVAR");
        assert_eq!(p(-2_030_022.0, 6), "-2.030022 MVAR");

        assert_eq!(s(-2_030_022_123.0), "-2.03 GVAR");
        assert_eq!(p(-2_030_022_123.0, 6), "-2.030022 GVAR");
    }
}
