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

#[cfg(test)]
mod tests {
    use super::Power;
    use super::{Current, Energy, Voltage};
    use crate::quantity::Percentage;
    use crate::quantity::{Quantity as _, test_utils::assert_f32_eq};

    #[test]
    fn test_power() {
        let power_1 = Power::from_watts(1000.0);
        assert_f32_eq(power_1.as_milliwatts(), 1_000_000.0);
        assert_f32_eq(power_1.as_watts(), 1000.0);
        assert_f32_eq(power_1.as_kilowatts(), 1.0);
        assert_f32_eq(power_1.as_megawatts(), 0.001);
        assert_f32_eq(power_1.as_gigawatts(), 0.000_001);

        let power_2 = Power::from_milliwatts(1_200_000.0);
        assert_f32_eq(power_2.as_watts(), 1200.0);
        let power_2 = Power::from_kilowatts(1.2);
        assert_f32_eq(power_2.as_watts(), 1200.0);
        let power_2 = Power::from_megawatts(0.001_2);
        assert_f32_eq(power_2.as_watts(), 1200.0);
        let power_2 = Power::from_gigawatts(0.000_001_2);
        assert_f32_eq(power_2.as_watts(), 1200.0);

        assert!(power_1 < power_2);
        assert!(power_2 > power_1);

        assert_f32_eq((power_1 + power_2).as_watts(), 2200.0);
        assert_f32_eq((power_2 - power_1).as_watts(), 200.0);
        assert_f32_eq((power_2 * 2.0).as_watts(), 2400.0);
        assert_f32_eq((power_2 / 2.0).as_watts(), 600.0);
        assert_f32_eq(
            (power_2 * Percentage::from_percentage(80.0)).as_watts(),
            960.0,
        );
        assert_f32_eq(power_2 / power_1, 1.2);

        assert_f32_eq(Power::zero().as_watts(), 0.0);
    }

    #[test]
    fn test_power_voltage_current() {
        let power = Power::from_kilowatts(2.0);
        let voltage = Voltage::from_volts(400.0);
        let current = Current::from_amperes(5.0);

        let computed_current = power / voltage;
        assert_f32_eq(computed_current.as_amperes(), 5.0);

        let computed_voltage = power / current;
        assert_f32_eq(computed_voltage.as_volts(), 400.0);
    }

    #[test]
    fn test_power_energy_duration() {
        let power = Power::from_kilowatts(0.5);
        let duration = std::time::Duration::from_secs(7200);
        let energy = Energy::from_kilowatthours(1.0);

        let computed_energy = power * duration;
        assert_f32_eq(computed_energy.as_kilowatthours(), 1.0);

        let computed_power = energy / duration;
        assert_f32_eq(computed_power.as_kilowatts(), 0.5);
    }

    #[test]
    fn test_power_formatting() {
        let s = |value| Power::from_watts(value).to_string();
        let p = |value, prec| format!("{:.prec$}", Power::from_watts(value), prec = prec);
        assert_eq!(s(0.0), "0 mW");

        assert_eq!(s(1.558), "1.558 W");
        assert_eq!(p(1.558, 1), "1.6 W");

        assert_eq!(s(1.5508), "1.551 W");
        assert_eq!(p(1.5508, 5), "1.5508 W");

        assert_eq!(s(2030.0), "2.03 kW");

        assert_eq!(s(2_030_022.0), "2.03 MW");
        assert_eq!(s(2_030_022_123.0), "2.03 GW");
        assert_eq!(p(2_030_022_123.0, 6), "2.030022 GW");

        assert_eq!(s(-1.558), "-1.558 W");
        assert_eq!(p(-1.558, 1), "-1.6 W");

        assert_eq!(s(-2030.0), "-2.03 kW");
        assert_eq!(p(-2030.0, 1), "-2 kW");

        assert_eq!(s(-2_030_022.0), "-2.03 MW");
        assert_eq!(s(-2_030_022_123.0), "-2.03 GW");
        assert_eq!(p(-2_030_022_123.0, 6), "-2.030022 GW");
    }
}
