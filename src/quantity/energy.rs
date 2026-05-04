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
        Power::from_watts(self.as_watthours() * 3600.0 / duration.as_secs_f32())
    }
}

#[cfg(test)]
mod tests {
    use crate::quantity::{Percentage, Power, Quantity as _, test_utils::assert_f32_eq};

    use super::Energy;

    #[test]
    fn test_energy() {
        let energy_1 = Energy::from_watthours(1000.0);

        assert_f32_eq(energy_1.as_milliwatthours(), 1_000_000.0);
        assert_f32_eq(energy_1.as_watthours(), 1000.0);
        assert_f32_eq(energy_1.as_kilowatthours(), 1.0);
        assert_f32_eq(energy_1.as_megawatthours(), 0.001);
        assert_f32_eq(energy_1.as_gigawatthours(), 0.000_001);

        let energy_2 = Energy::from_milliwatthours(1_200_000.0);
        assert_f32_eq(energy_2.as_watthours(), 1200.0);

        let energy_2 = Energy::from_kilowatthours(1.2);
        assert_f32_eq(energy_2.as_watthours(), 1200.0);

        let energy_2 = Energy::from_megawatthours(0.0012);
        assert_f32_eq(energy_2.as_watthours(), 1200.0);

        let energy_2 = Energy::from_gigawatthours(0.000_001_2);
        assert_f32_eq(energy_2.as_watthours(), 1200.0);

        assert!(energy_1 < energy_2);
        assert!(energy_2 > energy_1);

        assert_f32_eq((energy_1 + energy_2).as_watthours(), 2200.0);
        assert_f32_eq((energy_2 - energy_1).as_watthours(), 200.0);
        assert_f32_eq((energy_2 * 2.0).as_watthours(), 2400.0);
        assert_f32_eq(
            (energy_2 * Percentage::from_percentage(50.0)).as_watthours(),
            600.0,
        );
        assert_f32_eq((energy_2 / 3.0).as_watthours(), 400.0);
        assert_f32_eq(energy_2 / energy_1, 1.2);

        assert_f32_eq(Energy::zero().as_watthours(), 0.0);
    }

    #[test]
    fn test_energy_power_duration() {
        let energy = Energy::from_kilowatthours(1.0);
        let power = Power::from_kilowatts(0.5);

        let duration = energy / power;
        assert_f32_eq(duration.as_secs_f32(), 7200.0);

        let power_calculated = energy / duration;
        assert_f32_eq(power_calculated.as_kilowatts(), 0.5);
    }

    #[test]
    fn test_energy_formatting() {
        let s = |value| Energy::from_watthours(value).to_string();
        let p = |value, prec| format!("{:.prec$}", Energy::from_watthours(value), prec = prec);

        assert_eq!(s(0.0), "0 mWh");
        assert_eq!(s(1.558), "1.558 Wh");
        assert_eq!(p(1.558, 1), "1.6 Wh");

        assert_eq!(s(0.001558), "1.558 mWh");
        assert_eq!(p(0.001558, 1), "1.6 mWh");

        assert_eq!(s(1.5508), "1.551 Wh");
        assert_eq!(p(1.5508, 5), "1.5508 Wh");

        assert_eq!(s(0.0015508), "1.551 mWh");
        assert_eq!(p(0.0015508, 5), "1.5508 mWh");

        assert_eq!(s(1030.0449), "1.03 kWh");
        assert_eq!(p(1030.0449, 1), "1 kWh");

        assert_eq!(s(2_030_022.0), "2.03 MWh");
        assert_eq!(s(2_030_022_123.0), "2.03 GWh");
        assert_eq!(p(2_030_022_123.0, 6), "2.030022 GWh");

        assert_eq!(s(-1.558), "-1.558 Wh");
        assert_eq!(p(-1.558, 1), "-1.6 Wh");

        assert_eq!(s(-1030.0449), "-1.03 kWh");
        assert_eq!(p(-1030.0449, 1), "-1 kWh");

        assert_eq!(s(-2_030_022.0), "-2.03 MWh");
        assert_eq!(p(-2_030_022.0, 1), "-2 MWh");

        assert_eq!(s(-2_030_022_123.0), "-2.03 GWh");
        assert_eq!(p(-2_030_022_123.0, 6), "-2.030022 GWh");
    }
}
