// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the `Frequency` quantity and its operations.

qty_ctor! {
    #[doc = "A physical quantity representing frequency."]
    Frequency => {
        (from_hertz, as_hertz, "Hz", 1.0),
        (from_kilohertz, as_kilohertz, "kHz", 1e3),
        (from_megahertz, as_megahertz, "MHz", 1e6),
        (from_gigahertz, as_gigahertz, "GHz", 1e9),
    }
}

#[cfg(test)]
mod tests {
    use super::Frequency;
    use crate::quantity::{Percentage, Quantity as _, test_utils::assert_f32_eq};

    #[test]
    fn test_frequency() {
        let freq_1 = Frequency::from_hertz(1000.0);

        assert_f32_eq(freq_1.as_hertz(), 1000.0);
        assert_f32_eq(freq_1.as_kilohertz(), 1.0);
        assert_f32_eq(freq_1.as_megahertz(), 0.001);
        assert_f32_eq(freq_1.as_gigahertz(), 0.000_001);

        let freq_2 = Frequency::from_kilohertz(1.2);
        assert_f32_eq(freq_2.as_hertz(), 1200.0);

        let freq_2 = Frequency::from_megahertz(0.0012);
        assert_f32_eq(freq_2.as_hertz(), 1200.0);

        let freq_2 = Frequency::from_gigahertz(0.000_001_2);
        assert_f32_eq(freq_2.as_hertz(), 1200.0);

        assert!(freq_1 < freq_2);
        assert!(freq_2 > freq_1);

        assert_f32_eq((freq_1 + freq_2).as_hertz(), 2200.0);
        assert_f32_eq((freq_2 - freq_1).as_hertz(), 200.0);
        assert_f32_eq((freq_1 * 2.0).as_hertz(), 2000.0);
        assert_f32_eq((freq_2 / 2.0).as_hertz(), 600.0);
        assert_f32_eq(
            (freq_2 * Percentage::from_percentage(50.0)).as_hertz(),
            600.0,
        );
        assert_f32_eq(freq_2 / freq_1, 1.2);

        assert_f32_eq(Frequency::zero().as_hertz(), 0.0);
    }

    #[test]
    fn test_frequency_formatting() {
        let s = |value| Frequency::from_hertz(value).to_string();
        let p = |value, prec| format!("{:.prec$}", Frequency::from_hertz(value), prec = prec);
        assert_eq!(s(0.0), "0 Hz");

        assert_eq!(s(1.558), "1.558 Hz");
        assert_eq!(p(1.558, 1), "1.6 Hz");

        assert_eq!(s(1.5508), "1.551 Hz");
        assert_eq!(p(1.5508, 5), "1.5508 Hz");

        assert_eq!(s(2030.0), "2.03 kHz");

        assert_eq!(s(2_030_022.0), "2.03 MHz");
        assert_eq!(s(2_030_022_123.0), "2.03 GHz");
        assert_eq!(p(2_030_022_123.0, 6), "2.030022 GHz");

        assert_eq!(s(-1.558), "-1.558 Hz");
        assert_eq!(p(-1.558, 1), "-1.6 Hz");

        assert_eq!(s(-2030.0), "-2.03 kHz");
        assert_eq!(p(-2030.0, 1), "-2 kHz");

        assert_eq!(s(-2_030_022.0), "-2.03 MHz");
        assert_eq!(s(-2_030_022_123.0), "-2.03 GHz");
        assert_eq!(p(-2_030_022_123.0, 6), "-2.030022 GHz");
    }
}
