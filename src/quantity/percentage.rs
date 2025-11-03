// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the `Percentage` quantity and its operations.

qty_ctor! {
    #[doc = "A quantity representing a percentage (typically 0% to 100%)."]
    Percentage => {
        (from_percentage, as_percentage, "%", 1.0),
        (from_fraction, as_fraction, None, 100.0),
    }
}

#[cfg(test)]
mod tests {
    use super::Percentage;
    use crate::quantity::{Quantity as _, test_utils::assert_f32_eq};

    #[test]
    fn test_percentage() {
        let perc_1 = Percentage::from_percentage(50.0);

        assert_f32_eq(perc_1.as_percentage(), 50.0);
        assert_f32_eq(perc_1.as_fraction(), 0.5);

        let perc_2 = Percentage::from_fraction(0.8);
        assert_f32_eq(perc_2.as_percentage(), 80.0);
        assert_f32_eq(perc_2.as_fraction(), 0.8);

        assert!(perc_1 < perc_2);
        assert!(perc_2 > perc_1);

        assert_f32_eq((perc_1 + perc_2).as_percentage(), 130.0);
        assert_f32_eq((perc_2 - perc_1).as_percentage(), 30.0);
        assert_f32_eq((perc_1 * 2.0).as_percentage(), 100.0);
        assert_f32_eq((perc_2 / 2.0).as_percentage(), 40.0);
        assert_f32_eq((perc_1 * perc_2).as_percentage(), 40.0);
        assert_f32_eq(perc_2 / perc_1, 1.6);

        assert_f32_eq(Percentage::zero().as_percentage(), 0.0);
    }

    #[test]
    fn test_percentage_formatting() {
        let s = |value| Percentage::from_percentage(value).to_string();
        let p = |value, prec| format!("{:.prec$}", Percentage::from_percentage(value), prec = prec);
        assert_eq!(s(0.0), "0 %");
        assert_eq!(s(12.3456), "12.346 %");
        assert_eq!(p(12.3456, 2), "12.35 %");
        assert_eq!(p(12.3456, 4), "12.3456 %");
        assert_eq!(p(12.3456, 5), "12.3456 %");
        assert_eq!(s(100.0), "100 %");
    }
}
