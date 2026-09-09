// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! This module defines the `ApparentPower` quantity and its operations.

qty_ctor! {
    #[doc = "A physical quantity representing apparent power."]
    ApparentPower => {
        (from_volt_amperes, as_volt_amperes, "VA", 1e0),
        (from_kilovolt_amperes, as_kilovolt_amperes, "kVA", 1e3),
        (from_megavolt_amperes, as_megavolt_amperes, "MVA", 1e6),
    }
}

#[cfg(test)]
mod tests {
    use super::ApparentPower;
    use crate::quantity::{Quantity as _, test_utils::assert_f32_eq};

    #[test]
    fn test_apparent_power() {
        let s = ApparentPower::from_kilovolt_amperes(1.5);
        assert_f32_eq(s.as_volt_amperes(), 1500.0);
        assert_f32_eq(s.as_megavolt_amperes(), 0.0015);
        assert_eq!(s.to_string(), "1.5 kVA");
        assert_f32_eq((s * 2.0).as_kilovolt_amperes(), 3.0);
        assert_f32_eq(ApparentPower::zero().as_volt_amperes(), 0.0);
    }
}
