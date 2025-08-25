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
