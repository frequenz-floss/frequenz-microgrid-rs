// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the `Percentage` quantity and its operations.

qty_ctor! {
    #[doc = "A quantity representing a percentage (0% to 100%)."]
    Percentage => {
        (from_percentage, as_percentage, "%", 1.0),
        (from_fraction, as_fraction, None, 100.0),
    }
}
