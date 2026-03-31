// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Extensions to the generated protobuf code for electrical components.

use crate::client::{
    ElectricalComponent, ElectricalComponentCategory,
    proto::common::microgrid::electrical_components::{
        InverterType, electrical_component_category_specific_info::Kind,
    },
};

impl ElectricalComponent {
    /// Returns true if the component is an inverter, false otherwise.
    pub fn is_inverter(&self) -> bool {
        matches!(
            ElectricalComponentCategory::try_from(self.category),
            Ok(ElectricalComponentCategory::Inverter)
        )
    }

    /// Returns true if the component is a PV inverter, false otherwise.
    pub fn is_pv_inverter(&self) -> bool {
        if let Some(info) = &self.category_specific_info
            && let Some(Kind::Inverter(inverter_info)) = &info.kind
        {
            return matches!(
                InverterType::try_from(inverter_info.r#type),
                Ok(InverterType::Pv)
            );
        }
        false
    }

    /// Returns true if the component is a battery inverter, false otherwise.
    pub fn is_battery_inverter(&self) -> bool {
        if let Some(info) = &self.category_specific_info
            && let Some(Kind::Inverter(inverter_info)) = &info.kind
        {
            return matches!(
                InverterType::try_from(inverter_info.r#type),
                Ok(InverterType::Battery)
            );
        }
        false
    }

    /// Returns true if the component is a hybrid inverter, false otherwise.
    pub fn is_hybrid_inverter(&self) -> bool {
        if let Some(info) = &self.category_specific_info
            && let Some(Kind::Inverter(inverter_info)) = &info.kind
        {
            return matches!(
                InverterType::try_from(inverter_info.r#type),
                Ok(InverterType::Hybrid)
            );
        }
        false
    }
}
