// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! Component graph implementation for the microgrid API.

use tracing::{error, warn};

impl frequenz_microgrid_component_graph::Node
    for super::common::v1alpha8::microgrid::electrical_components::ElectricalComponent
{
    fn component_id(&self) -> u64 {
        self.id
    }

    fn category(&self) -> frequenz_microgrid_component_graph::ComponentCategory {
        use super::common::v1alpha8::microgrid::electrical_components as pb;
        use frequenz_microgrid_component_graph as gr;

        let category =
            pb::ElectricalComponentCategory::try_from(self.category).unwrap_or_else(|e| {
                error!("Error converting component category: {}", e);
                pb::ElectricalComponentCategory::Unspecified
            });

        match category {
            pb::ElectricalComponentCategory::Unspecified => gr::ComponentCategory::Unspecified,
            pb::ElectricalComponentCategory::GridConnectionPoint => {
                gr::ComponentCategory::GridConnectionPoint
            }
            pb::ElectricalComponentCategory::Meter => gr::ComponentCategory::Meter,
            pb::ElectricalComponentCategory::Inverter => {
                gr::ComponentCategory::Inverter(match self.category_specific_info {
                    Some(pb::ElectricalComponentCategorySpecificInfo { kind }) => match kind {
                        Some(pb::electrical_component_category_specific_info::Kind::Inverter(
                            inverter,
                        )) => {
                            match pb::InverterType::try_from(inverter.r#type).unwrap_or_else(|e| {
                                error!("Error converting inverter type: {}", e);
                                pb::InverterType::Unspecified
                            }) {
                                pb::InverterType::Pv => gr::InverterType::Pv,
                                pb::InverterType::Battery => gr::InverterType::Battery,
                                pb::InverterType::Hybrid => gr::InverterType::Hybrid,
                                pb::InverterType::Unspecified => gr::InverterType::Unspecified,
                            }
                        }
                        Some(_) => {
                            warn!("Unknown component specific info for inverter: {:?}", kind);
                            gr::InverterType::Unspecified
                        }
                        None => gr::InverterType::Unspecified,
                    },
                    _ => gr::InverterType::Unspecified,
                })
            }
            pb::ElectricalComponentCategory::Converter => gr::ComponentCategory::Converter,
            pb::ElectricalComponentCategory::Battery => {
                gr::ComponentCategory::Battery(match self.category_specific_info {
                    Some(pb::ElectricalComponentCategorySpecificInfo { kind }) => match kind {
                        Some(pb::electrical_component_category_specific_info::Kind::Battery(
                            battery,
                        )) => {
                            match pb::BatteryType::try_from(battery.r#type).unwrap_or_else(|e| {
                                error!("Error converting battery type: {}", e);
                                pb::BatteryType::Unspecified
                            }) {
                                pb::BatteryType::LiIon => gr::BatteryType::LiIon,
                                pb::BatteryType::NaIon => gr::BatteryType::NaIon,
                                pb::BatteryType::Unspecified => gr::BatteryType::Unspecified,
                            }
                        }
                        Some(_) => {
                            warn!("Unknown component specific info for battery: {:?}", kind);
                            gr::BatteryType::Unspecified
                        }
                        None => gr::BatteryType::Unspecified,
                    },
                    _ => gr::BatteryType::Unspecified,
                })
            }
            pb::ElectricalComponentCategory::EvCharger => {
                gr::ComponentCategory::EvCharger(match self.category_specific_info {
                    Some(pb::ElectricalComponentCategorySpecificInfo { kind }) => match kind {
                        Some(pb::electrical_component_category_specific_info::Kind::EvCharger(
                            ev_charger,
                        )) => match pb::EvChargerType::try_from(ev_charger.r#type).unwrap_or_else(
                            |e| {
                                error!("Error converting ev charger type: {}", e);
                                pb::EvChargerType::Unspecified
                            },
                        ) {
                            pb::EvChargerType::Ac => gr::EvChargerType::Ac,
                            pb::EvChargerType::Dc => gr::EvChargerType::Dc,
                            pb::EvChargerType::Hybrid => gr::EvChargerType::Hybrid,
                            pb::EvChargerType::Unspecified => gr::EvChargerType::Unspecified,
                        },
                        Some(_) => {
                            warn!("Unknown component specific info for ev charger: {:?}", kind);
                            gr::EvChargerType::Unspecified
                        }
                        None => gr::EvChargerType::Unspecified,
                    },
                    _ => gr::EvChargerType::Unspecified,
                })
            }
            pb::ElectricalComponentCategory::CryptoMiner => gr::ComponentCategory::CryptoMiner,
            pb::ElectricalComponentCategory::Electrolyzer => gr::ComponentCategory::Electrolyzer,
            pb::ElectricalComponentCategory::Chp => gr::ComponentCategory::Chp,
            pb::ElectricalComponentCategory::Hvac => gr::ComponentCategory::Hvac,
            pb::ElectricalComponentCategory::Breaker => gr::ComponentCategory::Breaker,
            pb::ElectricalComponentCategory::Precharger => gr::ComponentCategory::Precharger,
            pb::ElectricalComponentCategory::PowerTransformer => {
                gr::ComponentCategory::PowerTransformer
            }
            pb::ElectricalComponentCategory::Plc => gr::ComponentCategory::Plc,
            pb::ElectricalComponentCategory::StaticTransferSwitch => {
                gr::ComponentCategory::StaticTransferSwitch
            }
            pb::ElectricalComponentCategory::UninterruptiblePowerSupply => {
                gr::ComponentCategory::UninterruptiblePowerSupply
            }
            pb::ElectricalComponentCategory::CapacitorBank => gr::ComponentCategory::CapacitorBank,
            pb::ElectricalComponentCategory::WindTurbine => gr::ComponentCategory::WindTurbine,
        }
    }
}

impl frequenz_microgrid_component_graph::Edge
    for super::common::v1alpha8::microgrid::electrical_components::ElectricalComponentConnection
{
    fn source(&self) -> u64 {
        self.source_electrical_component_id
    }

    fn destination(&self) -> u64 {
        self.destination_electrical_component_id
    }
}
