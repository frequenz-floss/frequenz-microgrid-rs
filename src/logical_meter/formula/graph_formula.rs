// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! A formula generated from the component graph, subscribable through the
//! logical-meter actor.

use std::marker::PhantomData;

use super::{FORMULA_STREAM_CHANNEL_CAPACITY, FormulaSubscriber, Key, QuantitySink};
use crate::{
    Error, Sample, logical_meter::logical_meter_actor, metric::Metric, quantity::Quantity,
};
use async_trait::async_trait;
use frequenz_microgrid_formula_engine as engine;
use tokio::sync::{broadcast, mpsc};

/// A component-graph formula for metric `M`, with every component leaf
/// tagged with the metric.
#[derive(Clone)]
pub struct GraphFormula<M: Metric> {
    engine_formula: engine::Formula<f32, Key>,
    instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    phantom: PhantomData<fn() -> M>,
}

impl<M: Metric> std::fmt::Display for GraphFormula<M> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.engine_formula.fmt(f)
    }
}

#[async_trait]
impl<Q: Quantity + 'static, M: Metric<QuantityType = Q>> FormulaSubscriber for GraphFormula<M> {
    type QuantityType = Q;

    async fn subscribe(&self) -> Result<broadcast::Receiver<Sample<Q>>, Error> {
        let (tx, rx) = broadcast::channel(FORMULA_STREAM_CHANNEL_CAPACITY);
        self.instructions_tx
            .send(logical_meter_actor::Instruction::SubscribeFormula {
                engine_formula: self.engine_formula.clone(),
                sink: Box::new(QuantitySink { tx }),
            })
            .await
            .map_err(|e| Error::connection_failure(format!("Could not send instruction: {e}")))?;
        Ok(rx)
    }
}

impl<M: Metric> GraphFormula<M> {
    /// Creates a formula that subscribes through the given actor channel.
    pub(crate) fn new(
        engine_formula: engine::Formula<f32, Key>,
        instructions_tx: mpsc::Sender<logical_meter_actor::Instruction>,
    ) -> Self {
        Self {
            engine_formula,
            instructions_tx,
            phantom: PhantomData,
        }
    }
}
