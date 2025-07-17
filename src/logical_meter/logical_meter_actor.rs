// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module contains the logical meter actor, that takes care of resampling
//! component data, evaluating formulas based on that data, and streaming the
//! data to subscribers.

use chrono::{DateTime, TimeDelta, Timelike as _, Utc};
use frequenz_microgrid_formula_engine::FormulaEngine;
use frequenz_resampling::ResamplingFunction;
use std::collections::{HashMap, HashSet};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{MissedTickBehavior, interval};

use crate::proto::common::v1::metrics::metric_value_variant::MetricValueVariant;
use crate::{
    Error, Metric, MicrogridClientHandle, Sample,
    proto::common::v1::microgrid::components::ComponentData,
};

use super::config::LogicalMeterConfig;

struct LogicalMeterFormula {
    formula: FormulaEngine<f32>,
    sender: broadcast::Sender<Sample>,
}

struct ComponentDataResampler {
    component_id: u64,
    metric: Metric,
    resampler: frequenz_resampling::Resampler<f32, Sample>,
    receiver: broadcast::Receiver<ComponentData>,
}

pub(crate) enum Instruction {
    SubscribeFormula {
        formula: String,
        metric: Metric,
        response_tx: oneshot::Sender<broadcast::Receiver<Sample>>,
    },
}

pub(super) struct LogicalMeterActor {
    instructions_rx: mpsc::Receiver<Instruction>,
    client: MicrogridClientHandle,
    config: LogicalMeterConfig,
    next_ts: DateTime<Utc>,
    timer: tokio::time::Interval,
}

/// Returns the next timestamp aligned to the epoch based on the given interval.
pub(crate) fn epoch_align(timestamp: DateTime<Utc>, interval: TimeDelta) -> Option<DateTime<Utc>> {
    let millis_since_epoch = timestamp.timestamp_millis();
    let interval_millis = interval.num_milliseconds();

    let intervals_since_epoch = millis_since_epoch / interval_millis;
    let aligned_millis_since_epoch = intervals_since_epoch * interval_millis;

    let aligned_timestamp = DateTime::from_timestamp_millis(aligned_millis_since_epoch)?;

    let next_aligned_ts = aligned_timestamp + interval;
    Some(next_aligned_ts)
}

impl LogicalMeterActor {
    pub fn try_new(
        instructions_rx: mpsc::Receiver<Instruction>,
        client: MicrogridClientHandle,
        config: LogicalMeterConfig,
    ) -> Result<Self, Error> {
        let now = Utc::now();
        let next_ts = epoch_align(now, config.resampling_interval).ok_or_else(|| {
            Error::chrono_error("Failed to align current time to the epoch".to_string())
        })?;
        let mut timer =
            interval(config.resampling_interval.to_std().map_err(|e| {
                Error::chrono_error(format!("Failed to convert interval to std: {e}"))
            })?);
        timer.set_missed_tick_behavior(MissedTickBehavior::Burst);

        // The next tick should be at the next aligned timestamp.
        timer.reset_after(
            (next_ts - now)
                .to_std()
                .map_err(|e| Error::chrono_error(format!("Failed to calculate time delta: {e}")))?,
        );

        Ok(Self {
            instructions_rx,
            client,
            config,
            next_ts,
            timer,
        })
    }

    pub async fn run(mut self) {
        let mut resamplers: HashMap<u64, ComponentDataResampler> = HashMap::new();
        let mut formulas: HashMap<String, LogicalMeterFormula> = HashMap::new();

        loop {
            tokio::select! {
                _ = self.timer.tick() => {
                    if let Err(err) = self.do_next(&mut resamplers, &mut formulas) {
                        tracing::error!("Error resampling: {}", err);
                    };
                }
                instruction = self.instructions_rx.recv() => {
                    match instruction {
                        Some(Instruction::SubscribeFormula{formula, metric, response_tx}) => {
                            if let Err(err) = self.handle_subscribe_formula(
                                &formula,
                                metric,
                                response_tx,
                                &mut formulas,
                                &mut resamplers
                            ).await {
                                tracing::error!("Error adding formula: {err}");
                            };
                        }
                        None => {
                            tracing::warn!(
                                concat!(
                                    "LogicalMeterActor's instruction channel closed. ",
                                    "Shutting down actor."
                                )
                            );
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Handles SubscribeFormula instructions.
    ///
    /// If the formula already exists, it sends the existing receiver to the
    /// `response_tx`.
    ///
    /// If the formula does not exist, it creates a new `LogicalMeterFormula` with
    /// the given formula, metric, and a new broadcast channel.
    ///
    /// It also initializes the necessary `ComponentDataResampler` for each component
    /// in the formula, if it does not already exist.
    async fn handle_subscribe_formula(
        &mut self,
        formula: &str,
        metric: Metric,
        receiver_tx: oneshot::Sender<broadcast::Receiver<Sample>>,
        formulas: &mut HashMap<String, LogicalMeterFormula>,
        resamplers: &mut HashMap<u64, ComponentDataResampler>,
    ) -> Result<(), Error> {
        if formulas.contains_key(formula) {
            receiver_tx
                .send(formulas[formula].sender.subscribe())
                .map_err(|_| Error::internal("Failed to send receiver for formula".to_string()))?;
            return Ok(());
        }

        let formula_engine = FormulaEngine::try_new(formula)
            .map_err(|e| Error::formula_engine_error(format!("Failed to parse formula: {e}")))?;
        let (sender, receiver) = broadcast::channel(8);

        for component_id in formula_engine.components() {
            if resamplers.contains_key(component_id) {
                continue;
            }
            let resampler = ComponentDataResampler {
                component_id: *component_id,
                metric,
                resampler: frequenz_resampling::Resampler::new(
                    self.config.resampling_interval,
                    ResamplingFunction::Average,
                    3,
                    Utc::now()
                        .with_nanosecond(0)
                        .ok_or_else(|| Error::chrono_error("Failed to get current time."))?,
                    false,
                ),
                receiver: self.client.get_component_data_stream(*component_id).await?,
            };
            resamplers.insert(*component_id, resampler);
        }

        formulas.insert(
            formula.to_string(),
            LogicalMeterFormula {
                formula: formula_engine,
                sender,
            },
        );
        receiver_tx
            .send(receiver)
            .map_err(|_| Error::internal("Failed to send receiver for formula".to_string()))?;

        Ok(())
    }

    /// Resamples component data and evaluates formulas for the next timestamp.
    fn do_next(
        &mut self,
        resamplers: &mut HashMap<u64, ComponentDataResampler>,
        formulas: &mut HashMap<String, LogicalMeterFormula>,
    ) -> Result<(), Error> {
        let mut comp_data = HashMap::new();
        for (_, resampler) in resamplers.iter_mut() {
            while let Ok(data) = resampler.receiver.try_recv() {
                self.push_to_resampler(resampler, data, resampler.metric);
            }
            let resampled = resampler.resampler.resample(self.next_ts);
            if resampled.len() != 1 {
                return Err(Error::connection_failure(format!(
                    "Resampling produced {} values",
                    resampled.len()
                )));
            }
            comp_data.insert(resampler.component_id, resampled[0].clone().value());
        }

        let mut formulas_to_drop = vec![];
        for (formula_str, formula) in formulas.iter_mut() {
            let result = formula.formula.calculate(&comp_data).map_err(|e| {
                Error::formula_engine_error(format!("Failed to evaluate formula: {e}"))
            })?;

            if let Err(e) = formula.sender.send(Sample::new(self.next_ts, result)) {
                tracing::debug!("No remaining subscribers for formula: {formula_str}. Err: {e}");
                formulas_to_drop.push(formula_str.to_string());
            }
        }

        for formula_str in &formulas_to_drop {
            if let Some(formula) = formulas.remove(formula_str) {
                tracing::debug!("Dropping formula: {}", formula_str);
                drop(formula);
            }
        }
        if !formulas_to_drop.is_empty() {
            let mut components = HashSet::<u64>::new();
            for (_, formula) in formulas.iter() {
                components.extend(formula.formula.components());
            }
            resamplers.retain(|component_id, _| {
                if components.contains(component_id) {
                    true
                } else {
                    tracing::debug!("Dropping resampler for component {}", component_id);
                    false
                }
            });
        }

        self.next_ts += self.config.resampling_interval;
        Ok(())
    }

    /// Extracts the given metric from the given ComponentData and pushes it to
    /// the resampler's internal buffer.
    fn push_to_resampler(
        &mut self,
        resampler: &mut ComponentDataResampler,
        data: ComponentData,
        metric: Metric,
    ) {
        let Some(dd) = data
            .metric_samples
            .iter()
            .find(|s| s.metric == metric as i32)
        else {
            return;
        };
        let timestamp = if let Some(timestamp) = dd.sampled_at {
            if let Some(timestamp) =
                DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
            {
                timestamp
            } else {
                return;
            }
        } else {
            return;
        };

        let value = if let Some(value) = &dd.value {
            if let Some(value) = &value.metric_value_variant {
                Some(match value {
                    MetricValueVariant::SimpleMetric(value) => value.value,
                    MetricValueVariant::AggregatedMetric(value) => value.avg_value,
                })
            } else {
                return;
            }
        } else {
            return;
        };

        let sample = Sample::new(timestamp, value);

        resampler.resampler.push(sample);
    }
}
