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

use crate::ErrorKind;
use crate::proto::common::v1alpha8::metrics::{Metric, metric_value_variant::MetricValueVariant};
use crate::quantity::Quantity;
use crate::{
    Error, MicrogridClientHandle, Sample,
    proto::common::v1alpha8::microgrid::electrical_components::ElectricalComponentTelemetry,
};

use super::config::LogicalMeterConfig;

struct LogicalMeterFormula<Q: Quantity = f32> {
    formula: FormulaEngine<f32>,
    sender: broadcast::Sender<Sample<Q>>,
}

struct ComponentDataResampler {
    component_id: u64,
    metric: Metric,
    resampler: frequenz_resampling::Resampler<f32, Sample>,
    receiver: broadcast::Receiver<ElectricalComponentTelemetry>,
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
    resampler_ts: DateTime<Utc>,
    resampler_timer: tokio::time::Interval,
}

/// Returns the next timestamp aligned to the epoch based on the given interval.
pub(crate) fn epoch_align(timestamp: DateTime<Utc>, interval: TimeDelta) -> Option<DateTime<Utc>> {
    let millis_since_epoch = timestamp.timestamp_millis();
    let interval_millis = interval.num_milliseconds();

    let intervals_since_epoch = millis_since_epoch / interval_millis;
    let aligned_millis_since_epoch = intervals_since_epoch * interval_millis;

    let aligned_timestamp = DateTime::from_timestamp_millis(aligned_millis_since_epoch)?;

    Some(aligned_timestamp)
}

impl LogicalMeterActor {
    pub fn try_new(
        instructions_rx: mpsc::Receiver<Instruction>,
        client: MicrogridClientHandle,
        config: LogicalMeterConfig,
    ) -> Result<Self, Error> {
        let now = Utc::now();
        let last_aligned_ts = epoch_align(now, config.resampling_interval).ok_or_else(|| {
            Error::chrono_error("Failed to align current time to the epoch".to_string())
        })?;
        let mut timer =
            interval(config.resampling_interval.to_std().map_err(|e| {
                Error::chrono_error(format!("Failed to convert interval to std: {e}"))
            })?);
        timer.set_missed_tick_behavior(MissedTickBehavior::Burst);

        // The next tick should be at the next aligned timestamp.
        timer.reset_after(
            (last_aligned_ts + config.resampling_interval - now)
                .to_std()
                .map_err(|e| Error::chrono_error(format!("Failed to calculate time delta: {e}")))?,
        );

        Ok(Self {
            instructions_rx,
            client,
            config,
            resampler_ts: last_aligned_ts,
            resampler_timer: timer,
        })
    }

    pub async fn run(mut self) {
        let mut resamplers: HashMap<(u64, Metric), ComponentDataResampler> = HashMap::new();
        let mut formulas: HashMap<(String, Metric), LogicalMeterFormula> = HashMap::new();

        loop {
            tokio::select! {
                _ = self.resampler_timer.tick() => {
                    self.resampler_ts += self.config.resampling_interval;

                    let mut resampled = match self.resample_metrics(&mut resamplers) {
                        Ok(resampled) => resampled,
                        Err(err) => {
                            tracing::error!("Error resampling metrics: {}", err);
                            continue;
                        }
                    };

                    if let Err(err) = self.evaluate_formulas(&mut resampled, &mut formulas, |x| x) {
                        if err.kind() == ErrorKind::DroppedUnusedFormulas {
                            self.cleanup_resamplers(&formulas, &mut resamplers);
                        } else {
                            tracing::error!("Error evaluating formulas: {}", err);
                        }
                    };
                }
                instruction = self.instructions_rx.recv() => {
                    match instruction {
                        Some(Instruction::SubscribeFormula{formula, metric, response_tx}) => {
                            if let Err(err) = self.handle_subscribe_formula(
                                formula,
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
        formula: String,
        metric: Metric,
        receiver_tx: oneshot::Sender<broadcast::Receiver<Sample>>,
        formulas: &mut HashMap<(String, Metric), LogicalMeterFormula>,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
    ) -> Result<(), Error> {
        let formula_key = (formula, metric);
        if formulas.contains_key(&formula_key) {
            receiver_tx
                .send(formulas[&formula_key].sender.subscribe())
                .map_err(|_| Error::internal("Failed to send receiver for formula".to_string()))?;
            return Ok(());
        }

        let formula_engine = FormulaEngine::try_new(&formula_key.0)
            .map_err(|e| Error::formula_engine_error(format!("Failed to parse formula: {e}")))?;
        let (sender, receiver) = broadcast::channel(8);

        for component_id in formula_engine.components() {
            let resampler_key = (*component_id, metric);
            if resamplers.contains_key(&resampler_key) {
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
                receiver: self
                    .client
                    .receive_electrical_component_telemetry_stream(*component_id)
                    .await?,
            };
            resamplers.insert(resampler_key, resampler);
        }

        formulas.insert(
            formula_key,
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
    fn evaluate_formulas<Q: Quantity>(
        &mut self,
        resampled_metrics: &mut HashMap<Metric, HashMap<u64, Option<f32>>>,
        formulas: &mut HashMap<(String, Metric), LogicalMeterFormula<Q>>,
        transform: impl Fn(f32) -> Q,
    ) -> Result<(), Error> {
        let mut formulas_to_drop = vec![];
        for (formula_key, formula) in formulas.iter_mut() {
            let result = formula
                .formula
                .calculate(resampled_metrics.entry(formula_key.1).or_default())
                .map_err(|e| {
                    Error::formula_engine_error(format!("Failed to evaluate formula: {e}"))
                })?;

            if let Err(e) = formula
                .sender
                .send(Sample::new(self.resampler_ts, result.map(&transform)))
            {
                tracing::debug!(
                    "No remaining subscribers for formula: {}:({}). Err: {e}",
                    formula_key.1.as_str_name(),
                    formula_key.0
                );
                formulas_to_drop.push(formula_key.clone());
            }
        }

        for formula_key in &formulas_to_drop {
            if let Some(formula) = formulas.remove(formula_key) {
                tracing::debug!(
                    "Dropping formula: {}:({})",
                    formula_key.1.as_str_name(),
                    formula_key.0
                );
                drop(formula);
            }
        }
        if !formulas_to_drop.is_empty() {
            return Err(Error::dropped_unused_formulas("Dropped unused formulas"));
        }

        Ok(())
    }

    /// Resamples component telemetry
    fn resample_metrics(
        &mut self,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
    ) -> Result<HashMap<Metric, HashMap<u64, Option<f32>>>, Error> {
        let mut resampled_metrics: HashMap<Metric, HashMap<u64, Option<f32>>> = HashMap::new();

        for (_, resampler) in resamplers.iter_mut() {
            while let Ok(data) = resampler.receiver.try_recv() {
                self.push_to_resampler(resampler, data, resampler.metric);
            }
            let resampled = resampler.resampler.resample(self.resampler_ts);
            if resampled.len() != 1 {
                return Err(Error::connection_failure(format!(
                    "Resampling produced {} values",
                    resampled.len()
                )));
            }
            resampled_metrics
                .entry(resampler.metric)
                .or_default()
                .insert(resampler.component_id, resampled[0].clone().value());
        }

        Ok(resampled_metrics)
    }

    /// Cleans up resamplers that are no longer needed by any formula.
    fn cleanup_resamplers(
        &mut self,
        formulas: &HashMap<(String, Metric), LogicalMeterFormula>,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
    ) {
        let mut components = HashSet::<(u64, Metric)>::new();
        for ((_, metric), formula) in formulas.iter() {
            components.extend(formula.formula.components().iter().map(|&id| (id, *metric)));
        }
        resamplers.retain(|component_id, _| {
            if components.contains(component_id) {
                true
            } else {
                tracing::debug!(
                    "Dropping resampler for component {}:{}",
                    component_id.0,
                    component_id.1.as_str_name()
                );
                false
            }
        });
    }

    /// Extracts the given metric from the given ComponentData and pushes it to
    /// the resampler's internal buffer.
    fn push_to_resampler(
        &mut self,
        resampler: &mut ComponentDataResampler,
        data: ElectricalComponentTelemetry,
        metric: Metric,
    ) {
        let Some(dd) = data
            .metric_samples
            .iter()
            .find(|s| s.metric == metric as i32)
        else {
            tracing::warn!(
                "No data for metric {:?} in component {}",
                metric,
                resampler.component_id
            );
            return;
        };
        let timestamp = if let Some(timestamp) = dd.sample_time {
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
