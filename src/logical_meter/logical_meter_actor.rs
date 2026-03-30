// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module contains the logical meter actor, that takes care of resampling
//! component data, evaluating formulas based on that data, and streaming the
//! data to subscribers.

use chrono::{DateTime, TimeDelta, Utc};
use frequenz_microgrid_formula_engine::FormulaEngine;
use frequenz_resampling::ResamplingFunction;
use std::collections::{HashMap, HashSet};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::{MissedTickBehavior, interval};

use crate::ErrorKind;
use crate::proto::common::metrics::{Metric, metric_value_variant::MetricValueVariant};
use crate::quantity::{Current, Power, Quantity, ReactivePower, Voltage};
use crate::{
    Error, MicrogridClientHandle, Sample,
    proto::common::microgrid::electrical_components::ElectricalComponentTelemetry,
};

use super::config::LogicalMeterConfig;

struct LogicalMeterFormula<Q: Quantity = f32> {
    formula: FormulaEngine<f32>,
    sender: broadcast::Sender<Sample<Q>>,
}

struct ComponentDataResampler {
    component_id: u64,
    metric: Metric,
    resampler: frequenz_resampling::Resampler<f32, Sample<f32>>,
    receiver: broadcast::Receiver<ElectricalComponentTelemetry>,
}

/// Used to send strongly-typed formula streams from the LogicalMeterActor back
/// to the Handle.
pub(crate) enum TypedFormulaResponseSender {
    Power(oneshot::Sender<broadcast::Receiver<Sample<Power>>>),
    Voltage(oneshot::Sender<broadcast::Receiver<Sample<Voltage>>>),
    ReactivePower(oneshot::Sender<broadcast::Receiver<Sample<ReactivePower>>>),
    Current(oneshot::Sender<broadcast::Receiver<Sample<Current>>>),
}

impl<Q: Quantity + 'static> TryFrom<oneshot::Sender<broadcast::Receiver<Sample<Q>>>>
    for TypedFormulaResponseSender
{
    type Error = Error;

    fn try_from(
        sender: oneshot::Sender<broadcast::Receiver<Sample<Q>>>,
    ) -> Result<Self, Self::Error> {
        let sender: Box<dyn std::any::Any + Send> = Box::new(sender);

        let sender = match sender.downcast::<oneshot::Sender<broadcast::Receiver<Sample<Power>>>>()
        {
            Ok(sender) => return Ok(TypedFormulaResponseSender::Power(*sender)),
            Err(sender) => sender,
        };

        let sender =
            match sender.downcast::<oneshot::Sender<broadcast::Receiver<Sample<Voltage>>>>() {
                Ok(sender) => return Ok(TypedFormulaResponseSender::Voltage(*sender)),
                Err(sender) => sender,
            };

        let sender = match sender
            .downcast::<oneshot::Sender<broadcast::Receiver<Sample<ReactivePower>>>>()
        {
            Ok(sender) => return Ok(TypedFormulaResponseSender::ReactivePower(*sender)),
            Err(sender) => sender,
        };

        match sender.downcast::<oneshot::Sender<broadcast::Receiver<Sample<Current>>>>() {
            Ok(sender) => Ok(TypedFormulaResponseSender::Current(*sender)),
            _ => Err(Error::internal(format!(
                "Can't create TypedFormulaResponseSender for `{}`",
                std::any::type_name::<Q>()
            ))),
        }
    }
}

pub(crate) enum Instruction {
    SubscribeFormula {
        formula: String,
        metric: Metric,
        response_tx: TypedFormulaResponseSender,
    },
}

pub(super) struct LogicalMeterActor {
    instructions_rx: mpsc::Receiver<Instruction>,
    client: MicrogridClientHandle,
    config: LogicalMeterConfig,
    resampler_ts: DateTime<Utc>,
    resampler_timer: tokio::time::Interval,
}

/// Holds all active formulas, grouped by quantity type.
#[derive(Default)]
struct Formulas {
    power: HashMap<(String, Metric), LogicalMeterFormula<Power>>,
    voltage: HashMap<(String, Metric), LogicalMeterFormula<Voltage>>,
    reactive_power: HashMap<(String, Metric), LogicalMeterFormula<ReactivePower>>,
    current: HashMap<(String, Metric), LogicalMeterFormula<Current>>,
}

impl Formulas {
    /// Checks if a formula with the given key exists.
    fn contains_key(&self, key: &(String, Metric)) -> bool {
        self.power.contains_key(key)
            || self.voltage.contains_key(key)
            || self.reactive_power.contains_key(key)
            || self.current.contains_key(key)
    }

    /// Sends an existing subscription receiver for the formula with the given key.
    fn send_subscription(
        &self,
        key: &(String, Metric),
        receiver_tx: TypedFormulaResponseSender,
    ) -> Result<(), Error> {
        match receiver_tx {
            TypedFormulaResponseSender::Power(sender) => {
                if self.power.contains_key(key) {
                    sender
                        .send(self.power[key].sender.subscribe())
                        .map_err(|_| {
                            Error::internal("Failed to send receiver for formula".to_string())
                        })?;
                    return Ok(());
                }
            }
            TypedFormulaResponseSender::Voltage(sender) => {
                if self.voltage.contains_key(key) {
                    sender
                        .send(self.voltage[key].sender.subscribe())
                        .map_err(|_| {
                            Error::internal("Failed to send receiver for formula".to_string())
                        })?;
                    return Ok(());
                }
            }
            TypedFormulaResponseSender::ReactivePower(sender) => {
                if self.reactive_power.contains_key(key) {
                    sender
                        .send(self.reactive_power[key].sender.subscribe())
                        .map_err(|_| {
                            Error::internal("Failed to send receiver for formula".to_string())
                        })?;
                    return Ok(());
                }
            }
            TypedFormulaResponseSender::Current(sender) => {
                if self.current.contains_key(key) {
                    sender
                        .send(self.current[key].sender.subscribe())
                        .map_err(|_| {
                            Error::internal("Failed to send receiver for formula".to_string())
                        })?;
                    return Ok(());
                }
            }
        }
        Err(Error::internal(format!(
            "Formula exists, but can't find it: {}:({})",
            key.1.as_str_name(),
            key.0
        )))
    }

    /// Starts a new formula with the given formula string, metric, and sends a receiver
    /// back to the handle.
    fn start_formulas(
        &mut self,
        formula: String,
        metric: Metric,
        response_tx: TypedFormulaResponseSender,
    ) -> Result<HashSet<u64>, Error> {
        let formula_key = (formula, metric);

        let formula_engine = FormulaEngine::try_new(&formula_key.0)
            .map_err(|e| Error::formula_engine_error(format!("Failed to parse formula: {e}")))?;
        let components = formula_engine.components().clone();

        match response_tx {
            TypedFormulaResponseSender::Power(receiver_tx) => {
                let (sender, receiver) = broadcast::channel(100);
                self.power.insert(
                    formula_key,
                    LogicalMeterFormula {
                        formula: formula_engine,
                        sender,
                    },
                );
                receiver_tx.send(receiver).map_err(|_| {
                    Error::internal("Failed to send receiver for formula".to_string())
                })?;
            }
            TypedFormulaResponseSender::Voltage(receiver_tx) => {
                let (sender, receiver) = broadcast::channel(100);
                self.voltage.insert(
                    formula_key,
                    LogicalMeterFormula {
                        formula: formula_engine,
                        sender,
                    },
                );
                receiver_tx.send(receiver).map_err(|_| {
                    Error::internal("Failed to send receiver for formula".to_string())
                })?;
            }
            TypedFormulaResponseSender::ReactivePower(receiver_tx) => {
                let (sender, receiver) = broadcast::channel(100);
                self.reactive_power.insert(
                    formula_key,
                    LogicalMeterFormula {
                        formula: formula_engine,
                        sender,
                    },
                );
                receiver_tx.send(receiver).map_err(|_| {
                    Error::internal("Failed to send receiver for formula".to_string())
                })?;
            }
            TypedFormulaResponseSender::Current(receiver_tx) => {
                let (sender, receiver) = broadcast::channel(100);
                self.current.insert(
                    formula_key,
                    LogicalMeterFormula {
                        formula: formula_engine,
                        sender,
                    },
                );
                receiver_tx.send(receiver).map_err(|_| {
                    Error::internal("Failed to send receiver for formula".to_string())
                })?;
            }
        }

        Ok(components)
    }
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
        let mut formulas = Formulas::default();

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
                    if let Some(err) = {
                        self.evaluate_formulas(
                            &mut resampled, &mut formulas.power, Power::from_watts
                        )
                        .err()
                        .or(self.evaluate_formulas(
                            &mut resampled, &mut formulas.voltage, Voltage::from_volts
                        ).err())
                        .or(self.evaluate_formulas(
                            &mut resampled, &mut formulas.current, Current::from_amperes
                        ).err())
                        .or(self.evaluate_formulas(
                            &mut resampled,
                            &mut formulas.reactive_power,
                            ReactivePower::from_volt_amperes_reactive
                        ).err())
                    } {
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

    async fn start_resamplers(
        &mut self,
        components: &HashSet<u64>,
        metric: Metric,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
    ) -> Result<(), Error> {
        for component_id in components {
            let resampler_key = &(*component_id, metric);
            if resamplers.contains_key(resampler_key) {
                continue;
            }
            let resampler = ComponentDataResampler {
                component_id: *component_id,
                metric,
                resampler: frequenz_resampling::Resampler::new(
                    self.config.resampling_interval,
                    self.config
                        // Look for a specific metric override first
                        .resampling_overrides
                        .get(&metric)
                        .cloned()
                        // Then look for a configured default
                        .or_else(|| self.config.resampling_function.clone())
                        // Finally, default to average if no default is
                        // configured
                        .unwrap_or(ResamplingFunction::Average),
                    // The resampler expects max age to be i32, so we need to
                    // cap it if the user provided a higher value.
                    self.config.max_age_in_intervals.min(i32::MAX as u32) as i32,
                    self.resampler_ts,
                    false,
                ),
                receiver: self
                    .client
                    .receive_electrical_component_telemetry_stream(*component_id)
                    .await?,
            };
            resamplers.insert(*resampler_key, resampler);
        }
        Ok(())
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
        receiver_tx: TypedFormulaResponseSender,
        all_formulas: &mut Formulas,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
    ) -> Result<(), Error> {
        let formula_key = (formula.clone(), metric);
        if all_formulas.contains_key(&formula_key) {
            all_formulas.send_subscription(&formula_key, receiver_tx)
        } else {
            let components = all_formulas.start_formulas(formula, metric, receiver_tx)?;
            self.start_resamplers(&components, metric, resamplers).await
        }
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
        formulas: &Formulas,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
    ) {
        let mut components = HashSet::<(u64, Metric)>::new();
        for ((_, metric), formula) in formulas.power.iter() {
            components.extend(formula.formula.components().iter().map(|&id| (id, *metric)));
        }
        for ((_, metric), formula) in formulas.voltage.iter() {
            components.extend(formula.formula.components().iter().map(|&id| (id, *metric)));
        }
        for ((_, metric), formula) in formulas.reactive_power.iter() {
            components.extend(formula.formula.components().iter().map(|&id| (id, *metric)));
        }
        for ((_, metric), formula) in formulas.current.iter() {
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
            tracing::debug!(
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
