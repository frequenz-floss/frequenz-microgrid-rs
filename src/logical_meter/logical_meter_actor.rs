// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module contains the logical meter actor, that takes care of resampling
//! component data, evaluating formulas based on that data, and streaming the
//! data to subscribers.

use chrono::{DateTime, Utc};
use frequenz_microgrid_formula_engine::FormulaEngine;
use frequenz_resampling::ResamplingFunction;
use std::collections::{HashMap, HashSet};
use tokio::sync::{broadcast, mpsc, oneshot};

use crate::ErrorKind;
use crate::client::proto::common::metrics::{Metric, metric_value_variant::MetricValueVariant};
use crate::quantity::{Current, Power, Quantity, ReactivePower, Voltage};
use crate::wall_clock_timer::{Clock, WallClockTimer};
use crate::{
    Error, MicrogridClientHandle, Sample,
    client::proto::common::microgrid::electrical_components::ElectricalComponentTelemetry,
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

/// Polls the broadcast receiver once, logging `Lagged` as a warning
/// (it represents real data loss) and retrying. Returns `Some(data)`
/// with the next sample, or `None` on `Empty` / `Closed`. `Lagged` can
/// happen during a wall-clock jump if the server bursts enough samples
/// to fill the channel buffer, or under sustained back-pressure.
fn poll_telemetry(
    receiver: &mut broadcast::Receiver<ElectricalComponentTelemetry>,
    component_id: u64,
) -> Option<ElectricalComponentTelemetry> {
    loop {
        match receiver.try_recv() {
            Ok(data) => return Some(data),
            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => return None,
            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                tracing::warn!(
                    "resampler receiver lagged {n} samples for cid={component_id}; samples discarded"
                );
            }
            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => return None,
        }
    }
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

pub(super) struct LogicalMeterActor<C: Clock> {
    instructions_rx: mpsc::Receiver<Instruction>,
    client: MicrogridClientHandle,
    config: LogicalMeterConfig,
    resampler_ts: DateTime<Utc>,
    resampler_timer: WallClockTimer<C>,
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

impl<C: Clock> LogicalMeterActor<C> {
    pub(crate) fn try_new(
        instructions_rx: mpsc::Receiver<Instruction>,
        client: MicrogridClientHandle,
        config: LogicalMeterConfig,
        clock: C,
    ) -> Result<Self, Error> {
        if config.resampling_interval <= chrono::TimeDelta::zero() {
            return Err(Error::invalid_config(format!(
                "resampling_interval must be positive, got {:?}",
                config.resampling_interval
            )));
        }
        if config.max_age_in_intervals > i32::MAX as u32 {
            return Err(Error::invalid_config(format!(
                "max_age_in_intervals must fit in i32, got {}",
                config.max_age_in_intervals
            )));
        }
        let timer = WallClockTimer::try_new(config.resampling_interval, clock)?;
        // Resamplers created before the first tick use `resampler_ts` as
        // their start; setting it one interval before the first scheduled
        // tick lines up with the original semantics (first tick produces the
        // first resampled sample).
        let resampler_ts = timer.next_tick_time() - config.resampling_interval;

        Ok(Self {
            instructions_rx,
            client,
            config,
            resampler_ts,
            resampler_timer: timer,
        })
    }

    pub async fn run(mut self) {
        let mut resamplers: HashMap<(u64, Metric), ComponentDataResampler> = HashMap::new();
        let mut formulas = Formulas::default();

        loop {
            tokio::select! {
                tick_info = self.resampler_timer.tick() => {
                    if tick_info.resynced {
                        // Wall clock jumped; the inner resamplers' `start`
                        // fields reference the old clock frame and can't be
                        // advanced through the gap (the API is
                        // single-output-per-tick). Drop any buffered
                        // telemetry from the gap and rebuild them aligned
                        // to one interval before the realigned current
                        // tick, so the resample below emits a single
                        // (empty-buffer → `None`) sample at the realigned
                        // tick — preserving the every-interval cadence
                        // across the jump.
                        let realigned_current =
                            self.resampler_timer.next_tick_time()
                                - self.config.resampling_interval;
                        self.rebuild_resamplers_after_jump(
                            &mut resamplers,
                            realigned_current - self.config.resampling_interval,
                        );
                        self.resampler_ts = realigned_current;
                    } else {
                        self.resampler_ts = tick_info.expected_tick_time;
                    }

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

    /// Builds an inner resampler for `metric` aligned to `start`. Used
    /// by both the startup path and the post-jump rebuild path so the
    /// two stay consistent as `LogicalMeterConfig` evolves.
    fn build_resampler(
        &self,
        metric: Metric,
        start: DateTime<Utc>,
    ) -> frequenz_resampling::Resampler<f32, Sample<f32>> {
        let function = self
            .config
            // Look for a specific metric override first
            .resampling_overrides
            .get(&metric)
            .cloned()
            // Then look for a configured default
            .or_else(|| self.config.resampling_function.clone())
            // Finally, default to average if no default is configured
            .unwrap_or(ResamplingFunction::Average);
        frequenz_resampling::Resampler::new(
            self.config.resampling_interval,
            function,
            // Validated at construction to fit in `i32`.
            self.config.max_age_in_intervals as i32,
            start,
            false,
        )
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
                resampler: self.build_resampler(metric, self.resampler_ts),
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
            while let Some(data) = poll_telemetry(&mut resampler.receiver, resampler.component_id) {
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

    /// Rebuilds every inner `frequenz_resampling::Resampler` with `start`
    /// set to the given boundary, preserving each one's telemetry broadcast
    /// receiver. Buffered telemetry from the jumped-over window is drained
    /// and discarded (including `Lagged` errors from the broadcast receiver,
    /// which can happen when the server bursts enough samples during the
    /// jump to fill the channel).
    fn rebuild_resamplers_after_jump(
        &self,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
        start: DateTime<Utc>,
    ) {
        for resampler in resamplers.values_mut() {
            // Drain any samples that were queued during the jump window;
            // they are timestamped on the old wall-clock frame and would
            // pollute the freshly-aligned resampler.
            while poll_telemetry(&mut resampler.receiver, resampler.component_id).is_some() {}
            resampler.resampler = self.build_resampler(resampler.metric, start);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use tokio_stream::{StreamExt, wrappers::BroadcastStream};

    use crate::{
        LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle,
        client::test_utils::{MockComponent, MockMicrogridApiClient, TokioSyncedClock},
        logical_meter::formula::Formula,
        quantity::Power,
    };

    async fn new_handle(
        meter: MockComponent,
        config: LogicalMeterConfig,
        clock: TokioSyncedClock,
    ) -> LogicalMeterHandle {
        let api_client = MockMicrogridApiClient::new_with_clock(
            MockComponent::grid(1).with_children(vec![meter]),
            clock.clone(),
        );
        LogicalMeterHandle::try_new_with_clock(
            MicrogridClientHandle::new_from_client(api_client),
            config,
            clock,
        )
        .await
        .unwrap()
    }

    // Pins the upstream contract that `rebuild_resamplers_after_jump`
    // relies on: after rebuilding with `start = current - interval`, a
    // `resample(current)` call on an empty buffer must yield exactly
    // one output, with `value() == None`. If `frequenz_resampling`
    // ever returns zero outputs for an empty window, the jump-recovery
    // path flips from a graceful `None` sample to a runtime
    // `ConnectionFailure("Resampling produced N values")`, so this
    // assumption deserves a focused regression test rather than only
    // implicit coverage from the end-to-end NTP-jump tests.
    #[test]
    fn test_resampler_empty_window_yields_single_none_sample() {
        let interval = TimeDelta::try_seconds(1).unwrap();
        let current = chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let start = current - interval;
        let mut resampler: frequenz_resampling::Resampler<f32, Sample<f32>> =
            frequenz_resampling::Resampler::new(
                interval,
                frequenz_resampling::ResamplingFunction::Average,
                3,
                start,
                false,
            );
        let result = resampler.resample(current);
        assert_eq!(
            result.len(),
            1,
            "rebuild contract: empty window must yield exactly one sample, got {}",
            result.len(),
        );
        assert!(
            result[0].clone().value().is_none(),
            "rebuild contract: empty window must yield None, got {:?}",
            result[0].value(),
        );
    }

    #[tokio::test]
    async fn test_nonpositive_resampling_interval_rejected() {
        let api_client = MockMicrogridApiClient::new(MockComponent::grid(1));
        let client = MicrogridClientHandle::new_from_client(api_client);
        for bad in [TimeDelta::zero(), -TimeDelta::try_milliseconds(1).unwrap()] {
            let (_tx, rx) = mpsc::channel(1);
            let result = LogicalMeterActor::try_new(
                rx,
                client.clone(),
                LogicalMeterConfig::new(bad),
                TokioSyncedClock::new(),
            );
            match result {
                Err(e) => assert_eq!(e.kind(), crate::ErrorKind::InvalidConfig),
                Ok(_) => panic!("expected error for interval {bad:?}"),
            }
        }
    }

    #[tokio::test]
    async fn test_max_age_in_intervals_overflow_rejected() {
        let api_client = MockMicrogridApiClient::new(MockComponent::grid(1));
        let client = MicrogridClientHandle::new_from_client(api_client);
        let (_tx, rx) = mpsc::channel(1);
        let config = LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap())
            .with_max_age_in_intervals(i32::MAX as u32 + 1);
        let result = LogicalMeterActor::try_new(rx, client, config, TokioSyncedClock::new());
        match result {
            Err(e) => assert_eq!(e.kind(), crate::ErrorKind::InvalidConfig),
            Ok(_) => panic!("expected error for over-i32::MAX max_age_in_intervals"),
        }
    }

    async fn next_sample(stream: &mut BroadcastStream<Sample<Power>>) -> Option<Sample<Power>> {
        loop {
            match tokio::time::timeout(std::time::Duration::from_secs(10), stream.next()).await {
                Ok(Some(Ok(s))) => return Some(s),
                Ok(Some(Err(_))) => continue,
                _ => return None,
            }
        }
    }

    /// Anchors a `TokioSyncedClock` to the next whole-second boundary, so
    /// samples emitted at `anchor + 200ms·N` from the mock land on
    /// resampler-window boundaries regardless of when in real wall-time
    /// the test runs. Without this, `Utc::now()`'s subsecond offset can
    /// place the first resampler tick before the mock has emitted
    /// anything, surfacing as a flaky `None` first sample.
    fn aligned_clock() -> TokioSyncedClock {
        let anchor =
            chrono::DateTime::from_timestamp(chrono::Utc::now().timestamp() + 1, 0).unwrap();
        TokioSyncedClock::with_wall_anchor(anchor)
    }

    #[tokio::test(start_paused = true)]
    async fn test_actor_emits_samples_for_subscribed_formula() {
        let meter = MockComponent::meter(2)
            .with_power(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]);
        let lm = new_handle(
            meter,
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()),
            aligned_clock(),
        )
        .await;
        let formula: Formula<Power> = lm.grid::<crate::metric::AcPowerActive>().unwrap();
        let rx = formula.subscribe().await.unwrap();
        let mut stream = BroadcastStream::new(rx);

        let first = next_sample(&mut stream).await.expect("no first sample");
        let second = next_sample(&mut stream).await.expect("no second sample");

        assert_eq!(
            second.timestamp() - first.timestamp(),
            TimeDelta::try_seconds(1).unwrap(),
        );
        assert!(first.value().is_some());
    }

    #[tokio::test(start_paused = true)]
    async fn test_actor_shares_subscription_across_handles() {
        let meter = MockComponent::meter(2)
            .with_power(vec![10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0]);
        let lm = new_handle(
            meter,
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()),
            aligned_clock(),
        )
        .await;
        let mut a = BroadcastStream::new(
            lm.grid::<crate::metric::AcPowerActive>()
                .unwrap()
                .subscribe()
                .await
                .unwrap(),
        );
        let mut b = BroadcastStream::new(
            lm.grid::<crate::metric::AcPowerActive>()
                .unwrap()
                .subscribe()
                .await
                .unwrap(),
        );

        let sa = next_sample(&mut a).await.expect("no sample on a");
        let sb = next_sample(&mut b).await.expect("no sample on b");
        assert_eq!(sa.timestamp(), sb.timestamp());
        assert_eq!(
            sa.value().map(|v| v.as_watts()),
            sb.value().map(|v| v.as_watts()),
        );
    }

    // Shared body for forward/backward NTP-jump recovery tests. Asserts the
    // sample-timestamp contract across the jump in addition to values:
    //
    //  - pre-jump cadence is exactly `interval`, values are the baseline 10 W
    //  - the first post-jump sample is the resync tick: `None`-valued and
    //    timestamped at `last_pre + jump + interval` (holds for signed `jump`)
    //  - subsequent ticks flow at `interval` cadence with post-jump values
    async fn run_ntp_jump_recovery(jump: TimeDelta) {
        let interval = TimeDelta::try_milliseconds(200).unwrap();
        let clock = aligned_clock();
        let power: Vec<f32> = (0..200).map(|i| if i < 10 { 10.0 } else { 99.0 }).collect();
        let meter = MockComponent::meter(2).with_power(power);

        let lm = new_handle(meter, LogicalMeterConfig::new(interval), clock.clone()).await;
        let formula = lm.grid::<crate::metric::AcPowerActive>().unwrap();
        let mut stream = BroadcastStream::new(formula.subscribe().await.unwrap());

        let mut pre = Vec::new();
        for _ in 0..4 {
            if let Some(s) = next_sample(&mut stream).await {
                pre.push(s);
            }
        }
        assert_eq!(pre.len(), 4, "expected 4 pre-jump samples");
        for w in pre.windows(2) {
            assert_eq!(
                w[1].timestamp() - w[0].timestamp(),
                interval,
                "pre-jump cadence should be {interval:?}",
            );
        }
        for s in &pre {
            assert_eq!(
                s.value().map(|v| v.as_watts()),
                Some(10.0),
                "pre-jump sample should be baseline 10.0 W, got {:?}",
                s.value(),
            );
        }
        let last_pre_ts = pre.last().unwrap().timestamp();

        clock.inject_wall_jump(jump);

        let resync = next_sample(&mut stream)
            .await
            .expect("no resync sample after jump");
        assert!(
            resync.value().is_none(),
            "resync tick should be None (buffered telemetry was on the old clock frame), got {:?}",
            resync.value(),
        );
        assert_eq!(
            resync.timestamp() - last_pre_ts,
            jump + interval,
            "resync sample should be jump + interval after the last pre-jump sample",
        );

        // Collect enough post-jump samples to see the mock's power profile
        // roll past its baseline-10 prefix into the 99 region. Cadence and
        // "resync was the only None" are invariants across every sample;
        // the 99 W value only needs to appear by the end of the window.
        let mut post = Vec::new();
        for _ in 0..10 {
            if let Some(s) = next_sample(&mut stream).await {
                post.push(s);
            }
        }
        assert_eq!(post.len(), 10, "expected 10 post-jump samples");
        assert_eq!(
            post[0].timestamp() - resync.timestamp(),
            interval,
            "first post-resync tick should be one interval after the resync tick",
        );
        for w in post.windows(2) {
            assert_eq!(
                w[1].timestamp() - w[0].timestamp(),
                interval,
                "post-jump cadence should be {interval:?}",
            );
        }
        for s in &post {
            assert!(
                s.value().is_some(),
                "post-resync samples should carry real values, got {:?}",
                s.value(),
            );
        }
        let last = post.last().unwrap();
        assert!(
            last.value()
                .map(|v| (v.as_watts() - 99.0).abs() < 0.01)
                .unwrap_or(false),
            "last post-jump sample should be ≈99.0 W, got {:?}",
            last.value(),
        );
    }

    // Realistic NTP resync: a single shared clock drives both the mock
    // telemetry's `sample_time`s and the actor's `WallClockTimer`. A mid-run
    // `inject_wall_jump(+30s)` appears to both sides simultaneously, like a
    // whole-machine NTP adjustment. The WallClockTimer detects the drift
    // between wall and monotonic on the next sleep, resyncs, and the actor
    // rebuilds the inner resamplers. Post-jump telemetry should flow through
    // again.
    #[tokio::test(start_paused = true)]
    async fn test_actor_recovers_from_whole_machine_ntp_jump() {
        run_ntp_jump_recovery(TimeDelta::try_seconds(30).unwrap()).await;
    }

    // Symmetric to the forward-jump test: a whole-machine backward NTP
    // adjustment should resync the timer and flow post-jump telemetry.
    #[tokio::test(start_paused = true)]
    async fn test_actor_recovers_from_whole_machine_backward_ntp_jump() {
        run_ntp_jump_recovery(-TimeDelta::try_seconds(30).unwrap()).await;
    }
}
