// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module contains the logical meter actor, that takes care of resampling
//! component data, evaluating formulas based on that data, and streaming the
//! data to subscribers.

use chrono::{DateTime, Utc};
use frequenz_microgrid_formula_engine::{self as engine, Reading, ValueSource};
use frequenz_resampling::ResamplingFunction;
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use std::collections::{HashMap, HashSet, hash_map::Entry};
use tokio::sync::{broadcast, mpsc};

use crate::client::proto::common::metrics::{Metric, metric_value_variant::MetricValueVariant};
use crate::logical_meter::formula::Key;
use crate::wall_clock_timer::{Clock, WallClockTimer};
use crate::{
    Error, MicrogridClientHandle, Sample,
    client::proto::common::microgrid::electrical_components::ElectricalComponentTelemetry,
};

use super::config::LogicalMeterConfig;

/// Delivers one evaluated value to one subscriber.
pub(crate) trait FormulaSink: Send {
    /// Sends the value; returns `false` once no receiver is left.
    fn send(&self, timestamp: DateTime<Utc>, value: Option<f32>) -> bool;
}

pub(crate) enum Instruction {
    SubscribeFormula {
        engine_formula: engine::Formula<f32, Key>,
        sink: Box<dyn FormulaSink>,
    },
}

/// One distinct expression and everyone listening to it.
struct SubscribedFormula {
    engine_formula: engine::Formula<f32, Key>,
    sinks: Vec<Box<dyn FormulaSink>>,
}

/// The state of a component's telemetry subscription.
enum Feed {
    /// The subscribe request is still in flight, or it failed.
    Pending,
    /// Attached to the component's telemetry stream.
    Live(broadcast::Receiver<ElectricalComponentTelemetry>),
}

struct ComponentSubscription {
    resampler: frequenz_resampling::Resampler<f32, Sample<f32>>,
    feed: Feed,
    /// Consecutive ticks on which no formula read this component, counted once
    /// the subscription is attached.
    idle_ticks: u32,
}

/// A subscribe request in flight, yielding the key it was sent for together
/// with its result.
type SubscribeRequest = BoxFuture<
    'static,
    (
        Key,
        Result<broadcast::Receiver<ElectricalComponentTelemetry>, Error>,
    ),
>;

/// Reads a tick's resampled values and records every key read. A key that is
/// not in the snapshot is unknown: a key with no subscription or a
/// [`Feed::Pending`] one, and every key on the first tick after a wall-clock
/// jump, whose snapshot is empty.
struct RecordingSource<'a> {
    snapshot: &'a HashMap<Key, Option<f32>>,
    reads: &'a mut HashSet<Key>,
}

impl ValueSource<f32, Key> for RecordingSource<'_> {
    fn read(&mut self, key: &Key) -> Reading<f32> {
        self.reads.insert(*key);
        match self.snapshot.get(key) {
            Some(value) => Reading::Known(*value),
            None => Reading::Unknown,
        }
    }
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

pub(super) struct LogicalMeterActor<C: Clock> {
    instructions_rx: mpsc::Receiver<Instruction>,
    client: MicrogridClientHandle,
    config: LogicalMeterConfig,
    resampler_ts: DateTime<Utc>,
    resampler_timer: WallClockTimer<C>,
    /// The snapshot the formulas were last evaluated against, used to seed the
    /// component demand of a newly subscribed expression. A tick after a
    /// wall-clock jump stores an empty snapshot. A tick whose resampling fails
    /// leaves the previous snapshot in place.
    last_snapshot: HashMap<Key, Option<f32>>,
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
            last_snapshot: HashMap::new(),
        })
    }

    pub async fn run(mut self) {
        let mut subscriptions: HashMap<Key, ComponentSubscription> = HashMap::new();
        let mut formulas: HashMap<String, SubscribedFormula> = HashMap::new();
        let mut subscribe_requests: FuturesUnordered<SubscribeRequest> = FuturesUnordered::new();

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
                            &mut subscriptions,
                            realigned_current - self.config.resampling_interval,
                        );
                        self.resampler_ts = realigned_current;
                    } else {
                        self.resampler_ts = tick_info.expected_tick_time;
                    }

                    let snapshot = match self.resample(&mut subscriptions) {
                        Ok(snapshot) => snapshot,
                        Err(err) => {
                            tracing::error!("Error resampling metrics: {}", err);
                            continue;
                        }
                    };
                    if tick_info.resynced {
                        // This tick's resampled values have no data. Read as
                        // known-missing, they would make each COALESCE
                        // subscribe its fallback, and `COALESCE(.., 0)` emit a
                        // made-up 0. So evaluate against an empty snapshot:
                        // every component reads as unknown, and a formula that
                        // reads one emits `None`. Leave the subscriptions as
                        // they are: nothing is subscribed, and no component is
                        // counted as unread.
                        let empty = HashMap::new();
                        self.evaluate_formulas(&empty, &mut formulas);
                        self.last_snapshot = empty;
                    } else {
                        let used = self.evaluate_formulas(&snapshot, &mut formulas);
                        self.last_snapshot = snapshot;
                        self.reconcile_subscriptions(
                            &used,
                            &mut subscriptions,
                            &mut subscribe_requests,
                        );
                    }
                }
                Some((key, result)) = subscribe_requests.next(),
                    if !subscribe_requests.is_empty() => {
                    self.attach_subscription(key, result, &mut subscriptions);
                }
                instruction = self.instructions_rx.recv() => {
                    match instruction {
                        Some(Instruction::SubscribeFormula{engine_formula, sink}) => {
                            self.handle_subscribe_formula(
                                engine_formula,
                                sink,
                                &mut formulas,
                                &mut subscriptions,
                                &mut subscribe_requests,
                            );
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

    /// Resamples every subscribed component metric and returns this tick's
    /// values by key.
    ///
    /// A [`Feed::Pending`] subscription is left out of the snapshot, so its key
    /// reads as [`Reading::Unknown`]. Its resampler is still advanced: one that
    /// falls behind returns several values on its next call.
    fn resample(
        &self,
        subscriptions: &mut HashMap<Key, ComponentSubscription>,
    ) -> Result<HashMap<Key, Option<f32>>, Error> {
        let mut snapshot = HashMap::with_capacity(subscriptions.len());
        for (key, subscription) in subscriptions.iter_mut() {
            if let Feed::Live(receiver) = &mut subscription.feed {
                while let Some(data) = poll_telemetry(receiver, key.component_id) {
                    Self::push_to_resampler(&mut subscription.resampler, *key, data);
                }
            }
            let resampled = subscription.resampler.resample(self.resampler_ts);
            if resampled.len() != 1 {
                return Err(Error::connection_failure(format!(
                    "Resampling produced {} values",
                    resampled.len()
                )));
            }
            if matches!(subscription.feed, Feed::Live(_)) {
                snapshot.insert(*key, resampled[0].clone().value());
            }
        }
        Ok(snapshot)
    }

    /// Evaluates every formula against `snapshot` and sends the results. Sinks
    /// without receivers are dropped, and formulas without sinks with them.
    /// Returns the union of the keys the surviving formulas read, which is this
    /// tick's component demand.
    fn evaluate_formulas(
        &self,
        snapshot: &HashMap<Key, Option<f32>>,
        formulas: &mut HashMap<String, SubscribedFormula>,
    ) -> HashSet<Key> {
        let timestamp = self.resampler_ts;
        let mut used = HashSet::new();
        let mut reads = HashSet::new();
        formulas.retain(|rendered, formula| {
            reads.clear();
            let value = match formula.engine_formula.evaluate(&mut RecordingSource {
                snapshot,
                reads: &mut reads,
            }) {
                Ok(Reading::Known(value)) => value,
                Ok(Reading::Unknown) => None,
                Err(err) => {
                    tracing::error!("Failed to evaluate formula {rendered}: {err}");
                    None
                }
            };
            formula.sinks.retain(|sink| sink.send(timestamp, value));
            if formula.sinks.is_empty() {
                tracing::debug!("Dropping formula without subscribers: {rendered}");
                return false;
            }
            used.extend(reads.drain());
            true
        });
        used
    }

    /// Starts subscriptions for keys that were read but have no subscription,
    /// and drops attached subscriptions that have gone unread for
    /// `unsubscribe_after_idle_intervals` ticks.
    ///
    /// A pending subscription is never dropped, so at most one subscribe
    /// request is in flight per key.
    fn reconcile_subscriptions(
        &self,
        used: &HashSet<Key>,
        subscriptions: &mut HashMap<Key, ComponentSubscription>,
        subscribe_requests: &mut FuturesUnordered<SubscribeRequest>,
    ) {
        self.start_missing_subscriptions(used, subscriptions, subscribe_requests);
        let limit = self.config.unsubscribe_after_idle_intervals;
        subscriptions.retain(|key, subscription| {
            if used.contains(key) {
                subscription.idle_ticks = 0;
                return true;
            }
            if matches!(subscription.feed, Feed::Pending) {
                return true;
            }
            subscription.idle_ticks += 1;
            if subscription.idle_ticks >= limit {
                tracing::debug!("Dropping subscription of unread component {key}");
                return false;
            }
            true
        });
    }

    /// Registers a subscription for every key in `used` that has none and
    /// starts its telemetry subscription without blocking the actor.
    fn start_missing_subscriptions(
        &self,
        used: &HashSet<Key>,
        subscriptions: &mut HashMap<Key, ComponentSubscription>,
        subscribe_requests: &mut FuturesUnordered<SubscribeRequest>,
    ) {
        for &key in used {
            let Entry::Vacant(entry) = subscriptions.entry(key) else {
                continue;
            };
            entry.insert(ComponentSubscription {
                resampler: self.build_resampler(key.metric, self.resampler_ts),
                feed: Feed::Pending,
                idle_ticks: 0,
            });
            tracing::debug!("Subscribing to {key}");
            let client = self.client.clone();
            subscribe_requests.push(Box::pin(async move {
                let result = client
                    .receive_electrical_component_telemetry_stream(key.component_id)
                    .await;
                (key, result)
            }));
        }
    }

    /// Attaches the result of a subscribe request to the key's subscription.
    ///
    /// The request fails only once the client actor has stopped, so it could
    /// never succeed later. The error is logged once and the subscription stays
    /// pending. Its entry is kept, so no new request is sent for the key.
    fn attach_subscription(
        &self,
        key: Key,
        result: Result<broadcast::Receiver<ElectricalComponentTelemetry>, Error>,
        subscriptions: &mut HashMap<Key, ComponentSubscription>,
    ) {
        let Some(subscription) = subscriptions.get_mut(&key) else {
            tracing::debug!("Subscription for {key} completed, but it is gone");
            return;
        };
        match result {
            Ok(receiver) => {
                tracing::debug!("Subscribed to {key}");
                subscription.feed = Feed::Live(receiver);
            }
            Err(err) => tracing::error!("Subscribing to {key} failed, not retrying: {err}"),
        }
    }

    /// Registers a subscriber for `engine_formula`, creating the formula on
    /// first sight of the expression and pushing the sink onto it otherwise.
    ///
    /// Every subscription re-seeds demand: the expression is evaluated against
    /// the last snapshot only to find the components it needs; any not yet
    /// subscribed are subscribed at once. Nothing is sent to the sinks.
    fn handle_subscribe_formula(
        &self,
        engine_formula: engine::Formula<f32, Key>,
        sink: Box<dyn FormulaSink>,
        formulas: &mut HashMap<String, SubscribedFormula>,
        subscriptions: &mut HashMap<Key, ComponentSubscription>,
        subscribe_requests: &mut FuturesUnordered<SubscribeRequest>,
    ) {
        let formula = formulas
            .entry(engine_formula.to_string())
            .or_insert_with(|| SubscribedFormula {
                engine_formula,
                sinks: Vec::new(),
            });
        formula.sinks.push(sink);
        let mut reads = HashSet::new();
        if let Err(err) = formula.engine_formula.evaluate(&mut RecordingSource {
            snapshot: &self.last_snapshot,
            reads: &mut reads,
        }) {
            tracing::warn!(
                "Seed evaluation of formula {} failed: {err}",
                formula.engine_formula
            );
        }
        self.start_missing_subscriptions(&reads, subscriptions, subscribe_requests);
    }

    /// Rebuilds every inner `frequenz_resampling::Resampler` with `start`
    /// set to the given boundary, preserving each one's telemetry broadcast
    /// receiver. Buffered telemetry from the jumped-over window is drained
    /// and discarded (including `Lagged` errors from the broadcast receiver,
    /// which can happen when the server bursts enough samples during the
    /// jump to fill the channel).
    fn rebuild_resamplers_after_jump(
        &self,
        subscriptions: &mut HashMap<Key, ComponentSubscription>,
        start: DateTime<Utc>,
    ) {
        for (key, subscription) in subscriptions.iter_mut() {
            // Drain any samples that were queued during the jump window;
            // they are timestamped on the old wall-clock frame and would
            // pollute the freshly-aligned resampler.
            if let Feed::Live(receiver) = &mut subscription.feed {
                while poll_telemetry(receiver, key.component_id).is_some() {}
            }
            subscription.resampler = self.build_resampler(key.metric, start);
        }
    }

    /// Extracts the resampler's metric from the given telemetry and pushes it
    /// to the resampler's internal buffer.
    fn push_to_resampler(
        resampler: &mut frequenz_resampling::Resampler<f32, Sample<f32>>,
        key: Key,
        data: ElectricalComponentTelemetry,
    ) {
        let metric = key.metric;
        let Some(dd) = data
            .metric_samples
            .iter()
            .find(|s| s.metric == metric as i32)
        else {
            tracing::debug!(
                "No data for metric {:?} in component {}",
                metric,
                key.component_id
            );
            return;
        };
        let Some(timestamp) = dd.sample_time.and_then(|timestamp| {
            DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
        }) else {
            return;
        };
        let Some(variant) = dd
            .value
            .as_ref()
            .and_then(|value| value.metric_value_variant.as_ref())
        else {
            return;
        };
        let value = Some(match variant {
            MetricValueVariant::SimpleMetric(value) => value.value,
            MetricValueVariant::AggregatedMetric(value) => value.avg_value,
        });

        let sample = Sample::new(timestamp, value);

        resampler.push(sample);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};
    use tokio_stream::{StreamExt, wrappers::BroadcastStream};

    use crate::{
        LogicalMeterConfig, LogicalMeterHandle, MicrogridClientHandle,
        client::test_utils::{
            MockComponent, MockMicrogridApiClient, OpenStreams, TokioSyncedClock,
            wait_for_open_streams,
        },
        logical_meter::formula::Formula,
        quantity::{Frequency, Power, Quantity},
    };

    /// What a [`RecordingSink`] was sent, shared with the test.
    type Recorded = Arc<Mutex<Vec<(DateTime<Utc>, Option<f32>)>>>;

    /// A sink whose subscriber is still listening; records what it is sent.
    struct RecordingSink {
        recorded: Recorded,
    }

    impl FormulaSink for RecordingSink {
        fn send(&self, timestamp: DateTime<Utc>, value: Option<f32>) -> bool {
            self.recorded.lock().unwrap().push((timestamp, value));
            true
        }
    }

    /// A sink whose subscriber is gone: every send fails.
    struct DeadSink;

    impl FormulaSink for DeadSink {
        fn send(&self, _timestamp: DateTime<Utc>, _value: Option<f32>) -> bool {
            false
        }
    }

    fn recording_sink() -> (Box<dyn FormulaSink>, Recorded) {
        let recorded = Arc::new(Mutex::new(Vec::new()));
        (
            Box::new(RecordingSink {
                recorded: recorded.clone(),
            }),
            recorded,
        )
    }

    /// An actor that is never run, for exercising its per-tick bookkeeping
    /// methods directly.
    fn bare_actor() -> LogicalMeterActor<TokioSyncedClock> {
        bare_actor_with(LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()))
    }

    fn bare_actor_with(config: LogicalMeterConfig) -> LogicalMeterActor<TokioSyncedClock> {
        let api_client = MockMicrogridApiClient::new(MockComponent::grid(1));
        let (_tx, rx) = mpsc::channel(1);
        LogicalMeterActor::try_new(
            rx,
            MicrogridClientHandle::new_from_client(api_client),
            config,
            TokioSyncedClock::new(),
        )
        .unwrap()
    }

    fn active_power_key(component_id: u64) -> Key {
        Key {
            metric: Metric::AcPowerActive,
            component_id,
        }
    }

    /// A subscription for `key` in state `feed`, with its resampler aligned to
    /// `start`.
    fn subscription(
        actor: &LogicalMeterActor<TokioSyncedClock>,
        key: Key,
        start: DateTime<Utc>,
        feed: Feed,
    ) -> ComponentSubscription {
        ComponentSubscription {
            resampler: actor.build_resampler(key.metric, start),
            feed,
            idle_ticks: 0,
        }
    }

    fn active_power_formula(formula: &str) -> engine::Formula<f32, Key> {
        formula
            .parse::<engine::Formula<f32>>()
            .unwrap()
            .map_components(active_power_key)
    }

    fn subscribed_formula(formula: &str, sinks: Vec<Box<dyn FormulaSink>>) -> SubscribedFormula {
        SubscribedFormula {
            engine_formula: active_power_formula(formula),
            sinks,
        }
    }

    #[tokio::test]
    async fn test_evaluate_formulas_sends_to_live_sinks_and_prunes_dead_ones() {
        let actor = bare_actor();
        let (live, recorded) = recording_sink();
        let mut formulas = HashMap::from([(
            "#2".to_string(),
            subscribed_formula("#2", vec![live, Box::new(DeadSink)]),
        )]);

        let snapshot = HashMap::from([(active_power_key(2), Some(7.5))]);
        actor.evaluate_formulas(&snapshot, &mut formulas);

        assert_eq!(
            formulas["#2"].sinks.len(),
            1,
            "the dead sink should be gone"
        );
        assert_eq!(
            *recorded.lock().unwrap(),
            vec![(actor.resampler_ts, Some(7.5))],
        );

        // A key that is not in the snapshot reads as unknown, which the sink
        // sees as `None`.
        actor.evaluate_formulas(&HashMap::new(), &mut formulas);
        assert_eq!(
            *recorded.lock().unwrap(),
            vec![(actor.resampler_ts, Some(7.5)), (actor.resampler_ts, None)],
        );
        assert_eq!(formulas["#2"].sinks.len(), 1);
    }

    #[tokio::test]
    async fn test_evaluate_formulas_mixes_metrics() {
        let actor = bare_actor();
        let (live, recorded) = recording_sink();
        let ac_power = active_power_key(2);
        let dc_power = Key {
            metric: Metric::DcPower,
            component_id: 2,
        };
        // The inverter's loss: leaf `#2` is its DC power, leaf `#3` its AC
        // power.
        let engine_formula = "#2 - #3"
            .parse::<engine::Formula<f32>>()
            .unwrap()
            .map_components(|id| if id == 2 { dc_power } else { ac_power });
        let mut formulas = HashMap::from([(
            engine_formula.to_string(),
            SubscribedFormula {
                engine_formula,
                sinks: vec![live],
            },
        )]);

        let snapshot = HashMap::from([(dc_power, Some(10.0)), (ac_power, Some(9.5))]);
        let used = actor.evaluate_formulas(&snapshot, &mut formulas);

        assert_eq!(
            *recorded.lock().unwrap(),
            vec![(actor.resampler_ts, Some(0.5))]
        );
        assert_eq!(used, HashSet::from([dc_power, ac_power]));
    }

    #[tokio::test]
    async fn test_evaluate_formulas_with_a_missing_operand() {
        let actor = bare_actor();
        // Every operator needs every operand, except `AVG` and `COALESCE`,
        // which skip operands whose value is `None`. A NaN value counts as
        // `None`.
        for missing in [None, Some(f32::NAN)] {
            let snapshot = HashMap::from([
                (active_power_key(2), Some(1.0)),
                (active_power_key(3), missing),
            ]);
            for (formula, expected) in [
                ("AVG(#2, #3)", Some(1.0)),
                ("MIN(#2, #3)", None),
                ("#2 + #3", None),
                ("COALESCE(#3, #2)", Some(1.0)),
            ] {
                let (sink, recorded) = recording_sink();
                let mut formulas =
                    HashMap::from([(formula.to_string(), subscribed_formula(formula, vec![sink]))]);
                actor.evaluate_formulas(&snapshot, &mut formulas);
                assert_eq!(
                    recorded.lock().unwrap()[0].1,
                    expected,
                    "{formula} with #3 = {missing:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_evaluate_formulas_reads_each_metric_separately() {
        let actor = bare_actor();
        let voltage = Key {
            metric: Metric::AcVoltage,
            component_id: 2,
        };
        let engine_formula =
            engine::Formula::Component(active_power_key(2)) + engine::Formula::Component(voltage);
        let (sink, recorded) = recording_sink();
        let mut formulas = HashMap::from([(
            engine_formula.to_string(),
            SubscribedFormula {
                engine_formula,
                sinks: vec![sink],
            },
        )]);
        let snapshot = HashMap::from([(active_power_key(2), Some(1.0)), (voltage, Some(230.0))]);

        actor.evaluate_formulas(&snapshot, &mut formulas);

        assert_eq!(recorded.lock().unwrap()[0].1, Some(231.0));
    }

    #[tokio::test]
    async fn test_evaluate_formulas_drops_formulas_that_lost_their_last_sink() {
        let actor = bare_actor();
        let mut formulas = HashMap::from([(
            "#2".to_string(),
            subscribed_formula("#2", vec![Box::new(DeadSink)]),
        )]);

        let used = actor.evaluate_formulas(
            &HashMap::from([(active_power_key(2), Some(1.0))]),
            &mut formulas,
        );

        assert!(
            formulas.is_empty(),
            "the formula should go with its last sink"
        );
        assert!(
            used.is_empty(),
            "a dropped formula must not demand its components"
        );
    }

    #[tokio::test]
    async fn test_reconcile_subscriptions_ages_out_unread_keys() {
        let limit = 2;
        let actor = bare_actor_with(
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap())
                .with_unsubscribe_after_idle_intervals(limit),
        );
        let mut subscriptions = HashMap::new();
        for component_id in [2, 3] {
            let key = active_power_key(component_id);
            let (_tx, receiver) = broadcast::channel(1);
            subscriptions.insert(
                key,
                subscription(&actor, key, actor.resampler_ts, Feed::Live(receiver)),
            );
        }
        // Component 4's subscribe request is still in flight.
        let pending_key = active_power_key(4);
        subscriptions.insert(
            pending_key,
            subscription(&actor, pending_key, actor.resampler_ts, Feed::Pending),
        );
        let used = HashSet::from([active_power_key(2)]);
        let mut subscribe_requests = FuturesUnordered::new();

        // An unread key survives until it has gone unread for `limit`
        // consecutive ticks.
        for tick in 1..limit {
            actor.reconcile_subscriptions(&used, &mut subscriptions, &mut subscribe_requests);
            assert_eq!(subscriptions.len(), 3, "dropped too early on tick {tick}");
        }
        actor.reconcile_subscriptions(&used, &mut subscriptions, &mut subscribe_requests);

        assert_eq!(subscriptions.len(), 2);
        assert!(subscriptions.contains_key(&active_power_key(2)));
        assert_eq!(subscriptions[&active_power_key(2)].idle_ticks, 0);

        // A pending subscription never ages out, and its idle ticks are not
        // counted.
        for _ in 0..limit {
            actor.reconcile_subscriptions(&used, &mut subscriptions, &mut subscribe_requests);
        }
        assert!(matches!(subscriptions[&pending_key].feed, Feed::Pending));
        assert_eq!(subscriptions[&pending_key].idle_ticks, 0);
        assert!(
            subscribe_requests.is_empty(),
            "no subscription should start for an already-resampled key"
        );
    }

    #[tokio::test]
    async fn test_pending_subscription_reads_as_unknown() {
        let mut actor = bare_actor();
        let start = actor.resampler_ts - actor.config.resampling_interval;
        let pending_key = active_power_key(2);
        let attached_key = active_power_key(3);
        let (_tx, receiver) = broadcast::channel(1);
        let mut subscriptions = HashMap::from([
            // The pending key's subscription is still in flight.
            (
                pending_key,
                subscription(&actor, pending_key, start, Feed::Pending),
            ),
            (
                attached_key,
                subscription(&actor, attached_key, start, Feed::Live(receiver)),
            ),
        ]);

        let snapshot = actor.resample(&mut subscriptions).unwrap();

        assert_eq!(
            snapshot.keys().copied().collect::<HashSet<_>>(),
            HashSet::from([attached_key]),
            "a pending subscription must stay out of the snapshot so it reads unknown",
        );

        let (sink, recorded) = recording_sink();
        let mut formulas = HashMap::from([(
            "COALESCE(#2, #3)".to_string(),
            subscribed_formula("COALESCE(#2, #3)", vec![sink]),
        )]);

        let used = actor.evaluate_formulas(&snapshot, &mut formulas);

        assert_eq!(
            *recorded.lock().unwrap(),
            vec![(actor.resampler_ts, None)],
            "an unknown primary makes the whole coalesce unknown, sent as None",
        );
        assert_eq!(
            used,
            HashSet::from([pending_key]),
            "COALESCE stops at the unknown key and demands nothing beyond it",
        );

        // The pending subscription's resampler kept advancing, so the first
        // resample after its receiver attaches yields exactly one value for it.
        let (_tx, receiver) = broadcast::channel(1);
        subscriptions.get_mut(&pending_key).unwrap().feed = Feed::Live(receiver);
        actor.resampler_ts += actor.config.resampling_interval;
        let snapshot = actor.resample(&mut subscriptions).unwrap();
        assert!(snapshot.contains_key(&pending_key));
    }

    #[tokio::test]
    async fn test_reconcile_subscriptions_starts_missing_keys() {
        let actor = bare_actor();
        let mut subscriptions = HashMap::new();
        let mut subscribe_requests = FuturesUnordered::new();
        let used = HashSet::from([active_power_key(2)]);

        actor.reconcile_subscriptions(&used, &mut subscriptions, &mut subscribe_requests);

        assert_eq!(subscriptions.len(), 1);
        let subscription = &subscriptions[&active_power_key(2)];
        assert!(
            matches!(subscription.feed, Feed::Pending),
            "the receiver attaches only once the subscription completes"
        );
        assert_eq!(subscribe_requests.len(), 1);
    }

    #[tokio::test]
    async fn test_reconcile_subscriptions_resets_the_idle_count_on_a_read() {
        let limit = 3;
        let actor = bare_actor_with(
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap())
                .with_unsubscribe_after_idle_intervals(limit),
        );
        let key = active_power_key(2);
        let (_tx, receiver) = broadcast::channel(1);
        let mut subscriptions = HashMap::from([(
            key,
            subscription(&actor, key, actor.resampler_ts, Feed::Live(receiver)),
        )]);
        let mut subscribe_requests = FuturesUnordered::new();
        let unread = HashSet::new();

        for _ in 1..limit {
            actor.reconcile_subscriptions(&unread, &mut subscriptions, &mut subscribe_requests);
        }
        // One read starts the count again.
        actor.reconcile_subscriptions(
            &HashSet::from([key]),
            &mut subscriptions,
            &mut subscribe_requests,
        );
        for tick in 1..limit {
            actor.reconcile_subscriptions(&unread, &mut subscriptions, &mut subscribe_requests);
            assert!(
                subscriptions.contains_key(&key),
                "dropped on unread tick {tick} after the read"
            );
        }
        actor.reconcile_subscriptions(&unread, &mut subscriptions, &mut subscribe_requests);
        assert!(
            !subscriptions.contains_key(&key),
            "not dropped after {limit} unread ticks in a row"
        );
    }

    #[tokio::test]
    async fn test_handle_subscribe_formula_seeds_demand_from_the_last_snapshot() {
        let mut actor = bare_actor();
        let primary = active_power_key(2);
        let fallback = active_power_key(3);
        let (_tx, receiver) = broadcast::channel(1);
        let mut subscriptions = HashMap::from([(
            primary,
            subscription(&actor, primary, actor.resampler_ts, Feed::Live(receiver)),
        )]);
        // The last tick found the primary missing.
        actor.last_snapshot = HashMap::from([(primary, None)]);
        let mut formulas = HashMap::new();
        let mut subscribe_requests = FuturesUnordered::new();
        let (sink, _) = recording_sink();

        actor.handle_subscribe_formula(
            active_power_formula("COALESCE(#2, #3)"),
            sink,
            &mut formulas,
            &mut subscriptions,
            &mut subscribe_requests,
        );

        assert!(
            matches!(
                subscriptions.get(&fallback).map(|s| &s.feed),
                Some(Feed::Pending)
            ),
            "the fallback must be subscribed at once, not on the next tick"
        );
        assert_eq!(subscribe_requests.len(), 1);
    }

    #[tokio::test]
    async fn test_failed_subscription_stays_pending_and_is_not_resent() {
        let actor = bare_actor();
        let key = active_power_key(2);
        let mut subscriptions = HashMap::from([(
            key,
            subscription(&actor, key, actor.resampler_ts, Feed::Pending),
        )]);

        // The request fails only once the client actor has stopped.
        actor.attach_subscription(
            key,
            Err(Error::connection_failure("client stopped")),
            &mut subscriptions,
        );
        assert!(matches!(subscriptions[&key].feed, Feed::Pending));

        let mut subscribe_requests = FuturesUnordered::new();
        actor.reconcile_subscriptions(
            &HashSet::from([key]),
            &mut subscriptions,
            &mut subscribe_requests,
        );
        assert!(matches!(subscriptions[&key].feed, Feed::Pending));
        assert!(
            subscribe_requests.is_empty(),
            "no new request is sent for a failed subscription"
        );
    }

    async fn new_handle(
        meter: MockComponent,
        config: LogicalMeterConfig,
        clock: TokioSyncedClock,
    ) -> LogicalMeterHandle {
        handle_for(
            MockComponent::grid(1).with_children(vec![meter]),
            config,
            clock,
        )
        .await
        .0
    }

    async fn handle_for(
        root: MockComponent,
        config: LogicalMeterConfig,
        clock: TokioSyncedClock,
    ) -> (LogicalMeterHandle, Arc<Mutex<OpenStreams>>) {
        let api_client = MockMicrogridApiClient::new_with_clock(root, clock.clone());
        let open = api_client.open_telemetry_streams();
        let lm = LogicalMeterHandle::try_new_with_clock(
            MicrogridClientHandle::new_from_client(api_client),
            config,
            clock,
        )
        .await
        .unwrap();
        (lm, open)
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

    async fn next_sample<Q: Quantity + 'static>(
        stream: &mut BroadcastStream<Sample<Q>>,
    ) -> Option<Sample<Q>> {
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
    async fn test_actor_emits_samples_for_subscribed_frequency_formula() {
        let meter = MockComponent::meter(2)
            .with_frequency(vec![50.0, 50.1, 49.9, 50.0, 50.2, 49.8, 50.0, 50.1]);
        let lm = new_handle(
            meter,
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap()),
            aligned_clock(),
        )
        .await;
        let formula: Formula<Frequency> = lm.grid::<crate::metric::AcFrequency>().unwrap();
        let rx = formula.subscribe().await.unwrap();
        let mut stream = BroadcastStream::new(rx);

        let first = next_sample(&mut stream)
            .await
            .expect("no first frequency sample");
        assert!(
            first.value().is_some(),
            "expected a frequency value to be streamed for the grid",
        );
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

    /// `grid(1) -> meter(2) -> meter(3) -> pv_inverter(4)`: the PV formula is
    /// `COALESCE(#4, #3, 0)`, so the inverter is the primary and the meter the
    /// fallback.
    fn pv_chain(inverter: MockComponent, meter: MockComponent) -> MockComponent {
        MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![meter.with_children(vec![inverter])]),
        ])
    }

    async fn pv_chain_handle(
        inverter: MockComponent,
        meter: MockComponent,
        config: LogicalMeterConfig,
    ) -> (LogicalMeterHandle, Arc<Mutex<OpenStreams>>) {
        handle_for(pv_chain(inverter, meter), config, aligned_clock()).await
    }

    #[tokio::test(start_paused = true)]
    async fn test_only_the_primary_is_subscribed_while_it_delivers() {
        let interval = TimeDelta::try_seconds(1).unwrap();
        let (lm, open) = pv_chain_handle(
            MockComponent::pv_inverter(4).with_power(vec![10.0; 400]),
            MockComponent::meter(3).with_power(vec![1.0; 400]),
            LogicalMeterConfig::new(interval),
        )
        .await;
        let formula = lm.pv::<crate::metric::AcPowerActive>(None).unwrap();
        let mut stream = BroadcastStream::new(formula.subscribe().await.unwrap());

        for _ in 0..4 {
            let sample = next_sample(&mut stream).await.expect("no sample");
            assert_eq!(sample.value().map(|v| v.as_watts()), Some(10.0));
        }
        assert_eq!(open.lock().unwrap().ids(), BTreeSet::from([4]));
    }

    #[tokio::test(start_paused = true)]
    async fn test_fallback_is_subscribed_when_the_primary_goes_silent() {
        let interval = TimeDelta::try_seconds(1).unwrap();
        let (lm, open) = pv_chain_handle(
            MockComponent::pv_inverter(4)
                .with_power(vec![10.0; 5])
                .with_silence_after_metrics(),
            MockComponent::meter(3).with_power(vec![1.0; 400]),
            LogicalMeterConfig::new(interval),
        )
        .await;
        let formula = lm.pv::<crate::metric::AcPowerActive>(None).unwrap();
        let mut stream = BroadcastStream::new(formula.subscribe().await.unwrap());

        let mut values = Vec::new();
        for _ in 0..12 {
            let sample = next_sample(&mut stream).await.expect("no sample");
            values.push(sample.value().map(|v| v.as_watts()));
            if values.last() == Some(&Some(1.0)) {
                break;
            }
        }
        assert_eq!(values.first(), Some(&Some(10.0)), "{values:?}");
        assert_eq!(
            values.iter().filter(|value| value.is_none()).count(),
            1,
            "expected exactly one None while falling back: {values:?}"
        );
        assert_eq!(values.last(), Some(&Some(1.0)), "{values:?}");
        assert!(
            open.lock().unwrap().contains(3),
            "{:?}",
            open.lock().unwrap().ids()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_subscriptions_age_out_after_the_last_subscriber_drops() {
        let interval = TimeDelta::try_seconds(1).unwrap();
        let (lm, open) = pv_chain_handle(
            MockComponent::pv_inverter(4).with_power(vec![10.0; 400]),
            MockComponent::meter(3).with_power(vec![1.0; 400]),
            LogicalMeterConfig::new(interval).with_unsubscribe_after_idle_intervals(2),
        )
        .await;
        let formula = lm.pv::<crate::metric::AcPowerActive>(None).unwrap();
        let mut stream = BroadcastStream::new(formula.subscribe().await.unwrap());
        let _ = next_sample(&mut stream).await.expect("no sample");
        assert_eq!(open.lock().unwrap().ids(), BTreeSet::from([4]));

        drop(stream);
        // Up to six resampling intervals of simulated time.
        wait_for_open_streams(&open, BTreeSet::new(), interval.to_std().unwrap() / 5, 30).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_resync_tick_reads_every_component_as_unknown() {
        let interval = TimeDelta::try_seconds(1).unwrap();
        let clock = aligned_clock();
        // Inverter 5 has no meter of its own, so its PV formula is
        // `COALESCE(#5, 0)`.
        let (lm, open) = handle_for(
            MockComponent::grid(1).with_children(vec![MockComponent::meter(2).with_children(
                vec![
                    MockComponent::meter(3)
                        .with_power(vec![1.0; 400])
                        .with_children(vec![
                            MockComponent::pv_inverter(4).with_power(vec![10.0; 400]),
                        ]),
                    MockComponent::pv_inverter(5).with_power(vec![20.0; 400]),
                ],
            )]),
            LogicalMeterConfig::new(interval),
            clock.clone(),
        )
        .await;
        let pv_4 = lm
            .pv::<crate::metric::AcPowerActive>(Some(BTreeSet::from([4])))
            .unwrap();
        let pv_5 = lm
            .pv::<crate::metric::AcPowerActive>(Some(BTreeSet::from([5])))
            .unwrap();
        assert_eq!(pv_5.to_string(), "COALESCE(#5:AC_POWER_ACTIVE, 0)");
        let mut stream_4 = BroadcastStream::new(pv_4.subscribe().await.unwrap());
        let mut stream_5 = BroadcastStream::new(pv_5.subscribe().await.unwrap());
        for _ in 0..2 {
            let sample = next_sample(&mut stream_4).await.expect("no sample");
            assert_eq!(sample.value().map(|v| v.as_watts()), Some(10.0));
            let sample = next_sample(&mut stream_5).await.expect("no sample");
            assert_eq!(sample.value().map(|v| v.as_watts()), Some(20.0));
        }

        clock.inject_wall_jump(TimeDelta::try_seconds(30).unwrap());

        // On the resync tick the inverters have no data. They must read as
        // unknown, not as missing, so the meter fallback is not subscribed, and
        // `COALESCE(#5, 0)` does not send 0.
        let resync = next_sample(&mut stream_4).await.expect("no resync sample");
        assert_eq!(resync.value().map(|v| v.as_watts()), None);
        let resync = next_sample(&mut stream_5).await.expect("no resync sample");
        assert_eq!(resync.value().map(|v| v.as_watts()), None);
        for _ in 0..4 {
            let sample = next_sample(&mut stream_4).await.expect("no sample");
            assert_eq!(sample.value().map(|v| v.as_watts()), Some(10.0));
            let sample = next_sample(&mut stream_5).await.expect("no sample");
            assert_eq!(sample.value().map(|v| v.as_watts()), Some(20.0));
            assert_eq!(open.lock().unwrap().ids(), BTreeSet::from([4, 5]));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_resync_tick_keeps_the_subscriptions() {
        let interval = TimeDelta::try_seconds(1).unwrap();
        let clock = aligned_clock();
        // The inverter sends no active power, so the formula reads the meter
        // fallback on every tick.
        let (lm, open) = handle_for(
            pv_chain(
                MockComponent::pv_inverter(4).with_reactive_power(vec![5.0; 400]),
                MockComponent::meter(3).with_power(vec![1.0; 400]),
            ),
            LogicalMeterConfig::new(interval).with_unsubscribe_after_idle_intervals(1),
            clock.clone(),
        )
        .await;
        let formula = lm.pv::<crate::metric::AcPowerActive>(None).unwrap();
        let mut stream = BroadcastStream::new(formula.subscribe().await.unwrap());
        let mut values = Vec::new();
        while values.last() != Some(&Some(1.0)) {
            assert!(values.len() < 5, "no fallback value: {values:?}");
            let sample = next_sample(&mut stream).await.expect("no sample");
            values.push(sample.value().map(|v| v.as_watts()));
        }

        clock.inject_wall_jump(TimeDelta::try_seconds(30).unwrap());

        // The resync tick reads only the inverter, which is unknown. It must
        // not count as a tick on which the meter went unread, or the meter is
        // dropped and the next sample is `None` while it is subscribed again.
        let resync = next_sample(&mut stream).await.expect("no resync sample");
        assert_eq!(resync.value().map(|v| v.as_watts()), None);
        for _ in 0..4 {
            let sample = next_sample(&mut stream).await.expect("no sample");
            assert_eq!(sample.value().map(|v| v.as_watts()), Some(1.0));
            assert_eq!(open.lock().unwrap().ids(), BTreeSet::from([3, 4]));
        }
    }
}
