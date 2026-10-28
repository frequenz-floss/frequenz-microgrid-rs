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

/// An attached telemetry stream and the resampler it feeds.
struct ResampledStream {
    receiver: broadcast::Receiver<ElectricalComponentTelemetry>,
    resampler: frequenz_resampling::Resampler<f32, Sample<f32>>,
    /// Not resampled since it attached or since a wall-clock jump restarted it.
    /// See [`LogicalMeterActor::resample`].
    fresh: bool,
    /// Consecutive ticks on which no formula read this key. The first tick
    /// after the wall clock jumps by more than one resampling interval neither
    /// adds to the count nor resets it.
    unread_ticks: u32,
}

/// A subscribe request in flight, yielding the key it was sent for, the actor's
/// resync count when it was sent, and its result.
type SubscribeRequest = BoxFuture<
    'static,
    (
        Key,
        u64,
        Result<broadcast::Receiver<ElectricalComponentTelemetry>, Error>,
    ),
>;

/// Reads a tick's resampled values and records every key read. A key that is
/// not in the snapshot is unknown: a key with no subscription, or one that
/// [`LogicalMeterActor::resample`] left out of it.
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
    /// Counts the resync ticks so far: the ticks on which the timer detected a
    /// wall-clock jump. See [`Self::attach_subscription`].
    resyncs: u64,
    config: LogicalMeterConfig,
    resampler_ts: DateTime<Utc>,
    resampler_timer: WallClockTimer<C>,
    /// The snapshot the formulas were last evaluated against. A new
    /// subscriber's expression is evaluated against it to find the components
    /// it reads. A tick whose resampling fails leaves the previous snapshot in
    /// place.
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
            resyncs: 0,
            config,
            resampler_ts,
            resampler_timer: timer,
            last_snapshot: HashMap::new(),
        })
    }

    pub async fn run(mut self) {
        // `None` marks a pending subscription: its subscribe request is in
        // flight or has failed.
        let mut subscriptions: HashMap<Key, Option<ResampledStream>> = HashMap::new();
        let mut formulas: HashMap<String, SubscribedFormula> = HashMap::new();
        let mut subscribe_requests: FuturesUnordered<SubscribeRequest> = FuturesUnordered::new();

        loop {
            tokio::select! {
                tick_info = self.resampler_timer.tick() => {
                    if tick_info.resynced {
                        // Wall clock jumped. The inner resamplers' `start`
                        // fields are on the old clock frame. Resampling across
                        // the gap can give several outputs, or none after a
                        // backward jump, but this actor's `resample` needs
                        // exactly one per tick.
                        //
                        // So drop all buffered telemetry and restart every
                        // attached stream as fresh. Its new resampler starts
                        // one interval before the realigned current tick, so
                        // the resample below emits a single sample at that
                        // tick, and the every-interval cadence holds across the
                        // jump.
                        let realigned_current =
                            self.resampler_timer.next_tick_time()
                                - self.config.resampling_interval;
                        self.restart_streams_after_jump(
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
                    let read_keys = self.evaluate_formulas(&snapshot, &mut formulas);
                    self.last_snapshot = snapshot;
                    // On the resync tick, the restarted streams normally read
                    // as unknown, so no COALESCE reaches its fallbacks. Leave
                    // the subscriptions as they are, so none is counted as
                    // unread.
                    if !tick_info.resynced {
                        self.reconcile_subscriptions(
                            &read_keys,
                            &mut subscriptions,
                            &mut subscribe_requests,
                        );
                    }
                }
                Some((key, resyncs_when_sent, result)) = subscribe_requests.next(),
                    if !subscribe_requests.is_empty() => {
                    self.attach_subscription(key, resyncs_when_sent, result, &mut subscriptions);
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

    /// Builds an inner resampler for `metric` aligned to `start`.
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

    /// Attaches `receiver` to a new resampler for `metric` aligned to `start`.
    fn build_stream(
        &self,
        metric: Metric,
        receiver: broadcast::Receiver<ElectricalComponentTelemetry>,
        start: DateTime<Utc>,
    ) -> ResampledStream {
        ResampledStream {
            receiver,
            resampler: self.build_resampler(metric, start),
            fresh: true,
            unread_ticks: 0,
        }
    }

    /// Resamples every attached subscription and returns this tick's values by
    /// key.
    ///
    /// A subscription with no stream has no resampler and is left out of the
    /// snapshot, so its key reads as [`Reading::Unknown`].
    ///
    /// A fresh stream stops being fresh. If no message is waiting, or every
    /// waiting message gives it a sample later than this tick, it is left out
    /// of this one snapshot, so its key reads as unknown.
    fn resample(
        &self,
        subscriptions: &mut HashMap<Key, Option<ResampledStream>>,
    ) -> Result<HashMap<Key, Option<f32>>, Error> {
        let mut snapshot = HashMap::with_capacity(subscriptions.len());
        for (key, stream) in subscriptions.iter_mut() {
            let Some(stream) = stream else {
                continue;
            };
            let mut settled = false;
            while let Some(data) = poll_telemetry(&mut stream.receiver, key.component_id) {
                let sample_time = Self::push_to_resampler(&mut stream.resampler, *key, data);
                // A message settles the key, unless its sample is past this
                // tick: that sample stays in the resampler for a later tick.
                settled |= sample_time.is_none_or(|time| time <= self.resampler_ts);
            }
            let resampled = stream.resampler.resample(self.resampler_ts);
            if resampled.len() != 1 {
                return Err(Error::connection_failure(format!(
                    "Resampling produced {} values",
                    resampled.len()
                )));
            }
            if settled || !stream.fresh {
                snapshot.insert(*key, resampled[0].clone().value());
            }
            stream.fresh = false;
        }
        Ok(snapshot)
    }

    /// Evaluates every formula against `snapshot` and sends the results. Sinks
    /// without receivers are dropped, and formulas without sinks with them.
    /// Returns the union of the keys the surviving formulas read.
    fn evaluate_formulas(
        &self,
        snapshot: &HashMap<Key, Option<f32>>,
        formulas: &mut HashMap<String, SubscribedFormula>,
    ) -> HashSet<Key> {
        let timestamp = self.resampler_ts;
        let mut read_keys = HashSet::new();
        let mut formula_reads = HashSet::new();
        formulas.retain(|rendered, formula| {
            formula_reads.clear();
            let value = match formula.engine_formula.evaluate(&mut RecordingSource {
                snapshot,
                reads: &mut formula_reads,
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
            read_keys.extend(formula_reads.drain());
            true
        });
        read_keys
    }

    /// Starts subscriptions for keys that were read but have no subscription,
    /// and drops attached subscriptions that have gone unread for
    /// `unsubscribe_after_unread_ticks` ticks.
    ///
    /// A pending subscription is never dropped, so at most one subscribe
    /// request is in flight per key.
    fn reconcile_subscriptions(
        &self,
        read_keys: &HashSet<Key>,
        subscriptions: &mut HashMap<Key, Option<ResampledStream>>,
        subscribe_requests: &mut FuturesUnordered<SubscribeRequest>,
    ) {
        self.start_missing_subscriptions(read_keys, subscriptions, subscribe_requests);
        let limit = self.config.unsubscribe_after_unread_ticks;
        subscriptions.retain(|key, stream| {
            let Some(stream) = stream else {
                return true;
            };
            if read_keys.contains(key) {
                stream.unread_ticks = 0;
                return true;
            }
            stream.unread_ticks += 1;
            if stream.unread_ticks >= limit {
                tracing::debug!("Dropping subscription of unread component {key}");
                return false;
            }
            true
        });
    }

    /// Registers a subscription for every key in `keys` that has none and
    /// starts its telemetry subscription without blocking the actor.
    fn start_missing_subscriptions(
        &self,
        keys: &HashSet<Key>,
        subscriptions: &mut HashMap<Key, Option<ResampledStream>>,
        subscribe_requests: &mut FuturesUnordered<SubscribeRequest>,
    ) {
        for &key in keys {
            let Entry::Vacant(entry) = subscriptions.entry(key) else {
                continue;
            };
            entry.insert(None);
            tracing::debug!("Subscribing to {key}");
            let client = self.client.clone();
            let resyncs_when_sent = self.resyncs;
            subscribe_requests.push(Box::pin(async move {
                let result = client
                    .receive_electrical_component_telemetry_stream(key.component_id)
                    .await;
                (key, resyncs_when_sent, result)
            }));
        }
    }

    /// Attaches the result of a subscribe request to the key's subscription,
    /// draining the receiver first if the actor resynced after the request was
    /// sent.
    ///
    /// `resyncs_when_sent` is the actor's resync count when the request was
    /// sent. If it differs from the current count, the receiver can hold
    /// telemetry from before a wall-clock jump, which the resync did not drain.
    /// So all its queued telemetry is drained, including samples that arrived
    /// after the jump and after the resync tick.
    ///
    /// The request fails only once the client actor has stopped, so it could
    /// never succeed later. The error is logged once and the subscription stays
    /// pending. Its entry is kept, so no new request is sent for the key.
    fn attach_subscription(
        &self,
        key: Key,
        resyncs_when_sent: u64,
        result: Result<broadcast::Receiver<ElectricalComponentTelemetry>, Error>,
        subscriptions: &mut HashMap<Key, Option<ResampledStream>>,
    ) {
        let Some(subscription) = subscriptions.get_mut(&key) else {
            tracing::debug!("Subscription for {key} completed, but it is gone");
            return;
        };
        match result {
            Ok(mut receiver) => {
                tracing::debug!("Subscribed to {key}");
                if resyncs_when_sent != self.resyncs {
                    tracing::debug!("Draining {key}: the actor resynced while subscribing");
                    while poll_telemetry(&mut receiver, key.component_id).is_some() {}
                }
                *subscription = Some(self.build_stream(key.metric, receiver, self.resampler_ts));
            }
            Err(err) => tracing::error!("Subscribing to {key} failed, not retrying: {err}"),
        }
    }

    /// Registers a subscriber for `engine_formula`, creating the formula on
    /// first sight of the expression and pushing the sink onto it otherwise.
    ///
    /// For every new subscriber, the expression is evaluated against the last
    /// snapshot only to find the components it reads; any not yet subscribed
    /// are subscribed at once. Nothing is sent to the sinks.
    fn handle_subscribe_formula(
        &self,
        engine_formula: engine::Formula<f32, Key>,
        sink: Box<dyn FormulaSink>,
        formulas: &mut HashMap<String, SubscribedFormula>,
        subscriptions: &mut HashMap<Key, Option<ResampledStream>>,
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
                "Evaluating formula {} against the last snapshot failed: {err}",
                formula.engine_formula
            );
        }
        self.start_missing_subscriptions(&reads, subscriptions, subscribe_requests);
    }

    /// Restarts every attached stream as fresh, with a new resampler whose
    /// `start` is the given boundary. The stream keeps its telemetry broadcast
    /// receiver and its unread count. All buffered telemetry is drained and
    /// discarded, including messages that arrived after the jump.
    ///
    /// Draining also clears `Lagged` errors from the broadcast receiver, which
    /// happen when more samples arrive during the jump than the channel holds.
    ///
    /// The jump is also counted in `resyncs`, so a subscription with no stream
    /// yet has its receiver drained when it attaches. Its resampler is then
    /// aligned to the new frame.
    fn restart_streams_after_jump(
        &mut self,
        subscriptions: &mut HashMap<Key, Option<ResampledStream>>,
        start: DateTime<Utc>,
    ) {
        self.resyncs += 1;
        for (key, stream) in subscriptions.iter_mut() {
            let Some(stream) = stream else {
                continue;
            };
            // Drain every queued sample, so the new resampler starts empty:
            // samples from before the jump may not fit its window, for example
            // when the server's clock jumped too.
            while poll_telemetry(&mut stream.receiver, key.component_id).is_some() {}
            stream.resampler = self.build_resampler(key.metric, start);
            stream.fresh = true;
        }
    }

    /// Extracts the resampler's metric from the given telemetry and pushes it
    /// to the resampler's internal buffer. Returns the pushed sample's
    /// timestamp, or `None` if the telemetry has no sample of the metric, or
    /// its first one has no valid timestamp or no value.
    fn push_to_resampler(
        resampler: &mut frequenz_resampling::Resampler<f32, Sample<f32>>,
        key: Key,
        data: ElectricalComponentTelemetry,
    ) -> Option<DateTime<Utc>> {
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
            return None;
        };
        let timestamp = dd.sample_time.and_then(|timestamp| {
            DateTime::from_timestamp(timestamp.seconds, timestamp.nanos as u32)
        })?;
        let variant = dd
            .value
            .as_ref()
            .and_then(|value| value.metric_value_variant.as_ref())?;
        let value = Some(match variant {
            MetricValueVariant::SimpleMetric(value) => value.value,
            MetricValueVariant::AggregatedMetric(value) => value.avg_value,
        });

        let sample = Sample::new(timestamp, value);

        resampler.push(sample);
        Some(timestamp)
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

    /// A subscription attached to `receiver`, whose stream starts at `start`
    /// and has been resampled before.
    fn attached_subscription(
        actor: &LogicalMeterActor<TokioSyncedClock>,
        key: Key,
        receiver: broadcast::Receiver<ElectricalComponentTelemetry>,
        start: DateTime<Utc>,
    ) -> Option<ResampledStream> {
        let mut stream = actor.build_stream(key.metric, receiver, start);
        stream.fresh = false;
        Some(stream)
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

        let read_keys = actor.evaluate_formulas(&snapshot, &mut formulas);

        assert_eq!(recorded.lock().unwrap()[0].1, Some(231.0));
        assert_eq!(read_keys, HashSet::from([active_power_key(2), voltage]));
    }

    #[tokio::test]
    async fn test_evaluate_formulas_drops_formulas_that_lost_their_last_sink() {
        let actor = bare_actor();
        let mut formulas = HashMap::from([(
            "#2".to_string(),
            subscribed_formula("#2", vec![Box::new(DeadSink)]),
        )]);

        let read_keys = actor.evaluate_formulas(
            &HashMap::from([(active_power_key(2), Some(1.0))]),
            &mut formulas,
        );

        assert!(
            formulas.is_empty(),
            "the formula should go with its last sink"
        );
        assert!(
            read_keys.is_empty(),
            "a dropped formula's reads must not be returned"
        );
    }

    #[tokio::test]
    async fn test_reconcile_subscriptions_counts_each_metric_separately() {
        let actor = bare_actor_with(
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap())
                .with_unsubscribe_after_unread_ticks(2),
        );
        let read_key = active_power_key(2);
        let unread_key = Key {
            metric: Metric::AcVoltage,
            component_id: 2,
        };
        let mut subscriptions = HashMap::new();
        for key in [read_key, unread_key] {
            let (_tx, receiver) = broadcast::channel(1);
            subscriptions.insert(
                key,
                attached_subscription(&actor, key, receiver, actor.resampler_ts),
            );
        }
        let read_keys = HashSet::from([read_key]);
        let mut subscribe_requests = FuturesUnordered::new();

        actor.reconcile_subscriptions(&read_keys, &mut subscriptions, &mut subscribe_requests);
        assert!(subscriptions.contains_key(&unread_key));

        actor.reconcile_subscriptions(&read_keys, &mut subscriptions, &mut subscribe_requests);
        assert!(subscriptions.contains_key(&read_key));
        assert!(
            !subscriptions.contains_key(&unread_key),
            "a read of another metric of the component must not reset this count"
        );
    }

    #[tokio::test]
    async fn test_reconcile_subscriptions_ages_out_unread_keys() {
        let limit = 2;
        let actor = bare_actor_with(
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap())
                .with_unsubscribe_after_unread_ticks(limit),
        );
        let mut subscriptions = HashMap::new();
        for component_id in [2, 3] {
            let key = active_power_key(component_id);
            let (_tx, receiver) = broadcast::channel(1);
            subscriptions.insert(
                key,
                attached_subscription(&actor, key, receiver, actor.resampler_ts),
            );
        }
        // Component 4's subscribe request is still in flight.
        let pending_key = active_power_key(4);
        subscriptions.insert(pending_key, None);
        let read_keys = HashSet::from([active_power_key(2)]);
        let mut subscribe_requests = FuturesUnordered::new();

        // An unread key survives until it has gone unread for `limit`
        // consecutive ticks.
        for tick in 1..limit {
            actor.reconcile_subscriptions(&read_keys, &mut subscriptions, &mut subscribe_requests);
            assert_eq!(subscriptions.len(), 3, "dropped too early on tick {tick}");
        }
        actor.reconcile_subscriptions(&read_keys, &mut subscriptions, &mut subscribe_requests);

        assert_eq!(subscriptions.len(), 2);
        assert!(subscriptions.contains_key(&active_power_key(2)));
        assert_eq!(
            subscriptions[&active_power_key(2)]
                .as_ref()
                .map(|s| s.unread_ticks),
            Some(0)
        );

        // A pending subscription is never dropped.
        for _ in 0..limit {
            actor.reconcile_subscriptions(&read_keys, &mut subscriptions, &mut subscribe_requests);
        }
        assert!(subscriptions[&pending_key].is_none());
        assert!(
            subscribe_requests.is_empty(),
            "no subscription should start for an already-subscribed key"
        );
    }

    #[tokio::test]
    async fn test_pending_subscription_reads_as_unknown() {
        let actor = bare_actor();
        let start = actor.resampler_ts - actor.config.resampling_interval;
        let pending_key = active_power_key(2);
        let attached_key = active_power_key(3);
        let (_tx, receiver) = broadcast::channel(1);
        let mut subscriptions = HashMap::from([
            // The pending key's subscription is still in flight.
            (pending_key, None),
            (
                attached_key,
                attached_subscription(&actor, attached_key, receiver, start),
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

        let read_keys = actor.evaluate_formulas(&snapshot, &mut formulas);

        assert_eq!(
            *recorded.lock().unwrap(),
            vec![(actor.resampler_ts, None)],
            "an unknown primary makes the whole coalesce unknown, sent as None",
        );
        assert_eq!(
            read_keys,
            HashSet::from([pending_key]),
            "COALESCE stops at the unknown key and reads nothing beyond it",
        );
    }

    #[tokio::test]
    async fn test_new_subscription_without_a_message_reads_unknown_on_its_first_tick() {
        let mut actor = bare_actor();
        let start = actor.resampler_ts;
        let primary = active_power_key(4);
        let fallback = active_power_key(3);
        let (_primary_tx, primary_receiver) = broadcast::channel(1);
        let mut subscriptions = HashMap::from([
            // The primary is attached but silent, so it reads as missing.
            (
                primary,
                attached_subscription(&actor, primary, primary_receiver, start),
            ),
            (fallback, None),
        ]);
        let (_fallback_tx, fallback_receiver) = broadcast::channel(1);
        actor.attach_subscription(
            fallback,
            actor.resyncs,
            Ok(fallback_receiver),
            &mut subscriptions,
        );
        actor.resampler_ts += actor.config.resampling_interval;

        let (sink, recorded) = recording_sink();
        let mut formulas = HashMap::from([(
            "COALESCE(#4, #3, 0)".to_string(),
            subscribed_formula("COALESCE(#4, #3, 0)", vec![sink]),
        )]);

        let snapshot = actor.resample(&mut subscriptions).unwrap();
        actor.evaluate_formulas(&snapshot, &mut formulas);
        assert_eq!(
            *recorded.lock().unwrap(),
            vec![(actor.resampler_ts, None)],
            "the coalesce must wait for the new fallback, not fall through to 0",
        );
    }

    #[tokio::test]
    async fn test_new_subscription_is_settled_by_a_message_without_the_metric() {
        let mut actor = bare_actor();
        let key = active_power_key(3);
        let mut subscriptions = HashMap::from([(key, None)]);
        let (tx, receiver) = broadcast::channel(1);
        actor.attach_subscription(key, actor.resyncs, Ok(receiver), &mut subscriptions);
        actor.resampler_ts += actor.config.resampling_interval;

        // A message without the metric settles the key on the same tick.
        tx.send(ElectricalComponentTelemetry::default()).unwrap();
        let snapshot = actor.resample(&mut subscriptions).unwrap();
        assert_eq!(snapshot.get(&key), Some(&None));
    }

    /// A message from `key`'s component with one sample of `key`'s metric.
    fn telemetry(key: Key, timestamp: DateTime<Utc>, value: f32) -> ElectricalComponentTelemetry {
        use crate::client::proto::{
            common::metrics::{self, MetricSample, SimpleMetricValue},
            google::protobuf,
        };
        ElectricalComponentTelemetry {
            electrical_component_id: key.component_id,
            metric_samples: vec![MetricSample {
                sample_time: Some(protobuf::Timestamp {
                    seconds: timestamp.timestamp(),
                    nanos: timestamp.timestamp_subsec_nanos() as i32,
                }),
                metric: key.metric as i32,
                value: Some(metrics::MetricValueVariant {
                    metric_value_variant: Some(MetricValueVariant::SimpleMetric(
                        SimpleMetricValue { value },
                    )),
                }),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_new_subscription_is_not_settled_by_a_sample_past_its_tick() {
        let mut actor = bare_actor();
        let interval = actor.config.resampling_interval;
        let key = active_power_key(3);
        let mut subscriptions = HashMap::from([(key, None)]);
        let (tx, receiver) = broadcast::channel(1);
        actor.attach_subscription(key, actor.resyncs, Ok(receiver), &mut subscriptions);
        actor.resampler_ts += interval;

        let just_after = actor.resampler_ts + TimeDelta::milliseconds(1);
        tx.send(telemetry(key, just_after, 7.0)).unwrap();
        let snapshot = actor.resample(&mut subscriptions).unwrap();
        assert!(
            !snapshot.contains_key(&key),
            "a sample past this tick must leave the key unknown",
        );

        actor.resampler_ts += interval;
        let snapshot = actor.resample(&mut subscriptions).unwrap();
        assert_eq!(snapshot.get(&key), Some(&Some(7.0)));
    }

    #[tokio::test]
    async fn test_jump_keeps_the_unread_count() {
        let mut actor = bare_actor();
        let key = active_power_key(2);
        let (_tx, receiver) = broadcast::channel(1);
        let mut stream = attached_subscription(&actor, key, receiver, actor.resampler_ts).unwrap();
        stream.unread_ticks = 2;
        let mut subscriptions = HashMap::from([(key, Some(stream))]);

        actor.restart_streams_after_jump(&mut subscriptions, actor.resampler_ts);

        assert_eq!(
            subscriptions[&key].as_ref().map(|s| s.unread_ticks),
            Some(2)
        );
    }

    #[tokio::test]
    async fn test_jump_restarts_attached_streams_as_fresh() {
        let mut actor = bare_actor();
        let interval = actor.config.resampling_interval;
        let attached = active_power_key(2);
        let pending = active_power_key(3);
        let (tx, receiver) = broadcast::channel(1);
        let mut subscriptions = HashMap::from([
            (
                attached,
                attached_subscription(&actor, attached, receiver, actor.resampler_ts - interval),
            ),
            (pending, None),
        ]);
        // Queued during the jump, on the old wall-clock frame.
        tx.send(ElectricalComponentTelemetry::default()).unwrap();

        actor.restart_streams_after_jump(&mut subscriptions, actor.resampler_ts);
        assert_eq!(
            subscriptions[&attached].as_ref().map(|s| s.fresh),
            Some(true)
        );
        assert!(subscriptions[&pending].is_none());

        actor.resampler_ts += interval;
        let snapshot = actor.resample(&mut subscriptions).unwrap();
        assert!(
            snapshot.is_empty(),
            "a message drained after a jump must not count as the first one",
        );
    }

    #[tokio::test]
    async fn test_silent_new_subscription_reads_missing_after_one_tick() {
        let mut actor = bare_actor();
        let interval = actor.config.resampling_interval;
        let key = active_power_key(3);
        let mut subscriptions = HashMap::from([(key, None)]);
        let (_tx, receiver) = broadcast::channel(1);
        actor.attach_subscription(key, actor.resyncs, Ok(receiver), &mut subscriptions);
        assert_eq!(subscriptions[&key].as_ref().map(|s| s.fresh), Some(true));

        actor.resampler_ts += interval;
        let snapshot = actor.resample(&mut subscriptions).unwrap();
        assert!(!snapshot.contains_key(&key));
        assert_eq!(subscriptions[&key].as_ref().map(|s| s.fresh), Some(false));
        actor.resampler_ts += interval;
        let snapshot = actor.resample(&mut subscriptions).unwrap();
        assert_eq!(
            snapshot.get(&key),
            Some(&None),
            "a stream that never sends must end up missing",
        );
    }

    #[tokio::test]
    async fn test_reconcile_subscriptions_starts_missing_keys() {
        let actor = bare_actor();
        let mut subscriptions = HashMap::new();
        let mut subscribe_requests = FuturesUnordered::new();
        let read_keys = HashSet::from([active_power_key(2)]);

        actor.reconcile_subscriptions(&read_keys, &mut subscriptions, &mut subscribe_requests);

        assert_eq!(subscriptions.len(), 1);
        let subscription = &subscriptions[&active_power_key(2)];
        assert!(
            subscription.is_none(),
            "the receiver attaches only once the subscription completes"
        );
        assert_eq!(subscribe_requests.len(), 1);
    }

    #[tokio::test]
    async fn test_reconcile_subscriptions_resets_the_unread_count_on_a_read() {
        let limit = 3;
        let actor = bare_actor_with(
            LogicalMeterConfig::new(TimeDelta::try_seconds(1).unwrap())
                .with_unsubscribe_after_unread_ticks(limit),
        );
        let key = active_power_key(2);
        let (_tx, receiver) = broadcast::channel(1);
        let mut subscriptions = HashMap::from([(
            key,
            attached_subscription(&actor, key, receiver, actor.resampler_ts),
        )]);
        let mut subscribe_requests = FuturesUnordered::new();
        let no_reads = HashSet::new();

        for _ in 1..limit {
            actor.reconcile_subscriptions(&no_reads, &mut subscriptions, &mut subscribe_requests);
        }
        // One read starts the count again.
        actor.reconcile_subscriptions(
            &HashSet::from([key]),
            &mut subscriptions,
            &mut subscribe_requests,
        );
        for tick in 1..limit {
            actor.reconcile_subscriptions(&no_reads, &mut subscriptions, &mut subscribe_requests);
            assert!(
                subscriptions.contains_key(&key),
                "dropped on unread tick {tick} after the read"
            );
        }
        actor.reconcile_subscriptions(&no_reads, &mut subscriptions, &mut subscribe_requests);
        assert!(
            !subscriptions.contains_key(&key),
            "not dropped after {limit} unread ticks in a row"
        );
    }

    #[tokio::test]
    async fn test_handle_subscribe_formula_subscribes_from_the_last_snapshot() {
        let mut actor = bare_actor();
        let primary = active_power_key(2);
        let fallback = active_power_key(3);
        let (_tx, receiver) = broadcast::channel(1);
        let mut subscriptions = HashMap::from([(
            primary,
            attached_subscription(&actor, primary, receiver, actor.resampler_ts),
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
            subscriptions.get(&fallback).is_some_and(|s| s.is_none()),
            "the fallback must be subscribed at once, not on the next tick"
        );
        assert_eq!(subscribe_requests.len(), 1);
    }

    #[tokio::test]
    async fn test_failed_subscription_stays_pending_and_is_not_resent() {
        let actor = bare_actor();
        let key = active_power_key(2);
        let mut subscriptions = HashMap::from([(key, None)]);

        // The request fails only once the client actor has stopped.
        actor.attach_subscription(
            key,
            actor.resyncs,
            Err(Error::connection_failure("client stopped")),
            &mut subscriptions,
        );
        assert!(subscriptions[&key].is_none());

        let mut subscribe_requests = FuturesUnordered::new();
        actor.reconcile_subscriptions(
            &HashSet::from([key]),
            &mut subscriptions,
            &mut subscribe_requests,
        );
        assert!(subscriptions[&key].is_none());
        assert!(
            subscribe_requests.is_empty(),
            "no new request is sent for a failed subscription"
        );
    }

    #[tokio::test]
    async fn test_subscription_that_crosses_a_jump_drops_telemetry_from_before_it() {
        let mut actor = bare_actor();
        let crossed = active_power_key(3);
        let after = active_power_key(4);
        let queued_receiver = || {
            let (tx, receiver) = broadcast::channel(10);
            tx.send(ElectricalComponentTelemetry::default()).unwrap();
            receiver
        };

        // Requested before the jump, with telemetry already queued.
        let mut subscriptions = HashMap::from([(crossed, None)]);
        let resyncs_when_sent = actor.resyncs;
        let crossed_receiver = queued_receiver();
        actor.restart_streams_after_jump(&mut subscriptions, actor.resampler_ts);
        actor.attach_subscription(
            crossed,
            resyncs_when_sent,
            Ok(crossed_receiver),
            &mut subscriptions,
        );

        // Requested after the jump.
        subscriptions.insert(after, None);
        actor.attach_subscription(
            after,
            actor.resyncs,
            Ok(queued_receiver()),
            &mut subscriptions,
        );

        let queued = |key| subscriptions[&key].as_ref().unwrap().receiver.len();
        assert_eq!(queued(crossed), 0);
        assert_eq!(queued(after), 1);
    }

    #[tokio::test]
    async fn test_subscribe_request_carries_the_resync_count_from_when_it_was_sent() {
        let mut actor = bare_actor();
        let key = active_power_key(2);
        let mut subscriptions = HashMap::new();
        let mut subscribe_requests = FuturesUnordered::new();

        actor.restart_streams_after_jump(&mut subscriptions, actor.resampler_ts);
        let resyncs_when_sent = actor.resyncs;
        actor.start_missing_subscriptions(
            &HashSet::from([key]),
            &mut subscriptions,
            &mut subscribe_requests,
        );
        actor.restart_streams_after_jump(&mut subscriptions, actor.resampler_ts);

        let (_, resyncs_in_request, _) = subscribe_requests.next().await.unwrap();
        assert_eq!(resyncs_in_request, resyncs_when_sent);
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

    // Pins the upstream contract that `restart_streams_after_jump` relies on:
    // after rebuilding with `start = current - interval`, a `resample(current)`
    // call on an empty buffer must yield exactly one output, with `value() ==
    // None`. If `frequenz_resampling` ever returns zero outputs for an empty
    // window, the jump-recovery path flips from a graceful `None` sample to a
    // runtime `ConnectionFailure("Resampling produced N values")`, so this
    // assumption deserves a focused regression test rather than only implicit
    // coverage from the end-to-end NTP-jump tests.
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
    /// fallback. Meter 3 sends power; `configure_inverter` sets up inverter 4.
    fn pv_chain(configure_inverter: impl FnOnce(MockComponent) -> MockComponent) -> MockComponent {
        let inverter = configure_inverter(MockComponent::pv_inverter(4));
        let meter = MockComponent::meter(3).with_power(vec![1.0; 400]);
        MockComponent::grid(1).with_children(vec![
            MockComponent::meter(2).with_children(vec![meter.with_children(vec![inverter])]),
        ])
    }

    async fn pv_chain_handle(
        configure_inverter: impl FnOnce(MockComponent) -> MockComponent,
        config: LogicalMeterConfig,
    ) -> (LogicalMeterHandle, Arc<Mutex<OpenStreams>>) {
        handle_for(pv_chain(configure_inverter), config, aligned_clock()).await
    }

    #[tokio::test(start_paused = true)]
    async fn test_only_the_primary_is_subscribed_while_it_delivers() {
        let interval = TimeDelta::try_seconds(1).unwrap();
        let (lm, open) = pv_chain_handle(
            |inverter| inverter.with_power(vec![10.0; 400]),
            LogicalMeterConfig::new(interval),
        )
        .await;
        let formula = lm.pv::<crate::metric::AcPowerActive>(None).unwrap();
        let mut stream = BroadcastStream::new(formula.subscribe().await.unwrap());

        // Checked on every tick: a wrongly subscribed fallback would not stay
        // subscribed for long.
        for _ in 0..4 {
            let sample = next_sample(&mut stream).await.expect("no sample");
            assert_eq!(sample.value().map(|v| v.as_watts()), Some(10.0));
            assert_eq!(open.lock().unwrap().ids(), BTreeSet::from([4]));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn test_fallback_is_subscribed_when_the_primary_goes_silent() {
        let interval = TimeDelta::try_seconds(1).unwrap();
        let (lm, open) = pv_chain_handle(
            |inverter| {
                inverter
                    .with_power(vec![10.0; 5])
                    .with_silence_after_metrics()
            },
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
            |inverter| inverter.with_power(vec![10.0; 400]),
            LogicalMeterConfig::new(interval).with_unsubscribe_after_unread_ticks(2),
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
            pv_chain(|inverter| inverter.with_reactive_power(vec![5.0; 400])),
            LogicalMeterConfig::new(interval).with_unsubscribe_after_unread_ticks(1),
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
