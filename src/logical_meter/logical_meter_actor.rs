// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module contains the logical meter actor, that takes care of resampling
//! component data, evaluating formulas based on that data, and streaming the
//! data to subscribers.

use chrono::{DateTime, Utc};
use frequenz_microgrid_formula_engine::{self as engine, Reading};
use frequenz_resampling::ResamplingFunction;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use tokio::sync::{broadcast, mpsc};

use crate::client::proto::common::metrics::{Metric, metric_value_variant::MetricValueVariant};
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
        engine_formula: engine::Formula<f32>,
        metric: Metric,
        sink: Box<dyn FormulaSink>,
    },
}

/// One distinct expression and everyone listening to it.
struct SubscribedFormula {
    engine_formula: engine::Formula<f32>,
    sinks: Vec<Box<dyn FormulaSink>>,
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

pub(super) struct LogicalMeterActor<C: Clock> {
    instructions_rx: mpsc::Receiver<Instruction>,
    client: MicrogridClientHandle,
    config: LogicalMeterConfig,
    resampler_ts: DateTime<Utc>,
    resampler_timer: WallClockTimer<C>,
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
        let mut formulas: HashMap<(String, Metric), SubscribedFormula> = HashMap::new();

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

                    let resampled = match self.resample_metrics(&mut resamplers) {
                        Ok(resampled) => resampled,
                        Err(err) => {
                            tracing::error!("Error resampling metrics: {}", err);
                            continue;
                        }
                    };
                    self.evaluate_formulas(resampled, &mut formulas);
                    Self::drop_unused_resamplers(&formulas, &mut resamplers);
                }
                instruction = self.instructions_rx.recv() => {
                    match instruction {
                        Some(Instruction::SubscribeFormula{engine_formula, metric, sink}) => {
                            if let Err(err) = self.handle_subscribe_formula(
                                engine_formula,
                                metric,
                                sink,
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

    /// Registers a subscriber for `engine_formula`/`metric`, creating the entry
    /// and its resamplers on first sight.
    ///
    /// If a telemetry subscription cannot be started, no entry is created and
    /// the sink is dropped, which closes the subscriber's stream.
    async fn handle_subscribe_formula(
        &mut self,
        engine_formula: engine::Formula<f32>,
        metric: Metric,
        sink: Box<dyn FormulaSink>,
        formulas: &mut HashMap<(String, Metric), SubscribedFormula>,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
    ) -> Result<(), Error> {
        match formulas.entry((engine_formula.to_string(), metric)) {
            Entry::Occupied(mut entry) => entry.get_mut().sinks.push(sink),
            Entry::Vacant(slot) => {
                let components = engine_formula.components();
                self.start_resamplers(&components, metric, resamplers)
                    .await?;
                slot.insert(SubscribedFormula {
                    engine_formula,
                    sinks: vec![sink],
                });
            }
        }
        Ok(())
    }

    /// Evaluates every formula against `resampled_metrics` and sends the
    /// results. Sinks without receivers are dropped, and entries without sinks
    /// with them.
    fn evaluate_formulas(
        &self,
        mut resampled_metrics: HashMap<Metric, HashMap<u64, Option<f32>>>,
        formulas: &mut HashMap<(String, Metric), SubscribedFormula>,
    ) {
        let timestamp = self.resampler_ts;
        formulas.retain(|(rendered, metric), formula| {
            let values = resampled_metrics.entry(*metric).or_default();
            let value = match formula.engine_formula.evaluate(values) {
                Ok(Reading::Known(value)) => value,
                Ok(Reading::Unknown) => None,
                Err(err) => {
                    tracing::error!(
                        "Failed to evaluate formula {}:({rendered}): {err}",
                        metric.as_str_name()
                    );
                    None
                }
            };
            formula.sinks.retain(|sink| sink.send(timestamp, value));
            if formula.sinks.is_empty() {
                tracing::debug!(
                    "Dropping formula without subscribers: {}:({rendered})",
                    metric.as_str_name()
                );
            }
            !formula.sinks.is_empty()
        });
    }

    /// Drops resamplers no remaining formula references.
    fn drop_unused_resamplers(
        formulas: &HashMap<(String, Metric), SubscribedFormula>,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
    ) {
        let referenced: HashSet<(u64, Metric)> = formulas
            .iter()
            .flat_map(|((_, metric), formula)| {
                formula
                    .engine_formula
                    .components()
                    .into_iter()
                    .map(|component_id| (component_id, *metric))
            })
            .collect();
        resamplers.retain(|key, _| {
            let keep = referenced.contains(key);
            if !keep {
                tracing::debug!(
                    "Dropping resampler for component {}:{}",
                    key.0,
                    key.1.as_str_name()
                );
            }
            keep
        });
    }

    /// Resamples component telemetry
    fn resample_metrics(
        &mut self,
        resamplers: &mut HashMap<(u64, Metric), ComponentDataResampler>,
    ) -> Result<HashMap<Metric, HashMap<u64, Option<f32>>>, Error> {
        let mut resampled_metrics: HashMap<Metric, HashMap<u64, Option<f32>>> = HashMap::new();

        for resampler in resamplers.values_mut() {
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
        quantity::{Frequency, Power, Quantity},
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
}
