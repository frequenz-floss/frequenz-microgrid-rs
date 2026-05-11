// License: MIT
// Copyright © 2025 Frequenz Energy-as-a-Service GmbH

//! This module defines the configuration for the logical meter.

use crate::Sample;
use crate::client::proto::common::metrics::Metric;
use chrono::TimeDelta;
use frequenz_microgrid_component_graph::ComponentGraphConfig;
use frequenz_resampling::ResamplingFunction;
use std::collections::HashMap;

pub struct LogicalMeterConfig {
    /// The resampling interval for the logical meter.
    pub(crate) resampling_interval: TimeDelta,
    /// Resampler function.
    pub(crate) resampling_function: Option<ResamplingFunction<f32, Sample<f32>>>,
    /// Resampler overrides.
    pub(crate) resampling_overrides: HashMap<Metric, ResamplingFunction<f32, Sample<f32>>>,
    /// The maximum age of samples to be considered for resampling, in number of
    /// intervals.
    pub(crate) max_age_in_intervals: u32,
    /// Configuration forwarded to the underlying [`ComponentGraph`][cg]. Defaults
    /// to [`ComponentGraphConfig::default()`].
    ///
    /// [cg]: frequenz_microgrid_component_graph::ComponentGraph
    pub(crate) component_graph_config: ComponentGraphConfig,
}

impl LogicalMeterConfig {
    /// Creates a new `LogicalMeterConfig` with the given resampling interval.
    pub fn new(resampling_interval: TimeDelta) -> Self {
        Self {
            resampling_interval,
            resampling_function: None,
            resampling_overrides: HashMap::new(),
            max_age_in_intervals: 3,
            component_graph_config: ComponentGraphConfig::default(),
        }
    }

    /// Sets the default resampling function.
    ///
    /// This function will be used for all metrics that do not have a specific
    /// override set.
    ///
    /// If no default resampling function is set, the logical meter will default
    /// to using the `Average` resampling function.
    pub fn with_default_resampling_function(
        mut self,
        function: ResamplingFunction<f32, Sample<f32>>,
    ) -> Self {
        self.resampling_function = Some(function);
        self
    }

    /// Sets a resampling function override for a specific metric.
    ///
    /// If this function is called multiple times for the same metric, the last
    /// function provided will be used.
    pub fn override_resampling_function<M: crate::metric::Metric>(
        mut self,
        function: ResamplingFunction<f32, Sample<f32>>,
    ) -> Self {
        self.resampling_overrides.insert(M::METRIC, function);

        self
    }

    /// Sets the maximum age of samples to be considered for resampling, in
    /// number of intervals.
    ///
    /// Must be at least 1.  If a smaller value is provided, it will be clamped
    /// to 1.
    ///
    /// If not set, the default value is 3.
    pub fn with_max_age_in_intervals(mut self, max_age_in_intervals: u32) -> Self {
        // Ensure that the maximum age is at least 1 interval.
        self.max_age_in_intervals = max_age_in_intervals.max(1);
        self
    }

    /// Sets the [`ComponentGraphConfig`] forwarded to the underlying graph
    /// when [`LogicalMeterHandle::try_new`][lm] (and therefore
    /// [`Microgrid::try_new`][mg]) builds it. If not set, the graph crate's
    /// `Default::default()` is used.
    ///
    /// [lm]: crate::LogicalMeterHandle::try_new
    /// [mg]: crate::Microgrid::try_new
    pub fn with_component_graph_config(mut self, config: ComponentGraphConfig) -> Self {
        self.component_graph_config = config;
        self
    }
}
