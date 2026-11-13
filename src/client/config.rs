// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Connection settings for the microgrid API client.

use std::time::Duration;

use tonic::transport::Endpoint;

use crate::Error;

/// Settings for the connection to the microgrid API.
///
/// The defaults are a connect timeout of 10 seconds, and an HTTP/2 keepalive
/// ping after 60 seconds without data from the service. The connection closes
/// if a ping gets no answer within 20 seconds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MicrogridClientConfig {
    /// How long to wait for the TCP connection to the service.
    pub(crate) connect_timeout: Duration,
    /// When to ping the service, and how long to wait for its answer, or `None`
    /// for no pings.
    pub(crate) keep_alive: Option<KeepAlive>,
}

/// HTTP/2 keepalive settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct KeepAlive {
    /// How long the service may send nothing before the client pings it.
    pub(crate) interval: Duration,
    /// How long to wait for the answer to a ping before closing the connection.
    pub(crate) timeout: Duration,
}

impl Default for MicrogridClientConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            keep_alive: Some(KeepAlive {
                interval: Duration::from_secs(60),
                timeout: Duration::from_secs(20),
            }),
        }
    }
}

impl MicrogridClientConfig {
    /// Sets how long to wait for the TCP connection to the service.
    ///
    /// The time doesn't include the DNS lookup. Several addresses of one IP
    /// version each get an equal share of the time, one after another. The
    /// other IP version starts 300 ms later, or when all addresses of the first
    /// version fail, with its own full time. A call that can't connect in time
    /// fails, and a telemetry stream retries later. It must be more than zero.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Pings the service when it has sent nothing for `ping_interval`, also
    /// while no call is running, and closes the connection if a ping gets no
    /// answer within `ping_timeout`.
    ///
    /// This finds a service that went away without closing the connection, for
    /// example after a network change. Calls and streams on a closed connection
    /// fail, and telemetry streams reconnect. Both durations must be more than
    /// zero.
    pub fn with_keep_alive(mut self, ping_interval: Duration, ping_timeout: Duration) -> Self {
        self.keep_alive = Some(KeepAlive {
            interval: ping_interval,
            timeout: ping_timeout,
        });
        self
    }

    /// Turns keepalive pings off.
    ///
    /// Use this for a service that rejects keepalive pings. For a service that
    /// only rejects frequent pings, use [`Self::with_keep_alive`] with a longer
    /// interval instead.
    ///
    /// Without pings, a service that went away without closing the connection
    /// is not noticed while the client only waits for data, as a telemetry
    /// stream does. A new call on that connection fails only when the operating
    /// system gives up resending it, which can take many minutes.
    pub fn without_keep_alive(mut self) -> Self {
        self.keep_alive = None;
        self
    }

    /// Returns an `ErrorKind::InvalidConfig` error if a duration is zero.
    pub(super) fn validate(&self) -> Result<(), Error> {
        if self.connect_timeout.is_zero() {
            return Err(Error::invalid_config(format!(
                "connect_timeout must be positive, got {:?}",
                self.connect_timeout
            )));
        }
        if let Some(KeepAlive { interval, timeout }) = self.keep_alive
            && (interval.is_zero() || timeout.is_zero())
        {
            return Err(Error::invalid_config(format!(
                "keepalive ping interval and timeout must be positive, \
                 got {interval:?} and {timeout:?}"
            )));
        }
        Ok(())
    }

    /// Returns `endpoint` with these settings.
    pub(super) fn apply_to(&self, endpoint: Endpoint) -> Endpoint {
        let endpoint = endpoint.connect_timeout(self.connect_timeout);
        match self.keep_alive {
            Some(KeepAlive { interval, timeout }) => endpoint
                .http2_keep_alive_interval(interval)
                .keep_alive_timeout(timeout)
                .keep_alive_while_idle(true),
            None => endpoint,
        }
    }
}
