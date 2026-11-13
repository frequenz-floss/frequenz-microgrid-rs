// License: MIT
// Copyright © 2026 Frequenz Energy-as-a-Service GmbH

//! Conversions between protobuf times and `chrono` types.

use chrono::{DateTime, TimeDelta, Utc};

use super::pb::google::protobuf::Timestamp;

/// Converts a protobuf timestamp to a `DateTime`, or returns `None` if it is
/// out of range.
pub(crate) fn timestamp_to_datetime(timestamp: Timestamp) -> Option<DateTime<Utc>> {
    let nanos = u32::try_from(timestamp.nanos).ok()?;
    DateTime::from_timestamp(timestamp.seconds, nanos)
}

/// Converts a `DateTime` to a protobuf timestamp.
#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn datetime_to_timestamp(datetime: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: datetime.timestamp(),
        nanos: datetime.timestamp_subsec_nanos() as i32,
    }
}

/// Converts a request lifetime to the whole seconds the API expects, rounding
/// down.
///
/// A lifetime of -1 second or less is left out, so the service default applies.
pub(crate) fn request_lifetime_secs(request_lifetime: Option<TimeDelta>) -> Option<u64> {
    request_lifetime.and_then(|d| u64::try_from(d.num_seconds()).ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_round_trip() {
        let datetime = DateTime::from_timestamp(1_700_000_000, 250_000_000).unwrap();
        assert_eq!(
            timestamp_to_datetime(datetime_to_timestamp(datetime)),
            Some(datetime)
        );
    }

    #[test]
    fn test_timestamp_out_of_range() {
        let timestamp = Timestamp {
            seconds: i64::MAX,
            nanos: 0,
        };
        assert_eq!(timestamp_to_datetime(timestamp), None);
    }

    #[test]
    fn test_request_lifetime_secs() {
        assert_eq!(request_lifetime_secs(None), None);
        assert_eq!(
            request_lifetime_secs(Some(TimeDelta::milliseconds(30_900))),
            Some(30)
        );
        assert_eq!(request_lifetime_secs(Some(TimeDelta::seconds(-90))), None);
    }
}
