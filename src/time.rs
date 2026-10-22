//! Wall clock and the `ts` format (UTC RFC 3339, 9 fractional digits, `Z`), without a date
//! crate (ADR 0002). `MASU_TEST_NOW` shifts the clock for the seal tests (ADR 0009).

use std::sync::OnceLock;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const NS: i64 = 1_000_000_000;
pub const DAY_SECS: i64 = 86_400;

/// Offset added to the real clock: `MASU_TEST_NOW=<ts>` (test only) sets "now" to that instant
/// at start-up, and the clock keeps running from there.
fn offset() -> i64 {
    static OFFSET: OnceLock<i64> = OnceLock::new();
    *OFFSET.get_or_init(|| match std::env::var("MASU_TEST_NOW") {
        Ok(ts) => parse_ns(&ts).expect("MASU_TEST_NOW: not a ts") - real_now_ns(),
        Err(_) => 0,
    })
}

fn real_now_ns() -> i64 {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::ZERO);
    d.as_nanos() as i64
}

/// Unix nanoseconds now (shifted under `MASU_TEST_NOW`).
pub fn now_ns() -> i64 {
    real_now_ns() + offset()
}

/// `2026-10-19T08:14:00.000213904Z` for unix nanoseconds.
pub fn format_ns(ns: i64) -> String {
    let (secs, nanos) = (ns.div_euclid(NS), ns.rem_euclid(NS));
    let (days, sod) = (secs.div_euclid(DAY_SECS), secs.rem_euclid(DAY_SECS));
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{nanos:09}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

/// `YYYY-MM-DD` (UTC) of unix nanoseconds.
pub fn day_of_ns(ns: i64) -> String {
    format_ns(ns)[..10].to_string()
}

/// Unix nanoseconds of a `ts` string, `None` unless it has the exact format and fits an `i64`
/// (1677-09-21T00:12:43.145224192Z to 2262-04-11T23:47:16.854775807Z).
pub fn parse_ns(ts: &str) -> Option<i64> {
    if !crate::record::is_timestamp(ts) {
        return None;
    }
    let n = |a: usize, b: usize| ts[a..b].parse::<i64>().ok();
    let days = days_from_civil(n(0, 4)?, n(5, 7)?, n(8, 10)?);
    let secs = days * DAY_SECS + n(11, 13)? * 3600 + n(14, 16)? * 60 + n(17, 19)?;
    i64::try_from(i128::from(secs) * i128::from(NS) + i128::from(n(20, 29)?)).ok()
}

/// Unix day number of `YYYY-MM-DD`, `None` unless well-formed.
pub fn parse_day(day: &str) -> Option<i64> {
    parse_ns(&format!("{day}T00:00:00.000000000Z")).map(|ns| ns / NS / DAY_SECS)
}

// Howard Hinnant's algorithms.
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(month <= 2), month, day)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        for ts in [
            "1970-01-01T00:00:00.000000000Z",
            "2026-10-19T08:14:00.000213904Z",
            "2028-02-29T23:59:59.999999999Z",
            "2000-03-01T00:00:00.000000000Z",
        ] {
            assert_eq!(format_ns(parse_ns(ts).unwrap()), ts);
        }
        assert_eq!(
            parse_ns("2026-10-19T08:14:00.000213904Z"),
            Some(1_792_397_640_000_213_904)
        );
        assert_eq!(day_of_ns(1_792_397_640_000_213_904), "2026-10-19");
        assert_eq!(parse_day("2026-10-19"), Some(20745));
        assert_eq!(parse_day("2026-13-19"), None);
        assert!(parse_ns("2026-10-19T08:14:00Z").is_none());
        // The exact i64 bounds; one nanosecond past them, and far dates, are not representable.
        assert_eq!(parse_ns("1677-09-21T00:12:43.145224192Z"), Some(i64::MIN));
        assert_eq!(parse_ns("2262-04-11T23:47:16.854775807Z"), Some(i64::MAX));
        for ts in [
            "1677-09-21T00:12:43.145224191Z",
            "2262-04-11T23:47:16.854775808Z",
            "1600-01-01T00:00:00.000000000Z",
            "3000-01-01T00:00:00.000000000Z",
        ] {
            assert_eq!(parse_ns(ts), None, "{ts}");
        }
    }
}
