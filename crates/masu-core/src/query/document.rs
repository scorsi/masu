//! Bounded protobuf / ProtoJSON documents (ADR 0013). No HTTP or evaluation.

use crate::pb;
use prost::Message;
use std::collections::BTreeMap;
use std::fmt;

mod json;
mod wire;

pub const MAX_REQUEST_BYTES: usize = 64 * 1024;
pub const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_QUERIES: usize = 64;
const MAX_RANGE_NS: i128 = 31 * 86_400 * 1_000_000_000;

#[derive(Clone, Copy, Debug)]
pub enum Encoding {
    Protobuf,
    Json,
}

#[derive(Debug)]
pub enum DecodedQuery {
    Known(pb::Query),
    /// No executable known fields survive an unknown member anywhere in this query.
    Unsupported,
    /// The envelope is usable, but this known query has malformed parameters.
    Invalid,
}

#[derive(Debug)]
pub struct DecodedRequest {
    pub range: pb::Range,
    pub queries: BTreeMap<String, DecodedQuery>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    RequestTooLarge,
    ScanLimit,
    InvalidResponse,
    ResponseLimit,
}

impl Error {
    pub fn reason(self) -> &'static str {
        match self {
            Self::InvalidRequest => "invalid-request",
            Self::RequestTooLarge => "request-too-large",
            Self::ScanLimit => "scan-limit",
            Self::InvalidResponse => "invalid-response",
            Self::ResponseLimit => "response-limit",
        }
    }
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.reason())
    }
}
impl std::error::Error for Error {}

type Result<T> = std::result::Result<T, Error>;

pub fn decode_request(bytes: &[u8], encoding: Encoding) -> Result<DecodedRequest> {
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(Error::RequestTooLarge);
    }
    let (request, unsupported, invalid) = match encoding {
        Encoding::Protobuf => {
            let unsupported = wire::request(bytes)?;
            let request = pb::QueryRequest::decode(bytes).map_err(|_| Error::InvalidRequest)?;
            (request, unsupported, std::collections::BTreeSet::new())
        }
        Encoding::Json => json::request(bytes)?,
    };
    validate_request(&request)?;
    Ok(DecodedRequest {
        range: request.range.unwrap(),
        queries: request
            .queries
            .into_iter()
            .map(|(name, query)| {
                let query = if unsupported.contains(&name) {
                    DecodedQuery::Unsupported
                } else if invalid.contains(&name) || validate_query_wkt(&query).is_err() {
                    DecodedQuery::Invalid
                } else {
                    DecodedQuery::Known(query)
                };
                (name, query)
            })
            .collect(),
    })
}

pub fn encode_request(request: &pb::QueryRequest, encoding: Encoding) -> Result<Vec<u8>> {
    validate_request(request)?;
    for query in request.queries.values() {
        validate_query_wkt(query)?;
    }
    if request.encoded_len() > MAX_REQUEST_BYTES {
        return Err(Error::RequestTooLarge);
    }
    let bytes = match encoding {
        Encoding::Protobuf => request.encode_to_vec(),
        Encoding::Json => json::encode(
            &json::request_value(request),
            MAX_REQUEST_BYTES,
            Error::RequestTooLarge,
        )?,
    };
    if bytes.len() > MAX_REQUEST_BYTES {
        return Err(Error::RequestTooLarge);
    }
    Ok(bytes)
}

/// The caller also checks that result names are exactly the names of its request.
pub fn decode_response(bytes: &[u8], encoding: Encoding) -> Result<pb::QueryResponse> {
    if bytes.len() > MAX_RESPONSE_BYTES {
        return Err(Error::ResponseLimit);
    }
    let response = match encoding {
        Encoding::Protobuf => {
            wire::response(bytes).map_err(|_| Error::InvalidResponse)?;
            pb::QueryResponse::decode(bytes).map_err(|_| Error::InvalidResponse)?
        }
        Encoding::Json => json::response(bytes).map_err(|_| Error::InvalidResponse)?,
    };
    validate_response(&response)?;
    Ok(response)
}

pub fn encode_response(response: &pb::QueryResponse, encoding: Encoding) -> Result<Vec<u8>> {
    validate_response(response)?;
    if matches!(encoding, Encoding::Protobuf) && response.encoded_len() > MAX_RESPONSE_BYTES {
        return Err(Error::ResponseLimit);
    }
    match encoding {
        Encoding::Protobuf => Ok(response.encode_to_vec()),
        Encoding::Json => json::encode_response(response),
    }
}

fn timestamp_ns(ts: &prost_types::Timestamp) -> Result<i128> {
    if !(-62_135_596_800..=253_402_300_799).contains(&ts.seconds)
        || !(0..1_000_000_000).contains(&ts.nanos)
    {
        return Err(Error::InvalidRequest);
    }
    Ok(i128::from(ts.seconds) * 1_000_000_000 + i128::from(ts.nanos))
}

fn duration_ns(d: &prost_types::Duration) -> Result<i128> {
    if !(-315_576_000_000..=315_576_000_000).contains(&d.seconds)
        || !(-999_999_999..=999_999_999).contains(&d.nanos)
        || (d.seconds > 0 && d.nanos < 0)
        || (d.seconds < 0 && d.nanos > 0)
    {
        return Err(Error::InvalidRequest);
    }
    Ok(i128::from(d.seconds) * 1_000_000_000 + i128::from(d.nanos))
}

fn validate_request(request: &pb::QueryRequest) -> Result<()> {
    if request.queries.is_empty() || request.queries.len() > MAX_QUERIES {
        return Err(Error::InvalidRequest);
    }
    let range = request.range.as_ref().ok_or(Error::InvalidRequest)?;
    let from = timestamp_ns(range.from.as_ref().ok_or(Error::InvalidRequest)?)?;
    let to = timestamp_ns(range.to.as_ref().ok_or(Error::InvalidRequest)?)?;
    if from >= to {
        return Err(Error::InvalidRequest);
    }
    if to - from > MAX_RANGE_NS {
        return Err(Error::ScanLimit);
    }
    Ok(())
}

fn validate_query_wkt(query: &pb::Query) -> Result<()> {
    if let Some(pb::query::Body::Series(q)) = &query.body
        && let Some(step) = &q.step
    {
        duration_ns(step)?;
    }
    Ok(())
}

fn validate_series(series: &pb::Series) -> Result<()> {
    if let Some(start) = &series.start {
        timestamp_ns(start)?;
    }
    for ts in &series.ts {
        timestamp_ns(ts)?;
    }
    match &series.step {
        Some(step) if duration_ns(step)? > 0 && series.start.is_some() && series.ts.is_empty() => {
            Ok(())
        }
        None if series.start.is_none() && series.ts.len() == series.values.len() => Ok(()),
        _ => Err(Error::InvalidRequest),
    }
}
fn validate_result(result: &pb::QueryResult) -> Result<()> {
    let body = result.body.as_ref().ok_or(Error::InvalidRequest)?;
    if !result.cursor.is_empty() && !matches!(body, pb::query_result::Body::Table(_)) {
        return Err(Error::InvalidRequest);
    }
    match body {
        pb::query_result::Body::Series(frame) => {
            for series in &frame.series {
                validate_series(series)?;
            }
        }
        pb::query_result::Body::Counts(frame) => {
            timestamp_ns(frame.start.as_ref().ok_or(Error::InvalidRequest)?)?;
            if duration_ns(frame.step.as_ref().ok_or(Error::InvalidRequest)?)? <= 0 {
                return Err(Error::InvalidRequest);
            }
            if let Some(first) = frame.groups.first()
                && frame
                    .groups
                    .iter()
                    .any(|g| g.counts.len() != first.counts.len())
            {
                return Err(Error::InvalidRequest);
            }
        }
        pb::query_result::Body::Table(frame) => {
            for row in &frame.rows {
                json::validate_struct(row, 0)?;
            }
        }
        pb::query_result::Body::Error(error) => {
            if error.reason.is_empty() {
                return Err(Error::InvalidRequest);
            }
        }
    }
    Ok(())
}
fn validate_response(response: &pb::QueryResponse) -> Result<()> {
    if response.results.is_empty() || response.results.len() > MAX_QUERIES {
        return Err(Error::InvalidResponse);
    }
    for result in response.results.values() {
        validate_result(result).map_err(|_| Error::InvalidResponse)?;
    }
    Ok(())
}
/// The bare message/object length, before its parent's field tags, map entry or separators.
pub(crate) fn series_encoded_len(series: &pb::Series, encoding: Encoding) -> Result<u64> {
    validate_series(series).map_err(|_| Error::InvalidResponse)?;
    match encoding {
        Encoding::Protobuf => Ok(series.encoded_len() as u64),
        Encoding::Json => json::series_size(series),
    }
}
pub(crate) fn result_encoded_len(result: &pb::QueryResult, encoding: Encoding) -> Result<u64> {
    validate_result(result).map_err(|_| Error::InvalidResponse)?;
    match encoding {
        Encoding::Protobuf => Ok(result.encoded_len() as u64),
        Encoding::Json => json::result_size(result),
    }
}
pub(crate) fn table_row_encoded_len(row: &prost_types::Struct, encoding: Encoding) -> Result<u64> {
    json::validate_struct(row, 0).map_err(|_| Error::InvalidResponse)?;
    match encoding {
        Encoding::Protobuf => Ok(row.encoded_len() as u64),
        Encoding::Json => json::table_row_size(row),
    }
}
pub(crate) fn response_encoded_len(
    response: &pb::QueryResponse,
    encoding: Encoding,
) -> Result<u64> {
    validate_response(response)?;
    match encoding {
        Encoding::Protobuf => Ok(response.encoded_len() as u64),
        Encoding::Json => json::response_size(response),
    }
}

#[cfg(test)]
mod measured_tests {
    use super::*;
    #[test]
    fn query_document_borrowed_sizes_equal_actual_serialization_in_both_encodings() {
        let ts = prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 1,
        };
        let series = pb::Series {
            labels: std::collections::HashMap::from([("site".into(), "é\"\\\n".into())]),
            ts: vec![ts; 3],
            values: vec![-0.0, f64::NAN, f64::INFINITY],
            ..Default::default()
        };
        let row = prost_types::Struct {
            fields: std::collections::BTreeMap::from([(
                "message".into(),
                prost_types::Value {
                    kind: Some(prost_types::value::Kind::StringValue("x\n\"".into())),
                },
            )]),
        };
        let result = pb::QueryResult {
            body: Some(pb::query_result::Body::Series(pb::SeriesFrame {
                series: vec![series.clone()],
            })),
            ..Default::default()
        };
        let response = pb::QueryResponse {
            results: std::collections::BTreeMap::from([
                ("chart".into(), result.clone()),
                (
                    "events".into(),
                    pb::QueryResult {
                        body: Some(pb::query_result::Body::Table(pb::TableFrame {
                            rows: vec![row.clone()],
                        })),
                        cursor: "c\"".into(),
                    },
                ),
            ]),
            budget: Some(pb::Budget {
                records_read: u64::MAX,
                bytes_read: 11,
                elapsed_ms: 2,
            }),
        };
        for encoding in [Encoding::Json, Encoding::Protobuf] {
            let bytes = encode_response(&response, encoding).unwrap();
            assert_eq!(
                response_encoded_len(&response, encoding).unwrap(),
                bytes.len() as u64
            );
            let (series_size, result_size, row_size) = match encoding {
                Encoding::Protobuf => (
                    series.encoded_len() as u64,
                    result.encoded_len() as u64,
                    row.encoded_len() as u64,
                ),
                Encoding::Json => {
                    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    (
                        serde_json::to_vec(&json["results"]["chart"]["series"]["series"][0])
                            .unwrap()
                            .len() as u64,
                        serde_json::to_vec(&json["results"]["chart"]).unwrap().len() as u64,
                        serde_json::to_vec(&json["results"]["events"]["table"]["rows"][0])
                            .unwrap()
                            .len() as u64,
                    )
                }
            };
            assert_eq!(series_encoded_len(&series, encoding).unwrap(), series_size);
            assert_eq!(result_encoded_len(&result, encoding).unwrap(), result_size);
            assert_eq!(table_row_encoded_len(&row, encoding).unwrap(), row_size);
        }
    }
}
