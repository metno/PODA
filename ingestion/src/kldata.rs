use crate::{
    permissions::{timeseries_is_open, ParamPermitTable, StationPermitTable},
    Datum, Error, PooledPgConn,
};
use chrono::{DateTime, NaiveDateTime, Utc};
use regex::Regex;
use std::{
    collections::HashMap,
    fmt::Debug,
    str::{FromStr, Lines},
    sync::{Arc, RwLock},
};

/// Represents a set of observations that came in the same message from obsinn, with shared
/// station_id and type_id
pub struct ObsinnChunk {
    observations: Vec<ObsinnObs>,
    station_id: i32, // TODO: change name here to nationalnummer?
    type_id: i32,
}

/// Represents a single observation from an obsinn message
pub struct ObsinnObs {
    timestamp: DateTime<Utc>,
    id: ObsinnId,
    value: f32,
}

/// Identifier for a single observation within a given obsinn message
#[derive(Debug, Clone)]
struct ObsinnId {
    param_code: String,
    sensor_and_level: Option<(i32, i32)>,
}

fn parse_meta_field<T: FromStr>(field: &str, expected_key: &'static str) -> Result<T, Error>
where
    <T as FromStr>::Err: Debug,
{
    let (key, value) = field.split_once('=').ok_or_else(|| {
        Error::Parse(format!(
            "unexpected field in kldata header format: {}",
            field
        ))
    })?;

    if key != expected_key {
        return Err(Error::Parse(format!(
            "wrong key in field: expected '{}', got '{}'",
            expected_key, key
        )));
    }

    let res = value
        .parse::<T>()
        .map_err(|_| Error::Parse(format!("invalid number in kldata header for key {}", key)))?;

    Ok(res)
}

fn parse_meta(meta: &str) -> Result<(i32, i32, usize), Error> {
    let mut parts = meta.splitn(4, '/');

    let next_err = || Error::Parse("kldata header terminated early".to_string());

    if parts.next().ok_or_else(next_err)? != "kldata" {
        return Err(Error::Parse(
            "kldata indicator missing or out of order".to_string(),
        ));
    }

    let station_id = parse_meta_field(parts.next().ok_or_else(next_err)?, "nationalnr")?;
    let type_id = parse_meta_field(parts.next().ok_or_else(next_err)?, "type")?;
    let message_id = parse_meta_field(parts.next().ok_or_else(next_err)?, "messageid")?;

    Ok((station_id, type_id, message_id))
}

fn parse_columns(cols_raw: &str) -> Result<Vec<ObsinnId>, Error> {
    // this regex is taken from kvkafka's kldata parser
    // let col_regex = Regex::new(r"([^(),]+)(\([0-9]+,[0-9]+\))?").unwrap();
    // It matches all comma separated fields with pattern of type `name` and `name(x,y)`,
    // where `x` and `y` are ints
    // it is modified below to capture sensor and level separately, while keeping
    // the block collectively optional

    // TODO: is it possible to reuse this regex even more?
    let col_regex = Regex::new(r"([^(),]+)(\(([0-9]+),([0-9]+)\))?").unwrap();

    // TODO: gracefully handle errors here? Even though this shouldn't really ever panic?
    col_regex
        .captures_iter(cols_raw)
        .map(|caps| {
            Ok(ObsinnId {
                param_code: caps.get(1).unwrap().as_str().to_owned(),
                sensor_and_level: caps.get(2).map(|_| {
                    (
                        caps.get(3).unwrap().as_str().parse().unwrap(),
                        caps.get(4).unwrap().as_str().parse().unwrap(),
                    )
                }),
            })
        })
        .collect::<Result<Vec<ObsinnId>, Error>>()
}

fn parse_obs(csv_body: Lines<'_>, columns: &[ObsinnId]) -> Result<Vec<ObsinnObs>, Error> {
    let mut obs = Vec::new();
    let line_is_empty = || Error::Parse("empty row in kldata csv".to_string());

    for row in csv_body {
        let (timestamp, vals) = {
            let mut vals = row.split(',');

            let raw_timestamp = vals.next().ok_or_else(line_is_empty)?;

            // TODO: timestamp parsing needs to handle milliseconds and truncated timestamps?
            let timestamp = NaiveDateTime::parse_from_str(raw_timestamp, "%Y%m%d%H%M%S")
                .map_err(|e| Error::Parse(e.to_string()))?
                .and_utc();

            (timestamp, vals)
        };

        for (i, val) in vals.enumerate() {
            // Should we do some smart bounds-checking??
            let col = columns[i].clone();

            // TODO: parse differently based on param_code?
            obs.push(ObsinnObs {
                timestamp,
                id: col,
                value: val.parse().map_err(|_| {
                    Error::Parse(format!("value {} could not be parsed as float", val))
                })?,
            })
        }
    }

    if obs.is_empty() {
        return Err(line_is_empty());
    }

    Ok(obs)
}

pub fn parse_kldata(msg: &str) -> Result<(usize, ObsinnChunk), Error> {
    // parse the first two lines of the message as meta, and csv column names,
    // leave the rest as an iter over the lines of csv body
    let (station_id, type_id, message_id, columns, csv_body) = {
        let mut lines = msg.lines();

        let lines_err = || Error::Parse("kldata message contained too few lines".to_string());

        let (station_id, type_id, message_id) = parse_meta(lines.next().ok_or_else(lines_err)?)?;
        let columns = parse_columns(lines.next().ok_or_else(lines_err)?)?;

        (station_id, type_id, message_id, columns, lines)
    };

    Ok((
        message_id,
        ObsinnChunk {
            observations: parse_obs(csv_body, &columns)?,
            station_id,
            type_id,
        },
    ))
}

// TODO: rewrite such that queries can be pipelined?
// not pipelining here hurts latency, but shouldn't matter for throughput
pub async fn filter_and_label_kldata(
    chunk: ObsinnChunk,
    conn: &mut PooledPgConn<'_>,
    param_conversions: Arc<HashMap<String, (String, i32)>>,
    permit_table: Arc<RwLock<(ParamPermitTable, StationPermitTable)>>,
) -> Result<Vec<Datum>, Error> {
    let query_get_obsinn = conn
        .prepare(
            "SELECT timeseries \
                FROM labels.obsinn \
                WHERE nationalnummer = $1 \
                    AND type_id = $2 \
                    AND param_code = $3 \
                    AND (($4::int IS NULL AND lvl IS NULL) OR (lvl = $4)) \
                    AND (($5::int IS NULL AND sensor IS NULL) OR (sensor = $5))",
        )
        .await?;

    let mut data = Vec::with_capacity(chunk.observations.len());

    for in_datum in chunk.observations {
        // get the conversion first, so we avoid wasting a tsid if it doesn't exist
        let (_element_id, param_id) =
            param_conversions
                .get(&in_datum.id.param_code)
                .ok_or_else(|| {
                    Error::Parse(format!(
                        "unrecognised param_code {}",
                        in_datum.id.param_code
                    ))
                })?;

        if !timeseries_is_open(
            permit_table.clone(),
            chunk.station_id,
            chunk.type_id,
            param_id.to_owned(),
        )? {
            // TODO: log that the timeseries is closed?
            continue;
        }

        let transaction = conn.transaction().await?;

        let (sensor, lvl) = in_datum
            .id
            .sensor_and_level
            .map(|both| (Some(both.0), Some(both.1)))
            .unwrap_or((None, None));

        let obsinn_label_result = transaction
            .query_opt(
                &query_get_obsinn,
                &[
                    &chunk.station_id,
                    &chunk.type_id,
                    &in_datum.id.param_code,
                    &lvl,
                    &sensor,
                ],
            )
            .await?;

        let timeseries_id: i32 = match obsinn_label_result {
            Some(row) => row.get(0),
            None => {
                // create new timeseries
                // TODO: currently we create a timeseries with null location
                // In the future the location column should be moved to the timeseries metadata table
                let timeseries_id = transaction
                    .query_one(
                        "INSERT INTO public.timeseries (fromtime) VALUES ($1) RETURNING id",
                        &[&in_datum.timestamp],
                    )
                    .await?
                    .get(0);

                // create obsinn label
                transaction
                    .execute(
                        "INSERT INTO labels.obsinn \
                                (timeseries, nationalnummer, type_id, param_code, lvl, sensor) \
                            VALUES ($1, $2, $3, $4, $5, $6)",
                        &[
                            &timeseries_id,
                            &chunk.station_id,
                            &chunk.type_id,
                            &in_datum.id.param_code,
                            &lvl,
                            &sensor,
                        ],
                    )
                    .await?;

                // create met label
                transaction
                    .execute(
                        "INSERT INTO labels.met \
                                (timeseries, station_id, param_id, type_id, lvl, sensor) \
                            VALUES ($1, $2, $3, $4, $5, $6)",
                        &[
                            &timeseries_id,
                            &chunk.station_id,
                            param_id,
                            &chunk.type_id,
                            &lvl,
                            &sensor,
                        ],
                    )
                    .await?;

                timeseries_id
            }
        };

        transaction.commit().await?;

        data.push(Datum {
            timeseries_id,
            timestamp: in_datum.timestamp,
            value: in_datum.value,
        });
    }

    Ok(data)
}
