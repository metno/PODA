use async_trait::async_trait;
use bb8_postgres::PostgresConnectionManager;
use chrono::{DateTime, TimeZone, Utc};
use chronoutil::RelativeDuration;
use rove::data_switch::{self, DataCache, DataConnector, SpaceSpec, TimeSpec, Timeseries};
use thiserror::Error;
use tokio_postgres::NoTls;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("the connector does not know how to handle this time resolution: {0:?}")]
    UnhandledTimeResolution(RelativeDuration),
}

type PgConnectionPool = bb8::Pool<PostgresConnectionManager<NoTls>>;

#[derive(Debug)]
pub struct Connector {
    pool: PgConnectionPool,
}

fn extract_time_spec(
    time_spec: &TimeSpec,
    num_leading_points: u8,
    num_trailing_points: u8,
) -> Result<(DateTime<Utc>, DateTime<Utc>, &str), data_switch::Error> {
    // TODO: matching intervals like this is a hack, but currently necessary to avoid
    // SQL injection. Ideally we could pass an interval type as a query param, which would
    // also save us the query_string allocation, but no ToSql implementations for intervals
    // currently exist in tokio_postgres, so we need to implement it ourselves.
    let interval = match time_spec.time_resolution {
        x if x == RelativeDuration::minutes(1) => "1 minute",
        x if x == RelativeDuration::hours(1) => "1 hour",
        x if x == RelativeDuration::days(1) => "1 day",
        _ => {
            return Err(data_switch::Error::Other(Box::new(
                Error::UnhandledTimeResolution(time_spec.time_resolution),
            )))
        }
    };

    // TODO: should time_spec just use chrono timestamps instead of unix?
    // IIRC the reason for unix timestamps was easy compatibility with protobuf, but that's
    // less of a priority now
    let start_time = Utc.timestamp_opt(time_spec.timerange.start.0, 0).unwrap()
        - (time_spec.time_resolution * num_leading_points.into());
    // TODO: figure out whether the range in postgres is inclusive on the range here or
    // we need to add 1 second
    let end_time = Utc.timestamp_opt(time_spec.timerange.start.0, 0).unwrap()
        + (time_spec.time_resolution * num_trailing_points.into());

    Ok((start_time, end_time, interval))
}

impl Connector {
    async fn fetch_one(
        &self,
        ts_id: i32,
        time_spec: &TimeSpec,
        num_leading_points: u8,
        num_trailing_points: u8,
    ) -> Result<DataCache, data_switch::Error> {
        let (start_time, end_time, interval) =
            extract_time_spec(time_spec, num_leading_points, num_trailing_points)?;

        let query_string = format!("SELECT data.obsvalue, ts_rule.timestamp \
                FROM (SELECT data.obsvalue, data.obstime FROM data WHERE data.timeseries = $1) as data 
                    RIGHT JOIN generate_series($2::timestamptz, $3::timestamptz, interval '{}') AS ts_rule(timestamp) \
                        ON data.obstime = ts_rule.timestamp", interval);

        let conn = self
            .pool
            .get()
            .await
            .map_err(|e| data_switch::Error::Other(Box::new(e)))?;

        let data_results = conn
            .query(query_string.as_str(), &[&ts_id, &start_time, &end_time])
            .await
            .map_err(|e| data_switch::Error::Other(Box::new(e)))?;

        let cache = {
            let mut values = Vec::with_capacity(data_results.len());

            for row in data_results {
                values.push(row.get(0));
            }

            DataCache::new(
                vec![Timeseries {
                    tag: ts_id.to_string(),
                    values,
                }],
                // TODO: we need to either query to get the lat, lon, elev, or change olympian to
                // accept not having them
                vec![],
                vec![],
                vec![],
                time_spec.timerange.start,
                time_spec.time_resolution,
                num_leading_points,
                num_trailing_points,
            )
        };

        Ok(cache)
    }
}

#[async_trait]
impl DataConnector for Connector {
    async fn fetch_data(
        &self,
        space_spec: &SpaceSpec,
        time_spec: &TimeSpec,
        num_leading_points: u8,
        num_trailing_points: u8,
        _extra_spec: Option<&str>,
    ) -> Result<DataCache, data_switch::Error> {
        match space_spec {
            SpaceSpec::One(ts_id) => {
                self.fetch_one(
                    ts_id
                        .parse()
                        .map_err(|_| data_switch::Error::InvalidSeriesId(ts_id.to_string()))?,
                    time_spec,
                    num_leading_points,
                    num_trailing_points,
                )
                .await
            }
            // TODO: We should handle at least the All case, Polygon can be left unimplemented for
            // now
            _ => todo!(),
        }
    }
}
