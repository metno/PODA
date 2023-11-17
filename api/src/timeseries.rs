use crate::util::{Location, PooledPgConn};
use chrono::{DateTime, Utc};
use serde::Serialize;

// TODO: this should be more comprehensive once the schema supports it
// TODO: figure out what should be wrapped in Option here
#[derive(Debug, Serialize)]
pub struct TimeseriesInfo {
    pub ts_id: i32,
    pub fromtime: DateTime<Utc>,
    pub totime: DateTime<Utc>,
    station_id: i32,
    element_id: String,
    lvl: i32,
    sensor: i32,
    location: Location,
}

#[derive(Debug, Serialize)]
pub struct TimeseriesIrregular {
    header: TimeseriesInfo,
    data: Vec<f32>,
    timestamps: Vec<DateTime<Utc>>,
}

pub async fn get_timeseries_info(
    conn: &PooledPgConn<'_>,
    station_id: i32,
    element_id: String,
) -> Result<TimeseriesInfo, tokio_postgres::Error> {
    let ts_result = conn
        .query_one(
            "SELECT timeseries.id, \
                COALESCE(timeseries.fromtime, '1950-01-01 00:00:00+00'), \
                COALESCE(timeseries.totime, '9999-01-01 00:00:00+00'), \
                filter.lvl, \
                filter.sensor, \
                timeseries.loc \
                FROM timeseries JOIN labels.filter \
                    ON timeseries.id = filter.timeseries \
                WHERE filter.station_id = $1 AND filter.element_id = $2 \
                LIMIT 1", // TODO: we should probably do something smarter than LIMIT 1
            &[&station_id, &element_id],
        )
        .await?;

    let ts_id: i32 = ts_result.get(0);
    let fromtime: DateTime<Utc> = ts_result.get(1);
    let totime: DateTime<Utc> = ts_result.get(2);

    Ok(TimeseriesInfo {
        ts_id,
        fromtime,
        totime,
        station_id,
        element_id,
        lvl: ts_result.get(3),
        sensor: ts_result.get(4),
        location: ts_result.get(5),
    })
}

pub async fn get_timeseries_data_irregular(
    conn: &PooledPgConn<'_>,
    header: TimeseriesInfo,
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> Result<TimeseriesIrregular, tokio_postgres::Error> {
    let data_results = conn
        .query(
            "SELECT obsvalue, obstime FROM data \
                WHERE timeseries = $1 \
                    AND obstime BETWEEN $2 AND $3",
            &[&header.ts_id, &start_time, &end_time],
        )
        .await?;

    let ts_irregular = {
        let mut data = Vec::with_capacity(data_results.len());
        let mut timestamps = Vec::with_capacity(data_results.len());

        for row in data_results {
            data.push(row.get(0));
            timestamps.push(row.get(1));
        }

        TimeseriesIrregular {
            header,
            data,
            timestamps,
        }
    };

    Ok(ts_irregular)
}
