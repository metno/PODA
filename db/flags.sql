CREATE SCHEMA IF NOT EXISTS flags;

CREATE TABLE IF NOT EXISTS flags.confident (
    timeseries INT4 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    usable BOOLEAN NOT NULL,
    CONSTRAINT unique_confident_timeseries_obstime UNIQUE (timeseries, obstime),
    -- TODO: should this and confident_providence fk into public.data?
    CONSTRAINT fk_confident_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS confident_timestamp_index ON flags.confident (obstime);
CREATE INDEX IF NOT EXISTS confident_timeseries_index ON flags.confident USING HASH (timeseries);

CREATE TABLE IF NOT EXISTS flags.confident_provenance (
    timeseries INT4 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    pipeline TEXT NOT NULL,
    -- TODO: should this be an enum?
    flag INT4 NOT NULL,
    fail_condition TEXT NULL,
    CONSTRAINT unique_confident_providence_timeseries_obstime_pipeline UNIQUE (timeseries, obstime, pipeline),
    CONSTRAINT fk_confident_providence_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS confident_provenance_timestamp_index ON flags.confident_providence (obstime);
CREATE INDEX IF NOT EXISTS confident_provenance_timeseries_index ON flags.confident_providence USING HASH (timeseries);

CREATE TABLE IF NOT EXISTS flags.kvdata (
    timeseries INT4 REFERENCES public.timeseries,
    obstime TIMESTAMPTZ NOT NULL,
    original REAL NULL, -- could decide not to store this in the future? (KDVH migration will not contain this)
    corrected REAL NULL,
    controlinfo TEXT NULL,
    useinfo TEXT NULL,
    cfailed TEXT NULL,
    CONSTRAINT unique_kvdata_timeseries_obstime UNIQUE (timeseries, obstime)
);
CREATE INDEX IF NOT EXISTS kvdata_obstime_index ON flags.kvdata (obstime);
CREATE INDEX IF NOT EXISTS kvdata_timeseries_index ON flags.kvdata USING HASH (timeseries);
