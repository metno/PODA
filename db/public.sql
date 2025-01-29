DO $$ 
BEGIN
    IF (SELECT NOT EXISTS (select from pg_type where typname = 'location')) THEN
    CREATE TYPE location AS (
        lat REAL,
        lon REAL,
        hamsl REAL,
        hag REAL
    );
END IF;
END $$;

DO $$ 
BEGIN
    IF (SELECT NOT EXISTS (select from pg_type where typname = 'obs')) THEN
    CREATE TYPE obs AS (
        obstime TIMESTAMPTZ,
        obsvalue REAL
    );
END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.timeseries (
    id SERIAL PRIMARY KEY,
    fromtime TIMESTAMPTZ NULL,
    totime TIMESTAMPTZ NULL,
    loc location NULL, 
    deactivated BOOL NULL
);

CREATE TABLE IF NOT EXISTS public.data (
    timeseries INT4 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    obsvalue REAL,
    -- TODO: should qc_usable be NOT NULL? and default to true?
    -- It would make greatly reduce the update load when QCing old data
    qc_usable BOOLEAN,
    CONSTRAINT unique_data_timeseries_obstime UNIQUE (timeseries, obstime),
    CONSTRAINT fk_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS data_timestamp_index ON public.data (obstime);
CREATE INDEX IF NOT EXISTS data_timeseries_index ON public.data USING HASH (timeseries);


CREATE TABLE IF NOT EXISTS public.nonscalar_data (
    timeseries INT4 NOT NULL,
    obstime TIMESTAMPTZ NOT NULL,
    obsvalue TEXT,
    qc_usable BOOLEAN,
    CONSTRAINT unique_nonscalar_data_timeseries_obstime UNIQUE (timeseries, obstime),
    CONSTRAINT fk_nonscalar_data_timeseries FOREIGN KEY (timeseries) REFERENCES public.timeseries
) PARTITION BY RANGE (obstime);
CREATE INDEX IF NOT EXISTS nonscalar_data_timestamp_index ON public.nonscalar_data (obstime);
CREATE INDEX IF NOT EXISTS nonscalar_data_timeseries_index ON public.nonscalar_data USING HASH (timeseries);
