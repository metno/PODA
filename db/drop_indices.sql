-- Remove indices before bulk insertion
DROP INDEX IF EXISTS data_timestamp_index,
                     data_timeseries_index,
                     nonscalar_data_timestamp_index,
                     nonscalar_data_timeseries_index,
                     old_flags_obtime_index,
                     old_flags_timeseries_index;
