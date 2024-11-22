use async_trait::async_trait;
use rove::{
    data_switch,
    data_switch::{DataCache, DataConnector, SpaceSpec, TimeSpec},
};
use thiserror::Error;

#[derive(Error, Debug)]
#[non_exhaustive]
pub enum Error {}

#[derive(Debug)]
pub struct Connector;

#[async_trait]
impl DataConnector for Connector {
    async fn fetch_data(
        &self,
        _space_spec: &SpaceSpec,
        _time_spec: &TimeSpec,
        _num_leading_points: u8,
        _num_trailing_points: u8,
        _extra_spec: Option<&str>,
    ) -> Result<DataCache, data_switch::Error> {
        todo!();
    }
}
