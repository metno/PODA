use axum::{response::Json, routing::post, Router};
use chrono::{DateTime, Utc};
use serde::Serialize;

pub struct Datum {
    timeseries_id: i32,
    timestamp: DateTime<Utc>,
    value: f32,
}

pub mod kldata;
use kldata::parse_kldata;

#[derive(Debug, Serialize)]
struct KldataResp {
    message: String,
    message_id: usize,
    res: u8, // TODO: Should be an enum?
    retry: bool,
}

async fn handle_kldata(body: String) -> Json<KldataResp> {
    let (message_id, _obsinn_chunk) = parse_kldata(&body).unwrap();

    // TODO: Find or generate obsinn labels
    // TODO: Find or generate filter labels

    // TODO: Insert into data table

    Json(KldataResp {
        // TODO: fill in meaningful values here
        message: "".into(),
        message_id,
        res: 0,
        retry: false,
    })
}

#[tokio::main]
pub async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // build our application with a single route
    let app = Router::new().route("/kldata", post(handle_kldata));

    // run our app with hyper, listening globally on port 3000
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3001").await.unwrap();
    axum::serve(listener, app).await?;

    Ok(())
}
