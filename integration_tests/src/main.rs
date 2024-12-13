use std::fs;

use tokio_postgres::{Error, NoTls};

const CONNECT_STRING: &str = "host=localhost user=postgres dbname=postgres password=postgres";

async fn insert_schema(client: &tokio_postgres::Client, filename: &str) -> Result<(), Error> {
    let schema = fs::read_to_string(filename).expect("Should be able to read SQL file");
    client.batch_execute(schema.as_str()).await
}

#[tokio::main]
async fn main() {
    let (client, connection) = tokio_postgres::connect(CONNECT_STRING, NoTls)
        .await
        .expect("Should be able to connect to database");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // NOTE: order matters
    let schemas = [
        "db/public.sql",
        "db/partitions_generated.sql",
        "db/labels.sql",
        "db/flags.sql",
    ];
    for schema in schemas {
        insert_schema(&client, schema).await.unwrap();
    }
}
