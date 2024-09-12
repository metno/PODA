use bb8_postgres::PostgresConnectionManager;
use lard_ingestion::{kvkafka, permissions};
use std::sync::{Arc, RwLock};
use tokio_postgres::NoTls;

const PARAMCONV: &str = "resources/paramconversions.csv";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // TODO: use clap for argument parsing
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 5 {
        panic!(concat!(
            "not enough args passed in. At least the group for the kafka queue,",
            "and host, user, dbname needed, optionally password, for postgres"
        ))
    }

    let mut connect_string = format!("host={} user={} dbname={}", &args[2], &args[3], &args[4]);
    if args.len() > 5 {
        connect_string.push_str(" password=");
        connect_string.push_str(&args[5])
    };

    // Permit tables handling (needs connection to stinfosys database)
    let permit_tables = Arc::new(RwLock::new(permissions::fetch_permits().await?));
    let background_permit_tables = permit_tables.clone();

    // background task to refresh permit tables every 30 mins
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30 * 60));

        loop {
            interval.tick().await;
            async {
                // TODO: better error handling here? Nothing is listening to what returns on this task
                // but we could surface failures in metrics. Also we maybe don't want to bork the task
                // forever if these functions fail
                let new_tables = permissions::fetch_permits().await.unwrap();
                let mut tables = background_permit_tables.write().unwrap();
                *tables = new_tables;
            }
            .await;
        }
    });

    // Set up postgres connection pool
    let manager = PostgresConnectionManager::new_from_stringlike(connect_string, NoTls)?;
    let db_pool = bb8::Pool::builder().build(manager).await?;

    // Spawn kvkafka reader
    let kafka_group = args[1].to_string();
    tokio::spawn(kvkafka::read_and_insert(db_pool.clone(), kafka_group));

    // Set up and run our server + database
    lard_ingestion::run(db_pool, PARAMCONV, permit_tables).await
}
