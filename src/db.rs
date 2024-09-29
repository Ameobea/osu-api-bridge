use foundations::BootstrapResult;
use reqwest::StatusCode;
use sqlx::pool::PoolConnection;
use tokio::sync::OnceCell;

use crate::server::APIError;

static DB_POOL: OnceCell<sqlx::MySqlPool> = OnceCell::const_new();

pub(crate) async fn init_db_pool(db_url: &str) -> BootstrapResult<()> {
  let pool = sqlx::MySqlPool::connect(db_url).await?;
  DB_POOL.set(pool).unwrap();
  Ok(())
}

pub(crate) fn db_pool() -> &'static sqlx::MySqlPool {
  DB_POOL.get().expect("DB pool not initialized")
}

pub(crate) async fn conn() -> Result<PoolConnection<sqlx::MySql>, APIError> {
  db_pool().acquire().await.map_err(|err| {
    error!("Error acquiring database connection: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: format!("Error acquiring database connection"),
    }
  })
}
