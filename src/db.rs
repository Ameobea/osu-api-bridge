use foundations::BootstrapResult;
use tokio::sync::OnceCell;

static DB_POOL: OnceCell<sqlx::MySqlPool> = OnceCell::const_new();

pub(crate) async fn init_db_pool(db_url: &str) -> BootstrapResult<()> {
  let pool = sqlx::MySqlPool::connect(db_url).await?;
  DB_POOL.set(pool).unwrap();
  Ok(())
}

pub(crate) fn db_pool() -> &'static sqlx::MySqlPool {
  DB_POOL.get().expect("DB pool not initialized")
}
