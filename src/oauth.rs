use reqwest::{Client, StatusCode};
use serde::Deserialize;
use std::{
  future::Future,
  sync::Arc,
  time::{Duration, Instant},
};
use tokio::sync::{Mutex, OnceCell, RwLock};

use crate::{metrics::http_server, server::APIError};

#[derive(Clone, Deserialize)]
struct OAuthToken {
  token_type: String,
  access_token: String,
  expires_in: u32,
  refresh_token: Option<String>,
}

impl OAuthToken {
  fn build_auth_header(&self) -> String { format!("{} {}", self.token_type, self.access_token) }
}

// curl --request POST \
//     "https://osu.ppy.sh/oauth/token" \
//     --header "Accept: application/json" \
//     --header "Content-Type: application/x-www-form-urlencoded" \
//     --data "client_id=id&client_secret=secret&grant_type=client_credentials&scope=public"
async fn fetch_access_token() -> Result<OAuthToken, APIError> {
  let client_info = get_client_info();
  let form = [
    ("client_id", client_info.client_id.to_string()),
    ("client_secret", client_info.client_secret.to_string()),
    ("grant_type", "client_credentials".to_string()),
    ("scope", "public".to_string()),
  ];
  let res = REQWEST_CLIENT
    .post("https://osu.ppy.sh/oauth/token")
    .header("Accept", "application/json")
    .header("Content-Type", "application/x-www-form-urlencoded")
    .form(&form)
    .send()
    .await
    .map_err(|err| {
      error!("Failed to fetch access token: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to fetch access token".to_owned(),
      }
    })?;
  res.json().await.map_err(|err| {
    error!("Failed to read access token res: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to read access token response".to_owned(),
    }
  })
}

#[cfg(feature = "sql")]
async fn get_user_refresh_token(
  executor: impl sqlx::Executor<'_, Database = sqlx::MySql>,
) -> sqlx::Result<String> {
  sqlx::query_scalar!("SELECT refresh_token FROM user_refresh_token LIMIT 1 FOR UPDATE")
    .fetch_one(executor)
    .await
}

#[cfg(feature = "sql")]
async fn update_user_refresh_token(
  executor: impl sqlx::Executor<'_, Database = sqlx::MySql>,
  refresh_token: &str,
) -> sqlx::Result<()> {
  sqlx::query!(
    "UPDATE user_refresh_token SET refresh_token = ?",
    refresh_token
  )
  .execute(executor)
  .await?;
  Ok(())
}

#[cfg(feature = "sql")]
async fn fetch_user_access_token() -> Result<OAuthToken, APIError> {
  use crate::db::db_pool;

  let url = "https://osu.ppy.sh/oauth/token";
  let pool = db_pool();
  let mut txn = pool.begin().await.map_err(|err| {
    error!("Failed to start transaction: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to start transaction".to_owned(),
    }
  })?;
  let user_refresh_token = get_user_refresh_token(&mut *txn).await.map_err(|err| {
    error!("Failed to fetch user refresh token: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to fetch user refresh token".to_owned(),
    }
  })?;

  let client_info = get_client_info();
  let data = format!(
    "client_id={}&client_secret={}&grant_type=refresh_token&refresh_token={}&scope=public+identify",
    client_info.client_id, client_info.client_secret, user_refresh_token
  );
  let res_text = REQWEST_CLIENT
    .post(url)
    .header("Accept", "application/json")
    .header("Content-Type", "application/x-www-form-urlencoded")
    .body(data)
    .send()
    .await
    .map_err(|err| {
      error!("Failed to fetch user access token: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to fetch user access token".to_owned(),
      }
    })?
    .text()
    .await
    .map_err(|err| {
      error!("Failed to read user access token res: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to read user access token response".to_owned(),
      }
    })?;

  let deserializer = &mut serde_json::Deserializer::from_str(&res_text);
  let token_res: OAuthToken = match serde_path_to_error::deserialize(deserializer) {
    Ok(token) => Ok(token),
    Err(err) => {
      error!("Failed to parse user access token res: {res_text}; err: {err}");
      http_server::osu_api_requests_failed_total("fetch_user_hiscores", 200).inc();
      Err(APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to parse user access token response".to_owned(),
      })
    },
  }?;

  let Some(refresh_token) = &token_res.refresh_token else {
    error!("No refresh token in user access token response");
    http_server::osu_api_requests_failed_total("fetch_user_hiscores", 200).inc();
    return Err(APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "No refresh token in user access token response".to_owned(),
    });
  };
  info!(
    "Successfully fetched new user access token + refresh token.  Updating refresh token in DB..."
  );

  update_user_refresh_token(&mut *txn, refresh_token)
    .await
    .map_err(|err| {
      error!("Failed to update user refresh token: {err}");
      APIError {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "Failed to update user refresh token".to_owned(),
      }
    })?;
  txn.commit().await.map_err(|err| {
    error!("Failed to commit transaction: {err}");
    APIError {
      status: StatusCode::INTERNAL_SERVER_ERROR,
      message: "Failed to commit transaction".to_owned(),
    }
  })?;
  info!("Successfully updated refresh token in DB");

  Ok(token_res)
}

struct TokenCache {
  token: Option<OAuthToken>,
  expiration: Option<std::time::Instant>,
}

impl TokenCache {
  fn new() -> Self {
    TokenCache {
      token: None,
      expiration: None,
    }
  }

  fn is_valid(&self) -> bool {
    match self.expiration {
      // Add in 30 seconds of leeway to account for possible delay
      Some(expiration) => expiration > std::time::Instant::now() + Duration::from_secs(30),
      None => false,
    }
  }
}

pub struct ClientInfo {
  client_id: u32,
  client_secret: String,
}

lazy_static::lazy_static! {
  static ref TOKEN_CACHE: Arc<RwLock<TokenCache>> = Arc::new(RwLock::new(TokenCache::new()));

  #[cfg(feature = "daily_challenge")]
  static ref USER_TOKEN_CACHE: Arc<RwLock<TokenCache>> = Arc::new(RwLock::new(TokenCache::new()));

  pub static ref REQWEST_CLIENT: Client = Client::new();

  static ref CLIENT_INFO: OnceCell<ClientInfo> = OnceCell::new();
}

/// This must be called before any requests are made
pub fn set_client_info(client_id: u32, client_secret: String) {
  let _ = CLIENT_INFO.set(ClientInfo {
    client_id,
    client_secret,
  });
}

pub fn get_client_info() -> &'static ClientInfo { CLIENT_INFO.get().expect("Client info not set") }

async fn get_generic_auth_header<F: Future<Output = Result<OAuthToken, APIError>>>(
  token_cache: &mut TokenCache,
  fetch_access_token: impl Fn() -> F,
) -> Result<String, APIError> {
  info!("Fetching new OAuth token");
  let now = Instant::now();
  http_server::oauth_refresh_requests_total().inc();
  http_server::osu_api_requests_total("fetch_token").inc();

  let mut attempts = 0;
  let max_attempts = 8;
  let mut last_err = None;

  while attempts < max_attempts {
    match fetch_access_token().await {
      Ok(oauth_token) => {
        info!("Successfully fetched new OAuth token");
        token_cache.token = Some(oauth_token.clone());
        token_cache.expiration =
          Some(std::time::Instant::now() + Duration::from_secs(oauth_token.expires_in as u64));
        http_server::oauth_refresh_response_time_seconds().observe(now.elapsed().as_nanos() as u64);
        return Ok(oauth_token.build_auth_header());
      },
      Err(err) => {
        error!("Failed to fetch token: {err:?}");
        http_server::oauth_refresh_requests_failed_total().inc();
        attempts += 1;
        last_err = Some(err);
        tokio::time::sleep(Duration::from_secs(attempts)).await; // Linear backoff
      },
    }
  }

  Err(last_err.unwrap())
}

pub async fn get_auth_header() -> Result<String, APIError> {
  let token_cache = TOKEN_CACHE.read().await;
  if token_cache.is_valid() {
    if let Some(ref token) = token_cache.token {
      return Ok(token.build_auth_header());
    }
  }

  drop(token_cache);
  let mut token_cache = TOKEN_CACHE.write().await;
  get_generic_auth_header(&mut token_cache, fetch_access_token).await
}

#[cfg(feature = "daily_challenge")]
pub async fn get_user_auth_header() -> Result<String, APIError> {
  let token_cache = USER_TOKEN_CACHE.read().await;
  if token_cache.is_valid() {
    if let Some(ref token) = token_cache.token {
      return Ok(token.build_auth_header());
    }
  }

  drop(token_cache);
  let mut token_cache = USER_TOKEN_CACHE.write().await;
  get_generic_auth_header(&mut token_cache, fetch_user_access_token).await
}
