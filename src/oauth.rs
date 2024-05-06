use reqwest::Client;
use serde::Deserialize;
use std::{
  sync::Arc,
  time::{Duration, Instant},
};
use tokio::sync::{Mutex, OnceCell};

use crate::metrics::http_server;

#[derive(Clone, Deserialize)]
struct OAuthToken {
  token_type: String,
  access_token: String,
  expires_in: u32,
}

impl OAuthToken {
  fn build_auth_header(&self) -> String { format!("{} {}", self.token_type, self.access_token) }
}

// curl --request POST \
//     "https://osu.ppy.sh/oauth/token" \
//     --header "Accept: application/json" \
//     --header "Content-Type: application/x-www-form-urlencoded" \
//     --data "client_id=id&client_secret=secret&grant_type=client_credentials&scope=public"
async fn fetch_token() -> Result<OAuthToken, reqwest::Error> {
  let client_info = CLIENT_INFO.get().expect("Client info not set");
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
    .await?;
  res.json().await
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

struct ClientInfo {
  client_id: u32,
  client_secret: String,
}

// Global cache and Mutex
lazy_static::lazy_static! {
  static ref TOKEN_CACHE: Arc<Mutex<TokenCache>> = Arc::new(Mutex::new(TokenCache::new()));

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

pub async fn get_auth_header() -> Result<String, reqwest::Error> {
  let cache_lock = TOKEN_CACHE.clone();
  let mut token_cache = cache_lock.lock().await;

  if token_cache.is_valid() {
    if let Some(ref token) = token_cache.token {
      return Ok(token.build_auth_header());
    }
  }

  info!("Fetching new OAuth token");
  let now = Instant::now();
  http_server::oauth_refresh_requests_total().inc();
  http_server::osu_api_requests_total("fetch_token").inc();

  let mut attempts = 0;
  let max_attempts = 8;
  let mut last_err = None;

  while attempts < max_attempts {
    match fetch_token().await {
      Ok(oauth_token) => {
        info!("Successfully fetched new OAuth token");
        token_cache.token = Some(oauth_token.clone());
        token_cache.expiration =
          Some(std::time::Instant::now() + Duration::from_secs(oauth_token.expires_in as u64));
        http_server::oauth_refresh_response_time_seconds().observe(now.elapsed().as_nanos() as u64);
        return Ok(oauth_token.build_auth_header());
      },
      Err(err) => {
        error!("Failed to fetch token: {err}");
        http_server::oauth_refresh_requests_failed_total().inc();
        attempts += 1;
        last_err = Some(err);
        tokio::time::sleep(Duration::from_secs(attempts)).await; // Linear backoff
      },
    }
  }

  Err(last_err.unwrap())
}
