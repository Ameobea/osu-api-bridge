#![allow(unused_braces)]

use std::sync::Arc;

use axum::{
  extract::Request,
  middleware::{self, Next},
  response::Response,
  Router,
};
use foundations::BootstrapResult;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse};
use tracing::Level;

use crate::settings::ServerSettings;

async fn metrics_middleware(req: Request, next: Next) -> Response {
  let path = Arc::new(req.uri().path().to_owned());
  crate::metrics::http_server::requests_total(&path).inc();
  let res = next.run(req).await;
  if res.status().is_success() {
    crate::metrics::http_server::requests_success_total(&path).inc();
  } else {
    crate::metrics::http_server::requests_failed_total(&path, res.status().as_u16()).inc();
  }
  res
}

async fn index() -> &'static str { "osu-api-bridge up and running successfully!" }

pub async fn start_server(settings: &ServerSettings) -> BootstrapResult<()> {
  let router = Router::new()
    .route("/", axum::routing::get(index))
    .layer(
      tower_http::trace::TraceLayer::new_for_http()
        .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
        .on_response(DefaultOnResponse::default().level(Level::INFO)),
    )
    .layer(middleware::from_fn(metrics_middleware));
  let addr = format!("0.0.0.0:{}", settings.port);
  info!("Server is listening on http://{}", addr);
  let listener = tokio::net::TcpListener::bind(addr).await?;
  axum::serve(listener, router).await?;
  Ok(())
}
