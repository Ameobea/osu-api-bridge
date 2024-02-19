docker-build:
  docker build -t ameo/osu-api-bridge .

run:
  RUSTFLAGS="--cfg tokio_unstable --cfg foundations_unstable" RUST_LOG=debug cargo run -- --config=config.yml
