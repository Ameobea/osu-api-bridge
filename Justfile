code:
  RUSTFLAGS="-C target-feature=+bmi1,+bmi2,+avx2 --cfg tokio_unstable --cfg foundations_unstable" code .

generate-default-config:
  RUSTFLAGS="-C target-feature=+bmi1,+bmi2,+avx2 --cfg tokio_unstable --cfg foundations_unstable" RUST_LOG=debug cargo run -- --generate default-config.yml

migrate:
  sqlx migrate run

docker-build:
  docker build --network host -t ameo/osu-api-bridge:latest .

run:
  RUSTFLAGS="-C target-feature=+bmi1,+bmi2,+avx2 --cfg tokio_unstable --cfg foundations_unstable" RUST_LOG=debug cargo run -- --config=config.yml

release:
  RUSTFLAGS="-C target-feature=+bmi1,+bmi2,+avx2 --cfg tokio_unstable --cfg foundations_unstable" RUST_LOG=debug cargo run --release -- --config=config.yml

build-and-deploy:
  #!/bin/bash

  just docker-build
  docker save ameo/osu-api-bridge:latest | bzip2 > /tmp/osu-api-bridge.tar.bz2
  scp /tmp/osu-api-bridge.tar.bz2 debian@ameo.dev:/tmp/osu-api-bridge.tar.bz2
  ssh debian@ameo.dev -t 'cat /tmp/osu-api-bridge.tar.bz2 | bunzip2 | docker load && docker kill osu-api-bridge  && docker container rm osu-api-bridge && docker run   --name osu-api-bridge   --restart=always   -d   --net host   -v /opt/conf/osutrack/api-bridge-conf.yml:/opt/conf.yml   -e RUST_LOG=info   ameo/osu-api-bridge:latest   /usr/local/bin/osu-api-bridge --config /opt/conf.yml && rm /tmp/osu-api-bridge.tar.bz2'
