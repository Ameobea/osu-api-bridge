FROM debian:12.5-slim AS builder

RUN apt-get update && apt-get install -y curl build-essential pkg-config libssl-dev

# Install rust
RUN curl https://sh.rustup.rs/ -sSf | \
  sh -s -- -y --default-toolchain nightly-2024-02-03

ENV PATH="/root/.cargo/bin:${PATH}"

ADD . ./

RUN RUSTFLAGS="--cfg tokio_unstable --cfg foundations_unstable" cargo build --release

FROM debian:12.5-slim

RUN apt-get update && apt-get install -y libssl-dev ca-certificates && update-ca-certificates

COPY --from=builder \
  /target/release/osu-api-bridge \
  /usr/local/bin/
WORKDIR /root
CMD /usr/local/bin/osu-api-bridge
