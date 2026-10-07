ARG USERNAME=developer
ARG UID=501
ARG GID=20

FROM public.ecr.aws/docker/library/rust:trixie

ARG USERNAME=developer
ARG UID=501
ARG GID=20

ENV USERNAME=${USERNAME}
ENV DEV_UID=${UID}
ENV DEV_GID=${GID}

# Install all dependencies and setup rust in one layer
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    protobuf-compiler \
    libclang-dev \
    libprotobuf-dev \
    clang ripgrep \
    cmake curl \
    less && \
    rustup toolchain install nightly-2026-05-20 && \
    rustup default nightly-2026-05-20 && \
    rustup target add x86_64-unknown-linux-gnu && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Node.js 24 (includes npm) and pnpm
RUN curl -fsSL https://deb.nodesource.com/setup_24.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    npm install -g pnpm && \
    npm install -g @fission-ai/openspec@latest \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN cargo install cargo-binstall --locked && \
        cargo binstall --no-confirm --strategies crate-meta-data jj-cli

RUN cargo binstall --no-confirm jjpr

# Create group + user
RUN useradd \
  --create-home \
  --shell /bin/bash \
  developer

# UID:GID should be hard coded due to unknown issue with colima and docker
RUN  mkdir -p /workspaces \
    && chown 501:20  /workspaces

RUN mkdir -p /workspaces/open-deql/target && \
    chown -R 501:20 /workspaces/open-deql/target

RUN mkdir -p /tmp/open-deql-tmp && \
    chown -R 501:20 /tmp/open-deql-tmp

RUN mkdir -p  /tmp && \
    chown -R 501:20 /tmp
RUN mkdir -p /workspaces/open-deql/web/node_modules && \
    chown -R 501:20 /workspaces/open-deql/web/node_modules

RUN mkdir -p /home/developer/.rustup && \
    chown -R 501:20 /home/developer/.rustup
RUN mkdir -p /home/developer/.cargo && \
    chown -R 501:20 /home/developer/.cargo

RUN echo '\n# Rust and Cargo binaries\nexport PATH="$CARGO_HOME/bin:$PATH"' \
    >> /home/developer/.profile \
    && chown 501:20 /home/developer/.profile

USER ${USERNAME}
WORKDIR /workspaces
