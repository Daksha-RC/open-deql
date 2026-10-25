FROM public.ecr.aws/docker/library/rust:trixie

ARG BUN_VERSION=1.4.3
ARG USERNAME=coder
ARG UID=1000
ARG GID=1000

ENV USERNAME=${USERNAME}
ENV DEV_UID=${UID}
ENV DEV_GID=${GID}
ENV BUN_INSTALL=/opt/bun
ENV PATH="/opt/bun/bin:${PATH}"


RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        protobuf-compiler \
        libclang-dev \
        libprotobuf-dev \
        clang ripgrep \
        cmake curl \
        less && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && \
    apt-get install -y --no-install-recommends mold && \
    rm -rf /var/lib/apt/lists/*

# Install all dependencies of  rust in one layer
RUN rustup toolchain install nightly-2026-05-20 \
        --component clippy,rustfmt,rust-analyzer,llvm-tools,rust-src && \
    rustup default nightly-2026-05-20
    # rustup target add aarch64-unknown-linux-gnu

RUN apt-get update && \
    apt-get install -y --no-install-recommends lld && \
    rm -rf /var/lib/apt/lists/*
# Install Node.js 24 (includes npm) and pnpm
RUN curl -fsSL https://deb.nodesource.com/setup_24.x | bash - \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs curl unzip \
    && npm install -g pnpm \
    && node --version \
    && pnpm --version \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


RUN cargo install cargo-binstall --locked && \
    cargo binstall --no-confirm --strategies crate-meta-data jj-cli

RUN cargo binstall --no-confirm jjpr

RUN mkdir -p "$BUN_INSTALL" \
    && curl -fsSL https://bun.sh/install | bash -s "bun-v${BUN_VERSION}" \
    && bun install -g @oh-my-pi/pi-coding-agent \
    && bun install -g @fission-ai/openspec@latest \
    && chmod -R a+rX /opt/bun

RUN echo "bunx version: $(bunx --version)" \
    && echo "which bunx : $(which bunx)" \
    && echo "omp version: $(omp --version)"  \
    && echo "which omp : $(which omp)" \
    && echo "openspec version: $(openspec --version)"  \
    && echo "which openspec : $(which openspec)"

# Create group + user
RUN useradd \
    --create-home \
    --shell /bin/bash \
    ${USERNAME}

RUN ln -sf /opt/bun/bin/bun  /usr/bin/bun \
    && ln -sf /opt/bun/bin/bunx /usr/bin/bunx \
    && ln -sf /opt/bun/bin/omp  /usr/bin/omp \
    && ln -sf /opt/bun/bin/openspec  /usr/bin/openspec

RUN mkdir -p /home/${USERNAME}/open-deql && chown ${USERNAME}:${USERNAME} /home/${USERNAME}/open-deql
RUN mkdir -p /usr/local/cargo/registry /usr/local/cargo/git && \
    chown -R ${USERNAME}:${USERNAME} \
        /usr/local/cargo/registry \
        /usr/local/cargo/git

USER ${USERNAME}
WORKDIR /home/${USERNAME}/open-deql
