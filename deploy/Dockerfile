FROM debian:bookworm

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
       build-essential cmake ninja-build libyaml-cpp-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cmake -S . -B /build -G Ninja -DBUILD_TESTING=OFF \
    && cmake --build /build --target aegisgate_server aegisgate_mock_backend

ENTRYPOINT ["/build/aegisgate_server"]
