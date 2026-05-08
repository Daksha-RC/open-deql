## Ingest logs
```sh
curl -u root@example.com:SC7kwSJbGiCoeY8S \
    -k http://localhost:5080/api/default/default/_json \
    -d "[{\"level\":\"info\",\"job\":\"test\",\"log\":\"test message for openobserve\"}]"
```

# Use the cURL command from your Ingestion page
```
curl -u root@example.com:8N2C5pryAgPBajjE \
-H "Content-Type: application/json" \
 http://localhost:5080/api/default/default/_json \
-d "@k8slog_json.json"

curl -u root@example.com:8N2C5pryAgPBajjE -k http://localhost:5080/api/default/default/_json -d "[{\"level\":\"info\",\"job\":\"test\",\"log\":\"test message for openobserve\"}]"

```
### Sample code
```rust
 .allow_origin(AllowOrigin::predicate(|origin, _parts| {
            // Always allow common local dev origins used by the web frontend
            if let Ok(origin_str) = std::str::from_utf8(origin.as_bytes()) {
                if origin_str == "http://localhost:8081" || origin_str == "http://127.0.0.1:8081" {
                    return true;
                }
            }

```
### Dev startup command
```
ZO_ROOT_USER_EMAIL="root@example.com" ZO_ROOT_USER_PASSWORD="Complexpass#123" ZO_CORS_ALLOWED_ORIGINS="http://localhost:8081,http://127.0.0.1:8081" ZO_WEB_URL="http://localhost:8081" cargo run -p openobserve

```

### Linkre setup
```toml
[target.aarch64-apple-darwin]
linker = "/opt/homebrew/opt/llvm/bin/clang"
rustflags = ["-C", "link-arg=-fuse-ld=/opt/homebrew/opt/lld/bin/ld64.lld"]
```
# Build instructions
```
cargo run -p openobserve --features deql 
cargo build  -p openobserve --features deql 
cargo run  -p openobserve --features deql 
```