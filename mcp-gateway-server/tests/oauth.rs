use axum::{
    body::Body,
    http::{Request, StatusCode},
    Json, Router,
};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use mcp_gateway_server::{
    auth::oauth::{AuthError, OAuthVerifier},
    config::{GatewayConfig, OAuthConfig},
    server::{build_router, AppState},
};
use rsa::{pkcs8::EncodePrivateKey, traits::PublicKeyParts, RsaPrivateKey};
use tower::ServiceExt;
#[tokio::test]
async fn verifies_signature_issuer_audience_expiry_scope_and_metadata() {
    let private = RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let public = private.to_public_key();
    let jwks = serde_json::json!({"keys":[{"kty":"RSA","kid":"test","alg":"RS256","use":"sig","n":URL_SAFE_NO_PAD.encode(public.n().to_bytes_be()),"e":URL_SAFE_NO_PAD.encode(public.e().to_bytes_be())}]});
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let jwks_url = format!("http://{}/jwks", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/jwks",
        axum::routing::get(move || async move { Json(jwks) }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let oauth = OAuthConfig {
        issuer: "https://auth.example".into(),
        audience: "https://gateway.example/mcp".into(),
        jwks_url,
        resource_url: "https://gateway.example/mcp".into(),
        required_scopes: vec!["mcp:read".into()],
        tool_scopes: std::collections::HashMap::from([("echo".into(), vec!["mcp:write".into()])]),
    };
    let verifier = OAuthVerifier::new(oauth.clone());
    let mut header = Header::new(Algorithm::RS256);
    header.kid = Some("test".into());
    let key = EncodingKey::from_rsa_pem(
        private
            .to_pkcs8_pem(rsa::pkcs8::LineEnding::LF)
            .unwrap()
            .as_bytes(),
    )
    .unwrap();
    let now = chrono::Utc::now().timestamp();
    let claims = serde_json::json!({"iss":oauth.issuer,"aud":oauth.audience,"sub":"user-1","scope":"mcp:read","exp":now+300});
    let token = encode(&header, &claims, &key).unwrap();
    let principal = verifier.verify(&token).await.unwrap();
    assert_eq!(principal.subject, "user-1");
    assert!(!mcp_gateway_server::auth::oauth::allows_tool(
        &oauth, &principal, "echo"
    ));
    for (field, bad) in [
        ("aud", serde_json::json!("wrong")),
        ("iss", serde_json::json!("wrong")),
        ("exp", serde_json::json!(now - 60)),
        ("nbf", serde_json::json!(now + 300)),
    ] {
        let mut invalid = claims.clone();
        invalid[field] = bad;
        assert!(matches!(
            verifier
                .verify(&encode(&header, &invalid, &key).unwrap())
                .await,
            Err(AuthError::InvalidToken)
        ));
    }
    let mut no_scope = claims.clone();
    no_scope["scope"] = serde_json::json!("other");
    assert!(matches!(
        verifier
            .verify(&encode(&header, &no_scope, &key).unwrap())
            .await,
        Err(AuthError::InsufficientScope)
    ));
    let mut tampered = token.clone().into_bytes();
    let i = tampered.iter().position(|b| *b == b'.').unwrap() + 1;
    tampered[i] = if tampered[i] == b'a' { b'b' } else { b'a' };
    assert!(verifier
        .verify(&String::from_utf8(tampered).unwrap())
        .await
        .is_err());
    let mut config = GatewayConfig::default();
    config.auth.enabled = true;
    config.auth.oauth = Some(oauth);
    config.backends = vec![mcp_gateway_server::config::BackendConfig {
        name: "stdio".into(),
        transport: "stdio".into(),
        command: Some("python3".into()),
        args: vec![
            "-u".into(),
            format!("{}/tests/fixtures/backend.py", env!("CARGO_MANIFEST_DIR")),
        ],
        ..Default::default()
    }];
    let state = AppState::new(config);
    let app = build_router(state.clone());
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(response.headers()["www-authenticate"]
        .to_str()
        .unwrap()
        .contains("resource_metadata="));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/.well-known/oauth-protected-resource")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    for (method, params) in [
        ("tools/list", serde_json::json!({})),
        (
            "tools/call",
            serde_json::json!({"name":"echo","arguments":{"value":"forbidden"}}),
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/mcp")
                    .header("authorization", format!("Bearer {token}"))
                    .header("content-type", "application/json")
                    .header("accept", "application/json, text/event-stream")
                    .body(Body::from(
                        serde_json::json!({"jsonrpc":"2.0","id":1,"method":method,"params":params})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = axum::body::to_bytes(response.into_body(), 10000)
            .await
            .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
        if method == "tools/list" {
            assert!(value["result"]["tools"].as_array().unwrap().is_empty());
        } else {
            assert!(value.get("error").is_some());
        }
    }
    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .header(
                    "authorization",
                    format!("Bearer {}", encode(&header, &no_scope, &key).unwrap()),
                )
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    state.backends.shutdown().await;
    task.abort();
}
