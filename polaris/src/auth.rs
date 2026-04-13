use serde::Deserialize;

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

/// Acquire an OAuth2 bearer token from Polaris using client credentials flow.
pub async fn acquire_token(
    base_url: &str,
    client_id: &str,
    client_secret: &str,
    scope: &str,
) -> Result<String, String> {
    let url = format!("{base_url}/api/catalog/v1/oauth/tokens");
    let client = reqwest::Client::new();
    let resp = client
        .post(&url)
        .header("Content-Type", "application/x-www-form-urlencoded")
        .body(format!(
            "grant_type=client_credentials&client_id={client_id}&client_secret={client_secret}&scope={scope}"
        ))
        .send()
        .await
        .map_err(|e| format!("token request failed: {e}"))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("token request returned {status}: {body}"));
    }

    let token_resp: TokenResponse = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse token response: {e}"))?;

    Ok(token_resp.access_token)
}
