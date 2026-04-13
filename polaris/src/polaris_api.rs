use serde::Deserialize;

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableFormat {
    Iceberg,
    Delta,
}

#[derive(Debug, Deserialize)]
pub struct GenericTableResponse {
    pub table: GenericTable,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub struct GenericTable {
    pub format: TableFormat,
    pub name: String,
    pub base_location: Option<String>,
}

/// Load table metadata from the Polaris Generic Tables API.
pub async fn load_generic_table(
    client: &reqwest::Client,
    base_url: &str,
    warehouse: &str,
    namespace: &str,
    table_name: &str,
) -> Result<GenericTable, String> {
    let url = format!(
        "{base_url}/api/catalog/polaris/v1/{warehouse}/namespaces/{namespace}/generic-tables/{table_name}"
    );
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| format!("generic table request failed: {e}"))?;

    if resp.status().as_u16() == 404 {
        return Err("table_not_found".to_string());
    }

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(format!("generic table request returned {status}: {body}"));
    }

    let wrapper: GenericTableResponse = resp
        .json()
        .await
        .map_err(|e| format!("failed to parse generic table response: {e}"))?;

    Ok(wrapper.table)
}

/// Detect the format of a table. First tries the Generic Tables API;
/// if the table is not found there, assumes Iceberg (standard catalog).
pub async fn detect_format(
    client: &reqwest::Client,
    base_url: &str,
    warehouse: &str,
    namespace: &str,
    table_name: &str,
) -> TableFormat {
    match load_generic_table(client, base_url, warehouse, namespace, table_name).await {
        Ok(t) => t.format,
        Err(e) if e == "table_not_found" => TableFormat::Iceberg,
        Err(_) => TableFormat::Iceberg, // default fallback
    }
}
