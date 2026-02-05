use std::time::Duration;

use ::reqwest;
use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

use super::TripleStoreBackend;
use crate::managers::triple_store::{
    config::{DKG_REPOSITORY, TripleStoreManagerConfig},
    error::{Result, TripleStoreError},
};

/// Blazegraph triple store backend implementation
pub(crate) struct BlazegraphBackend {
    client: Client,
    config: TripleStoreManagerConfig,
}

impl BlazegraphBackend {
    /// Create a new Blazegraph backend
    pub(crate) fn new(config: TripleStoreManagerConfig) -> Result<Self> {
        let client = Client::builder()
            // Connection pooling: keep up to 10 idle connections per host
            .pool_max_idle_per_host(10)
            // Close idle connections after 30 seconds
            .pool_idle_timeout(Duration::from_secs(30))
            // TCP keepalive to detect dead connections
            .tcp_keepalive(Duration::from_secs(60))
            // Timeout for establishing new connections
            .connect_timeout(Duration::from_secs(10))
            // Default request timeout (overridden per-request)
            .timeout(Duration::from_millis(config.timeouts.query_ms))
            .build()?;

        Ok(Self { client, config })
    }

    /// Build request with optional authentication
    fn auth_headers(&self, builder: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match (&self.config.username, &self.config.password) {
            (Some(user), Some(pass)) => builder.basic_auth(user, Some(pass)),
            _ => builder,
        }
    }

    /// Decode Blazegraph's escaped Unicode sequences (\Uxxxxxxxx)
    fn decode_unicode_escapes(input: &str) -> String {
        let mut result = String::with_capacity(input.len());
        let mut chars = input.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.peek() {
                    Some('U') => {
                        chars.next(); // consume 'U'
                        let hex: String = chars.by_ref().take(8).collect();
                        if hex.len() == 8
                            && let Ok(code) = u32::from_str_radix(&hex, 16)
                            && let Some(decoded) = char::from_u32(code)
                        {
                            result.push(decoded);
                            continue;
                        }
                        // Fallback: keep original
                        result.push('\\');
                        result.push('U');
                        result.push_str(&hex);
                    }
                    Some('u') => {
                        chars.next(); // consume 'u'
                        let hex: String = chars.by_ref().take(4).collect();
                        if hex.len() == 4
                            && let Ok(code) = u32::from_str_radix(&hex, 16)
                            && let Some(decoded) = char::from_u32(code)
                        {
                            result.push(decoded);
                            continue;
                        }
                        // Fallback: keep original
                        result.push('\\');
                        result.push('u');
                        result.push_str(&hex);
                    }
                    _ => result.push(c),
                }
            } else {
                result.push(c);
            }
        }

        result
    }
}

#[async_trait]
impl TripleStoreBackend for BlazegraphBackend {
    fn name(&self) -> &'static str {
        "blazegraph"
    }

    async fn health_check(&self) -> Result<bool> {
        let url = self.config.status_endpoint();

        let response = self
            .auth_headers(self.client.get(&url))
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        Ok(response.status().is_success())
    }

    async fn repository_exists(&self) -> Result<bool> {
        // Check if namespace exists by querying its properties endpoint
        // This matches the JS implementation which checks /blazegraph/namespace/{name}/properties
        let url = format!(
            "{}/{}/properties?describe-each-named-graph=false",
            self.config.namespace_endpoint(),
            DKG_REPOSITORY
        );

        let response = self
            .auth_headers(self.client.get(&url))
            .header("Accept", "application/ld+json")
            .timeout(Duration::from_secs(10))
            .send()
            .await?;

        // 404 means namespace doesn't exist, success means it does
        if response.status().as_u16() == 404 {
            Ok(false)
        } else {
            Ok(response.status().is_success())
        }
    }

    async fn create_repository(&self) -> Result<()> {
        let url = self.config.namespace_endpoint();
        let name = DKG_REPOSITORY;

        // Blazegraph namespace configuration properties (Java properties format)
        // These settings match the JS ot-node implementation exactly
        let properties = format!(
            "com.bigdata.rdf.sail.truthMaintenance=false\n\
             com.bigdata.namespace.{name}.spo.com.bigdata.btree.BTree.branchingFactor=1024\n\
             com.bigdata.rdf.store.AbstractTripleStore.textIndex=false\n\
             com.bigdata.rdf.store.AbstractTripleStore.justify=false\n\
             com.bigdata.rdf.store.AbstractTripleStore.statementIdentifiers=false\n\
             com.bigdata.rdf.store.AbstractTripleStore.axiomsClass=com.bigdata.rdf.axioms.NoAxioms\n\
             com.bigdata.rdf.sail.namespace={name}\n\
             com.bigdata.rdf.store.AbstractTripleStore.quads=true\n\
             com.bigdata.namespace.{name}.lex.com.bigdata.btree.BTree.branchingFactor=400\n\
             com.bigdata.rdf.store.AbstractTripleStore.geoSpatial=false\n\
             com.bigdata.journal.Journal.groupCommit=false\n\
             com.bigdata.rdf.sail.isolatableIndices=false\n\
             com.bigdata.rdf.store.AbstractTripleStore.enableRawRecordsSupport=false\n\
             com.bigdata.rdf.store.AbstractTripleStore.Options.inlineTextLiterals=true\n\
             com.bigdata.rdf.store.AbstractTripleStore.Options.maxInlineTextLength=128\n\
             com.bigdata.rdf.store.AbstractTripleStore.Options.blobsThreshold=256\n"
        );

        // Blazegraph expects properties as text/plain body
        let response = self
            .auth_headers(self.client.post(&url))
            .header("Content-Type", "text/plain")
            .body(properties)
            .timeout(Duration::from_secs(30))
            .send()
            .await?;

        if response.status().is_success() || response.status().as_u16() == 201 {
            tracing::info!(
                repository = %DKG_REPOSITORY,
                "Created Blazegraph namespace"
            );
            Ok(())
        } else {
            let status = response.status().as_u16();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(TripleStoreError::Backend { status, message })
        }
    }

    #[cfg(test)]
    async fn delete_repository(&self) -> Result<()> {
        let url = format!("{}/{}", self.config.namespace_endpoint(), DKG_REPOSITORY);

        let response = self
            .auth_headers(self.client.delete(&url))
            .timeout(Duration::from_secs(30))
            .send()
            .await?;

        if response.status().is_success() {
            tracing::info!(
                repository = %DKG_REPOSITORY,
                "Deleted Blazegraph namespace"
            );
            Ok(())
        } else {
            let status = response.status().as_u16();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(TripleStoreError::Backend { status, message })
        }
    }

    async fn construct(&self, query: &str, timeout: Duration) -> Result<String> {
        let url = self.config.sparql_endpoint();
        let timeout_ms = timeout.as_millis();

        let response = self
            .auth_headers(self.client.post(&url))
            .header("Content-Type", "application/sparql-query")
            .header("Accept", "application/n-quads")
            .header("X-BIGDATA-MAX-QUERY-MILLIS", timeout_ms.to_string())
            .timeout(timeout + Duration::from_secs(5))
            .body(query.to_string())
            .send()
            .await?;

        if response.status().is_success() {
            let body = response.text().await?;
            Ok(Self::decode_unicode_escapes(&body))
        } else {
            let status = response.status().as_u16();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(TripleStoreError::Backend { status, message })
        }
    }

    async fn update(&self, query: &str, timeout: Duration) -> Result<()> {
        let url = self.config.sparql_endpoint();
        let timeout_ms = timeout.as_millis();

        let response = self
            .auth_headers(self.client.post(&url))
            .header("Content-Type", "application/sparql-update")
            .header("X-BIGDATA-MAX-QUERY-MILLIS", timeout_ms.to_string())
            .timeout(timeout + Duration::from_secs(5))
            .body(query.to_string())
            .send()
            .await?;

        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status().as_u16();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(TripleStoreError::Backend { status, message })
        }
    }

    async fn ask(&self, query: &str, timeout: Duration) -> Result<bool> {
        let url = self.config.sparql_endpoint();
        let timeout_ms = timeout.as_millis();

        let response = self
            .auth_headers(self.client.post(&url))
            .header("Content-Type", "application/sparql-query")
            .header("Accept", "application/sparql-results+json")
            .header("X-BIGDATA-MAX-QUERY-MILLIS", timeout_ms.to_string())
            .timeout(timeout + Duration::from_secs(5))
            .body(query.to_string())
            .send()
            .await?;

        if response.status().is_success() {
            let body = response.text().await?;
            parse_ask_json(&body)
        } else {
            let status = response.status().as_u16();
            let message = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            Err(TripleStoreError::Backend { status, message })
        }
    }
}

#[derive(Deserialize)]
struct SparqlAskResponse {
    boolean: bool,
}

fn parse_ask_json(json: &str) -> Result<bool> {
    let response: SparqlAskResponse =
        serde_json::from_str(json).map_err(|e| TripleStoreError::ParseError {
            reason: format!("Failed to parse ASK response: {e}"),
        })?;

    Ok(response.boolean)
}
