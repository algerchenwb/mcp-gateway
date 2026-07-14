use serde::{Deserialize, Serialize};
use serde_json::Value;

/// A tool definition as returned by `tools/list`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tool {
    /// Unique name of the tool.
    pub name: String,
    /// Human-readable description.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// JSON Schema describing the tool's input parameters.
    #[serde(rename = "inputSchema")]
    pub input_schema: Value,
}

/// Request to invoke a tool, sent via `tools/call`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallRequest {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Value>,
}

/// The result of a tool invocation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolCallResult {
    pub content: Vec<Content>,
    #[serde(skip_serializing_if = "Option::is_none", rename = "isError")]
    pub is_error: Option<bool>,
}

/// Content types returned by a tool.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Content {
    #[serde(rename = "text")]
    TextContent {
        text: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        annotations: Option<Value>,
    },
    #[serde(rename = "image")]
    ImageContent {
        data: String,
        #[serde(rename = "mimeType")]
        mime_type: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        annotations: Option<Value>,
    },
    #[serde(rename = "resource")]
    ResourceContent {
        resource: ResourceReference,
        #[serde(skip_serializing_if = "Option::is_none")]
        annotations: Option<Value>,
    },
}

/// Reference to an embedded resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceReference {
    pub uri: String,
    #[serde(rename = "mimeType", skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub blob: Option<String>,
}

// ── Convenience constructors ──

impl ToolCallResult {
    pub fn text(text: impl Into<String>) -> Self {
        Self {
            content: vec![Content::TextContent {
                text: text.into(),
                annotations: None,
            }],
            is_error: None,
        }
    }

    pub fn error(text: impl Into<String>) -> Self {
        Self {
            content: vec![Content::TextContent {
                text: text.into(),
                annotations: None,
            }],
            is_error: Some(true),
        }
    }
}

impl Tool {
    pub fn new(name: impl Into<String>, input_schema: Value) -> Self {
        Self {
            name: name.into(),
            description: None,
            input_schema,
        }
    }

    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tool_call_result_serialization() {
        let result = ToolCallResult::text("Hello, world!");
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("\"text\":\"Hello, world!\""));
    }

    #[test]
    fn test_tool_deserialization() {
        let json = r#"{
            "name": "get_weather",
            "description": "Get current weather",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "city": {"type": "string"}
                }
            }
        }"#;
        let tool: Tool = serde_json::from_str(json).unwrap();
        assert_eq!(tool.name, "get_weather");
        assert_eq!(tool.description.unwrap(), "Get current weather");
    }
}