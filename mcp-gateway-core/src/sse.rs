//! Incremental UTF-8 SSE decoder shared by MCP HTTP clients.
use crate::error::{McpError, McpResult};
#[derive(Default)]
pub struct SseDecoder {
    buffer: Vec<u8>,
    event: String,
    data: Vec<String>,
    size: usize,
}
impl SseDecoder {
    pub fn push(&mut self, bytes: &[u8], limit: usize) -> McpResult<Vec<(String, String)>> {
        self.buffer.extend_from_slice(bytes);
        if self.buffer.len() + self.size > limit {
            return Err(McpError::Transport("SSE event exceeds size limit".into()));
        }
        let mut events = Vec::new();
        while let Some(end) = self.buffer.iter().position(|b| *b == b'\n') {
            let mut raw: Vec<_> = self.buffer.drain(..=end).collect();
            raw.pop();
            if raw.last() == Some(&b'\r') {
                raw.pop();
            }
            let line = String::from_utf8(raw)
                .map_err(|_| McpError::Transport("SSE is not UTF-8".into()))?;
            if line.is_empty() {
                if !self.data.is_empty() {
                    events.push((
                        if self.event.is_empty() {
                            "message".into()
                        } else {
                            std::mem::take(&mut self.event)
                        },
                        self.data.join("\n"),
                    ));
                }
                self.event.clear();
                self.data.clear();
                self.size = 0;
            } else if !line.starts_with(':') {
                let (field, value) = line.split_once(':').unwrap_or((&line, ""));
                let value = value.strip_prefix(' ').unwrap_or(value);
                match field {
                    "event" => self.event = value.to_owned(),
                    "data" => {
                        self.size += value.len();
                        self.data.push(value.to_owned());
                    }
                    _ => {}
                }
            }
        }
        Ok(events)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn handles_split_utf8_and_multiline_data() {
        let raw = "event: message\r\ndata: 你好\r\ndata: world\r\n\r\n".as_bytes();
        let mut decoder = SseDecoder::default();
        let mut events = Vec::new();
        for byte in raw {
            events.extend(decoder.push(&[*byte], 1024).unwrap());
        }
        assert_eq!(events, vec![("message".into(), "你好\nworld".into())]);
    }
}
