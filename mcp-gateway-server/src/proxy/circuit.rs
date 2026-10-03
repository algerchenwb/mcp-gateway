//! Passive transport failure circuit with a single recovery probe and stale-result isolation.
use mcp_gateway_core::error::{McpError, McpResult};
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
struct State {
    failures: u32,
    open_until: Option<Instant>,
    probing: bool,
    epoch: u64,
}
pub struct Circuit {
    state: Mutex<State>,
    threshold: u32,
    cooldown: Duration,
    name: String,
}
pub struct Attempt {
    circuit: Arc<Circuit>,
    epoch: u64,
    probe: bool,
    complete: bool,
}
impl Circuit {
    pub fn new(name: String, threshold: u32, cooldown: Duration) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                failures: 0,
                open_until: None,
                probing: false,
                epoch: 0,
            }),
            threshold,
            cooldown,
            name,
        })
    }
    pub fn available(&self) -> bool {
        let state = self.state.lock().expect("circuit lock poisoned");
        !state.probing && state.open_until.is_none_or(|until| Instant::now() >= until)
    }
    pub fn begin(self: &Arc<Self>) -> McpResult<Attempt> {
        let mut state = self.state.lock().expect("circuit lock poisoned");
        if state.probing || state.open_until.is_some_and(|until| Instant::now() < until) {
            return Err(McpError::CircuitBreakerOpen(self.name.clone()));
        }
        let probe = state.open_until.is_some();
        if probe {
            state.probing = true;
        }
        Ok(Attempt {
            circuit: self.clone(),
            epoch: state.epoch,
            probe,
            complete: false,
        })
    }
}
impl Attempt {
    pub fn finish(&mut self, failed: bool) {
        self.complete = true;
        let mut state = self.circuit.state.lock().expect("circuit lock poisoned");
        if state.epoch != self.epoch {
            return;
        }
        if failed {
            state.failures = state.failures.saturating_add(1);
            if self.probe || state.failures >= self.circuit.threshold {
                state.open_until = Some(Instant::now() + self.circuit.cooldown);
                state.probing = false;
                state.epoch += 1;
                tracing::warn!(backend = %self.circuit.name, cooldown_ms = self.circuit.cooldown.as_millis() as u64, "backend circuit opened");
            }
        } else {
            state.failures = 0;
            if self.probe {
                state.open_until = None;
                state.probing = false;
                state.epoch += 1;
                tracing::info!(backend = %self.circuit.name, "backend circuit recovered");
            }
        }
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        // Caller cancellation is not a health failure; release a cancelled probe.
        if self.probe && !self.complete {
            let mut state = self.circuit.state.lock().expect("circuit lock poisoned");
            if state.epoch == self.epoch {
                state.probing = false;
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stale_success_cannot_close_a_newly_opened_circuit() {
        let circuit = Circuit::new("a".into(), 1, Duration::from_secs(60));
        let mut stale = circuit.begin().unwrap();
        circuit.begin().unwrap().finish(true);
        stale.finish(false);
        assert!(!circuit.available());
        assert!(circuit.begin().is_err());
    }
    #[tokio::test]
    async fn only_one_probe_runs_and_cancellation_releases_it_without_marking_healthy() {
        let circuit = Circuit::new("a".into(), 1, Duration::from_millis(10));
        circuit.begin().unwrap().finish(true);
        tokio::time::sleep(Duration::from_millis(20)).await;
        let probe = circuit.begin().unwrap();
        assert!(!circuit.available());
        assert!(circuit.begin().is_err());
        drop(probe);
        assert!(circuit.available());
        circuit.begin().unwrap().finish(false);
        assert!(circuit.begin().is_ok());
    }
}
