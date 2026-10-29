//! What the relay remembers beside the database, one lock per concern.
use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
    time::Instant,
};

use tokio::sync::Notify;

use crate::lock;

/// `(burst, per second)` of sends per sender.
pub const DEFAULT_LIMIT: (f64, f64) = (100.0, 20.0);
/// Urgent sends skip the normal budget, so they get a much smaller one: a
/// looping sender cannot flood with them.
pub const URGENT_LIMIT: (f64, f64) = (10.0, 1.0);

/// The connections listening as each name.
#[derive(Default)]
pub struct Listeners(Mutex<HashMap<String, Vec<Arc<Notify>>>>);

impl Listeners {
    /// `wake` is notified by `notify(name)` until the guard drops.
    pub fn register(
        &self,
        name: &str,
        wake: Arc<Notify>,
    ) -> Registration<'_> {
        lock(&self.0).entry(name.to_string()).or_default().push(wake.clone());
        Registration { owner: self, name: name.to_string(), wake }
    }

    /// Wakeups coalesce: any number before the listener runs cost it one pass.
    pub fn notify(
        &self,
        name: &str,
    ) {
        lock(&self.0).get(name).into_iter().flatten().for_each(|w| w.notify_one());
    }

    pub fn online(
        &self,
        name: &str,
    ) -> bool {
        lock(&self.0).get(name).is_some_and(|v| !v.is_empty())
    }
}

pub struct Registration<'a> {
    owner: &'a Listeners,
    name: String,
    wake: Arc<Notify>,
}

impl Drop for Registration<'_> {
    fn drop(&mut self) {
        let mut all = lock(&self.owner.0);
        let Some(mine) = all.get_mut(&self.name) else { return };
        mine.retain(|w| !Arc::ptr_eq(w, &self.wake));
        if mine.is_empty() {
            all.remove(&self.name);
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum State {
    Queued,
    Delivered,
    Read,
}

impl State {
    pub fn as_str(self) -> &'static str {
        match self {
            State::Queued => "queued",
            State::Delivered => "delivered",
            State::Read => "read",
        }
    }
}

#[derive(Default)]
struct Recipient {
    /// Highest id acked and not yet known to be committed.
    acked: Option<i64>,
    /// Pushed to a live listener, not yet acked.
    pushed: BTreeSet<i64>,
}

/// Acks and pushes per recipient. Both change together, hence one lock. Lost
/// on restart, which costs a redelivery or a `queued` that was `delivered`.
#[derive(Default)]
pub struct Ledger(Mutex<HashMap<String, Recipient>>);

impl Ledger {
    pub fn ack(
        &self,
        name: &str,
        id: i64,
    ) {
        let mut all = lock(&self.0);
        let r = all.entry(name.to_string()).or_default();
        r.acked = r.acked.max(Some(id));
    }

    /// The acks to commit. They stay visible until `settle`: until then a
    /// reconnecting listener must still not be offered what it acked.
    pub fn snapshot(&self) -> Vec<(String, i64)> {
        lock(&self.0).iter().filter_map(|(n, r)| Some((n.clone(), r.acked?))).collect()
    }

    /// Forgets what `done` committed, unless it was acked past since.
    pub fn settle(
        &self,
        done: &[(String, i64)],
    ) {
        let mut all = lock(&self.0);
        for (name, id) in done {
            let Some(r) = all.get_mut(name) else { continue };
            if r.acked.is_some_and(|a| a <= *id) {
                r.acked = None;
            }
            r.pushed.retain(|p| p > id);
        }
    }

    pub fn pushed(
        &self,
        name: &str,
        ids: impl IntoIterator<Item = i64>,
    ) {
        lock(&self.0).entry(name.to_string()).or_default().pushed.extend(ids);
    }

    /// The listener is gone: what was pushed to it is queued again.
    pub fn forget_pushed(
        &self,
        name: &str,
    ) {
        if let Some(r) = lock(&self.0).get_mut(name) {
            r.pushed.clear();
        }
    }

    /// For each `(recipient, id, read in the database)`.
    pub fn states(
        &self,
        rows: &[(&str, i64, bool)],
    ) -> Vec<State> {
        let all = lock(&self.0);
        rows.iter()
            .map(|(to, id, read)| {
                let r = all.get(*to);
                if *read || r.is_some_and(|r| r.acked.is_some_and(|a| a >= *id)) {
                    State::Read
                } else if r.is_some_and(|r| r.pushed.contains(id)) {
                    State::Delivered
                } else {
                    State::Queued
                }
            })
            .collect()
    }
}

/// Token buckets per sender, normal and urgent apart.
pub struct RateLimit {
    normal: (f64, f64),
    buckets: Mutex<HashMap<(String, bool), (f64, Instant)>>,
}

impl RateLimit {
    pub fn new(normal: (f64, f64)) -> Self {
        RateLimit { normal, buckets: Mutex::default() }
    }

    /// Takes one send from `name`'s budget; on refusal, the milliseconds until
    /// one is available.
    pub fn take(
        &self,
        name: &str,
        urgent: bool,
    ) -> Result<(), u64> {
        let (burst, per_sec) = if urgent { URGENT_LIMIT } else { self.normal };
        let now = Instant::now();
        let mut all = lock(&self.buckets);
        let (tokens, at) = all.entry((name.to_string(), urgent)).or_insert((burst, now));
        *tokens = (*tokens + now.duration_since(*at).as_secs_f64() * per_sec).min(burst);
        *at = now;
        if *tokens >= 1.0 {
            *tokens -= 1.0;
            return Ok(());
        }
        Err((((1.0 - *tokens) / per_sec) * 1000.0).ceil() as u64)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// One wakeup per queued send cost a database read each; a burst must cost
    /// the listener a single pass.
    #[tokio::test]
    async fn a_burst_of_notifications_wakes_the_listener_once() {
        let listeners = Listeners::default();
        let wake = Arc::new(Notify::new());
        let _reg = listeners.register("b", wake.clone());
        for _ in 0..100 {
            listeners.notify("b");
        }
        wake.notified().await;
        let second = tokio::time::timeout(Duration::from_millis(50), wake.notified()).await;
        assert!(second.is_err(), "a second wakeup was queued");
    }

    #[test]
    fn dropping_the_registration_takes_the_name_offline() {
        let listeners = Listeners::default();
        let (one, two) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
        let (a, b) = (listeners.register("x", one), listeners.register("x", two));
        drop(a);
        assert!(listeners.online("x"));
        drop(b);
        assert!(!listeners.online("x"));
    }

    /// `ack i64::MAX` made `upto + 1` overflow and panic.
    #[test]
    fn an_ack_of_the_largest_id_does_not_overflow() {
        let ledger = Ledger::default();
        ledger.pushed("b", [1, 2]);
        ledger.ack("b", i64::MAX);
        ledger.settle(&ledger.snapshot());
        assert_eq!(ledger.states(&[("b", 1, false)]), [State::Queued]);
    }

    /// Draining the acks before their commit landed let a listener that
    /// reconnected meanwhile be offered what it had already acked.
    #[test]
    fn acks_stay_visible_until_settled() {
        let ledger = Ledger::default();
        ledger.ack("b", 5);
        let taken = ledger.snapshot();
        assert_eq!(ledger.states(&[("b", 5, false)]), [State::Read], "invisible while the commit is in flight");
        assert_eq!(ledger.snapshot(), taken, "a second snapshot lost them");
        ledger.ack("b", 9);
        ledger.settle(&taken);
        assert_eq!(ledger.snapshot(), [("b".to_string(), 9)], "an ack newer than the commit was forgotten");
    }
}
