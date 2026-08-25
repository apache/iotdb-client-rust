// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Session pools (protocol spec §7): a bounded set of open sessions,
//! eagerly grown to `min_size` and lazily to `max_size`, handed out as
//! RAII guards. Round-robin endpoint spread happens naturally via the
//! rotating start index in [`Session::open`].

use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::client::session::{Session, SessionConfig};
use crate::error::{Error, Result};

/// Configuration for a [`SessionPool`] (or [`TableSessionPool`]).
///
/// `session` carries the per-connection settings — node urls,
/// user/password, dialect/database, connect timeout — shared by every
/// pooled session.
#[derive(Debug, Clone)]
pub struct SessionPoolConfig {
    pub session: SessionConfig,
    /// Upper bound on live sessions (idle + handed out). Default 8,
    /// matching the C# SDK's pool size.
    pub max_size: usize,
    /// Sessions opened eagerly when the pool is created. Default 0.
    pub min_size: usize,
    /// How long [`SessionPool::acquire`] waits for an idle session once the
    /// pool is at `max_size`. Default 60 s (Node.js `waitTimeout`).
    pub acquire_timeout: Duration,
    /// Idle sessions unused for longer than this are closed by the lazy
    /// sweep (never shrinking the pool below `min_size`). Default 60 s
    /// (Node.js `maxIdleTime`).
    pub max_idle_time: Duration,
    /// Minimum time between idle sweeps. There is **no** timer thread —
    /// the sweep runs lazily on pool activity (acquire/release) once this
    /// much time has passed since the previous sweep, so a completely idle
    /// pool keeps its sessions until the next acquire. Default 30 s
    /// (Node.js sweep interval).
    pub idle_sweep_interval: Duration,
}

impl Default for SessionPoolConfig {
    fn default() -> Self {
        Self {
            session: SessionConfig::default(),
            max_size: 8,
            min_size: 0,
            acquire_timeout: Duration::from_secs(60),
            max_idle_time: Duration::from_secs(60),
            idle_sweep_interval: Duration::from_secs(30),
        }
    }
}

impl SessionPoolConfig {
    /// Set endpoints from `"host:port"` node-url strings.
    pub fn with_node_urls<S: AsRef<str>>(mut self, node_urls: &[S]) -> Result<Self> {
        self.session = self.session.with_node_urls(node_urls)?;
        Ok(self)
    }
}

/// An idle session together with the moment it went idle, so the sweep
/// can measure how long it has been unused.
struct IdleEntry {
    session: Session,
    since: Instant,
}

impl IdleEntry {
    fn new(session: Session) -> Self {
        Self {
            session,
            since: Instant::now(),
        }
    }
}

/// Idle sessions plus the pool lifecycle flag, guarded by one mutex so
/// [`Condvar`] waiters observe both consistently. `last_sweep` gates the
/// lazy idle sweep (see [`SessionPoolConfig::idle_sweep_interval`]).
struct PoolState {
    idle: VecDeque<IdleEntry>,
    closed: bool,
    last_sweep: Instant,
}

/// A pool of open tree-model [`Session`]s.
///
/// Sessions are created lazily up to `max_size` (after the eager
/// `min_size`); [`SessionPool::acquire`] blocks up to `acquire_timeout`
/// when the pool is exhausted. Dead sessions are discarded on acquire and
/// on release; sessions idle longer than `max_idle_time` are closed by a
/// lazy sweep that runs on pool activity (no timer thread — see
/// [`SessionPoolConfig::idle_sweep_interval`]), never shrinking the pool
/// below `min_size`. The pool tracks the most recent `USE <db>` seen on any
/// released session and replays it on acquire so every handed-out session
/// is in the pool's current database (spec §6.2).
pub struct SessionPool {
    config: SessionPoolConfig,
    state: Mutex<PoolState>,
    available: Condvar,
    /// Sessions alive right now: idle + handed out. Only mutated while
    /// holding `state`, so `live < max_size` checks cannot overshoot.
    live: AtomicUsize,
    /// Pool-level current database, updated from released sessions.
    database: Mutex<Option<String>>,
}

impl SessionPool {
    /// Create the pool and eagerly open `min_size` sessions.
    pub fn new(config: SessionPoolConfig) -> Result<SessionPool> {
        if config.min_size > config.max_size {
            return Err(Error::Client(format!(
                "pool min_size ({}) > max_size ({})",
                config.min_size, config.max_size
            )));
        }
        let database = config.session.database.clone();
        let pool = SessionPool {
            config,
            state: Mutex::new(PoolState {
                idle: VecDeque::new(),
                closed: false,
                last_sweep: Instant::now(),
            }),
            available: Condvar::new(),
            live: AtomicUsize::new(0),
            database: Mutex::new(database),
        };
        for _ in 0..pool.config.min_size {
            let session = pool.open_session()?;
            let mut state = pool.state.lock().expect("pool lock poisoned");
            state.idle.push_back(IdleEntry::new(session));
            pool.live.fetch_add(1, Ordering::Relaxed);
        }
        Ok(pool)
    }

    /// Sessions alive right now (idle + handed out).
    pub fn live_count(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    /// Sessions sitting idle in the pool right now.
    pub fn idle_count(&self) -> usize {
        self.state.lock().expect("pool lock poisoned").idle.len()
    }

    /// Lazy idle sweep, run at acquire/release time (there is no timer
    /// thread — a completely inactive pool is only swept on its next use).
    /// No-op until `idle_sweep_interval` has elapsed since the last sweep;
    /// then removes sessions idle longer than `max_idle_time`, oldest
    /// first, never shrinking the pool (idle + handed out) below
    /// `min_size`. Returns the expired sessions — the caller closes them
    /// **after** dropping the state lock, so a slow `closeSession` RPC
    /// cannot stall other pool users.
    #[must_use]
    fn sweep_idle(&self, state: &mut PoolState) -> Vec<Session> {
        let now = Instant::now();
        if now.duration_since(state.last_sweep) < self.config.idle_sweep_interval {
            return Vec::new();
        }
        state.last_sweep = now;
        let mut expired = Vec::new();
        let mut i = 0;
        while i < state.idle.len() {
            if self.live.load(Ordering::Relaxed) - expired.len() <= self.config.min_size {
                break;
            }
            if now.duration_since(state.idle[i].since) > self.config.max_idle_time {
                expired.push(state.idle.remove(i).expect("index in bounds").session);
            } else {
                i += 1;
            }
        }
        if !expired.is_empty() {
            self.live.fetch_sub(expired.len(), Ordering::Relaxed);
            log::debug!("idle sweep evicted {} session(s)", expired.len());
        }
        expired
    }

    /// Acquire a session, blocking up to `acquire_timeout` when the pool is
    /// at capacity with nothing idle. Dead idle sessions are discarded and
    /// the acquire retried; a fresh session is opened while under
    /// `max_size`.
    pub fn acquire(&self) -> Result<PooledSession<'_>> {
        // `checked_add` makes `acquire_timeout = Duration::MAX` mean
        // "wait without a deadline" instead of panicking on Instant overflow.
        let deadline = Instant::now().checked_add(self.config.acquire_timeout);
        let mut last_err: Option<Error> = None;
        let mut state = self.state.lock().expect("pool lock poisoned");
        loop {
            if state.closed {
                return Err(Error::Client("session pool is closed".into()));
            }
            if let Some(deadline) = deadline {
                if Instant::now() >= deadline {
                    return Err(last_err.unwrap_or_else(|| {
                        Error::Client(format!(
                            "pool exhausted: no session available within {:?} ({} live, max {})",
                            self.config.acquire_timeout,
                            self.live.load(Ordering::Relaxed),
                            self.config.max_size
                        ))
                    }));
                }
            }
            let expired = self.sweep_idle(&mut state);
            if !expired.is_empty() {
                drop(state);
                for mut session in expired {
                    let _ = session.close();
                }
                state = self.state.lock().expect("pool lock poisoned");
                continue; // re-check closed/idle after re-locking
            }
            // Idle session available → validate liveness, evict the dead.
            if let Some(entry) = state.idle.pop_front() {
                if entry.session.is_open() {
                    drop(state);
                    match self.hand_out(entry.session) {
                        Ok(guard) => return Ok(guard),
                        Err(e) => {
                            // Account under the lock, then keep spending the
                            // acquire_timeout budget on the remaining idle
                            // candidates instead of failing instantly.
                            last_err = Some(e);
                            state = self.state.lock().expect("pool lock poisoned");
                            self.live.fetch_sub(1, Ordering::Relaxed);
                            self.available.notify_one();
                            continue;
                        }
                    }
                }
                self.live.fetch_sub(1, Ordering::Relaxed);
                continue; // dead session discarded; retry
            }
            // Below capacity → grow lazily. Count the slot while still
            // holding the lock so concurrent acquires cannot overshoot.
            if self.live.load(Ordering::Relaxed) < self.config.max_size {
                self.live.fetch_add(1, Ordering::Relaxed);
                drop(state);
                match self.open_session() {
                    Ok(session) => match self.hand_out(session) {
                        Ok(guard) => return Ok(guard),
                        Err(e) => {
                            let state = self.state.lock().expect("pool lock poisoned");
                            self.live.fetch_sub(1, Ordering::Relaxed);
                            self.available.notify_one();
                            drop(state);
                            // A broken USE replay is a terminal error for
                            // this acquire: report it directly.
                            return Err(e);
                        }
                    },
                    Err(e) => {
                        last_err = Some(e);
                        state = self.state.lock().expect("pool lock poisoned");
                        self.live.fetch_sub(1, Ordering::Relaxed);
                        self.available.notify_one();
                        // Spend the remaining acquire_timeout budget waiting
                        // for a release instead of failing immediately; the
                        // loop re-checks closed/idle/deadline at the top.
                        state = self.wait_for_change(state, deadline);
                    }
                }
                continue;
            }
            // At capacity → wait for a release, bounded by the deadline.
            state = self.wait_for_change(state, deadline);
        }
    }

    /// Park on the availability Condvar until a release or (when a deadline
    /// is set) the deadline passes. The returned guard still holds the state
    /// lock so the caller re-evaluates the predicate safely.
    fn wait_for_change<'a>(
        &self,
        state: std::sync::MutexGuard<'a, PoolState>,
        deadline: Option<Instant>,
    ) -> std::sync::MutexGuard<'a, PoolState> {
        match deadline {
            Some(deadline) => {
                let now = Instant::now();
                if now >= deadline {
                    return state;
                }
                let (guard, _) = self
                    .available
                    .wait_timeout(state, deadline - now)
                    .expect("pool lock poisoned");
                guard
            }
            None => self.available.wait(state).expect("pool lock poisoned"),
        }
    }

    /// Acquire a session for writes to `device_id`, preferring an idle
    /// session already connected to the device's redirected endpoint.
    ///
    /// A status-400 insert response leaves a device → endpoint hint in the
    /// session that saw it ([`Session::redirect_hint`]); if any idle
    /// session carries such a hint **and** another idle session is
    /// connected to that endpoint, that session is handed out. In every
    /// other case — no hint, no matching idle session, hint expired — this
    /// is exactly [`SessionPool::acquire`]. The pool never opens a new
    /// connection to the hinted endpoint (Node.js-style dedicated
    /// per-endpoint sessions are future work).
    pub fn acquire_for_device(&self, device_id: &str) -> Result<PooledSession<'_>> {
        let matched = {
            let mut state = self.state.lock().expect("pool lock poisoned");
            if state.closed {
                None
            } else {
                // Any idle session may hold the hint (the one that got the
                // 400), not necessarily one connected to the hinted node.
                // When several sessions hold conflicting hints, prefer the
                // newest (highest cache seq).
                let hint = state
                    .idle
                    .iter_mut()
                    .filter_map(|e| e.session.redirect_hint_with_seq(device_id))
                    .max_by_key(|(_, seq)| *seq);
                match hint {
                    Some((endpoint, _)) => state
                        .idle
                        .iter()
                        .position(|e| {
                            e.session.is_open()
                                && e.session
                                    .current_endpoint()
                                    .is_some_and(|current| current.equivalent(&endpoint))
                        })
                        .map(|pos| state.idle.remove(pos).expect("index in bounds").session),
                    None => None,
                }
            }
        };
        if let Some(session) = matched {
            match self.hand_out(session) {
                Ok(guard) => return Ok(guard),
                Err(_) => {
                    // Account under the lock, then fall back to the normal
                    // acquire path (the hinted hand-out may still succeed).
                    let state = self.state.lock().expect("pool lock poisoned");
                    self.live.fetch_sub(1, Ordering::Relaxed);
                    self.available.notify_one();
                    drop(state);
                }
            }
        }
        self.acquire()
    }

    /// Convenience: acquire a session, run one non-query statement, release.
    /// `USE <db>` propagates to the whole pool via the database tracking.
    pub fn execute_non_query(&self, sql: &str) -> Result<()> {
        self.acquire()?.execute_non_query(sql)
    }

    /// Close the pool: no further acquires; drain and close all idle
    /// sessions. Sessions currently handed out are closed when their guards
    /// drop.
    pub fn close(&self) {
        // Set the flag, decrement `live` and wake every waiter under the
        // same lock the waiters re-check the predicate under (no lost
        // Condvar wakeups); the blocking closeSession RPCs run after the
        // lock is released, bounded by the session's socket_timeout.
        let drained = {
            let mut state = self.state.lock().expect("pool lock poisoned");
            state.closed = true;
            self.live.fetch_sub(state.idle.len(), Ordering::Relaxed);
            self.available.notify_all();
            std::mem::take(&mut state.idle)
        };
        for mut entry in drained {
            let _ = entry.session.close();
        }
    }

    fn open_session(&self) -> Result<Session> {
        let mut config = self.config.session.clone();
        // New sessions start in the pool's current database (config key
        // "db"), so no catch-up USE is needed on first hand-out.
        config.database = self.database.lock().expect("pool lock poisoned").clone();
        let mut session = Session::new(config);
        session.open()?;
        Ok(session)
    }

    /// Final step of acquire: sync the session onto the pool's current
    /// database before handing it out. On USE failure the session is
    /// discarded, not returned to the pool; the caller re-takes the state
    /// lock to decrement `live` and notify, keeping the decrement and the
    /// predicate waiters re-evaluate under the same lock.
    fn hand_out(&self, mut session: Session) -> Result<PooledSession<'_>> {
        let pool_db = self.database.lock().expect("pool lock poisoned").clone();
        if let Some(db) = pool_db {
            if session.database() != Some(db.as_str()) {
                if let Err(e) = session.execute_non_query(&format!("USE {db}")) {
                    let _ = session.close();
                    return Err(e);
                }
            }
        }
        // Pooled sessions skip the reconnect pacing sleeps so a pool slot is
        // never held for the full C#-style reconnect walk.
        session.mark_pooled();
        Ok(PooledSession {
            pool: self,
            session: Some(session),
        })
    }

    /// Return a session from a dropped guard. Dead sessions are discarded;
    /// live ones update the pool database and go back to the idle queue.
    fn release(&self, session: Session) {
        if !session.is_open() {
            let state = self.state.lock().expect("pool lock poisoned");
            self.live.fetch_sub(1, Ordering::Relaxed);
            self.available.notify_one();
            drop(state);
            return;
        }
        if let Some(db) = session.database() {
            let mut pool_db = self.database.lock().expect("pool lock poisoned");
            if pool_db.as_deref() != Some(db) {
                *pool_db = Some(db.to_string());
            }
        }
        let mut state = self.state.lock().expect("pool lock poisoned");
        if state.closed {
            self.live.fetch_sub(1, Ordering::Relaxed);
            self.available.notify_one();
            drop(state);
            let mut session = session;
            let _ = session.close();
        } else {
            state.idle.push_back(IdleEntry::new(session));
            let expired = self.sweep_idle(&mut state);
            self.available.notify_one();
            drop(state);
            for mut session in expired {
                let _ = session.close();
            }
        }
    }

    /// Test hook: push a pre-built session (possibly dead) into the idle
    /// queue, counting it as live.
    #[cfg(test)]
    fn inject_idle(&self, session: Session) {
        let mut state = self.state.lock().expect("pool lock poisoned");
        state.idle.push_back(IdleEntry::new(session));
        self.live.fetch_add(1, Ordering::Relaxed);
    }
}

impl Drop for SessionPool {
    fn drop(&mut self) {
        self.close();
    }
}

/// RAII guard for a pooled [`Session`]. Derefs to the session; returns it
/// to the pool on drop (dead sessions are discarded instead).
pub struct PooledSession<'a> {
    pool: &'a SessionPool,
    session: Option<Session>,
}

impl std::fmt::Debug for PooledSession<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledSession")
            .field("open", &self.session.as_ref().is_some_and(Session::is_open))
            .finish_non_exhaustive()
    }
}

impl Deref for PooledSession<'_> {
    type Target = Session;

    fn deref(&self) -> &Session {
        self.session.as_ref().expect("session taken")
    }
}

impl DerefMut for PooledSession<'_> {
    fn deref_mut(&mut self) -> &mut Session {
        self.session.as_mut().expect("session taken")
    }
}

impl Drop for PooledSession<'_> {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            self.pool.release(session);
        }
    }
}

/// A pool of table-dialect sessions — a [`SessionPool`] whose sessions are
/// opened with `sql_dialect="table"` (and optionally a database), per
/// protocol spec §6. `USE <db>` on any pooled session propagates pool-wide
/// on release.
pub struct TableSessionPool {
    pool: SessionPool,
}

impl TableSessionPool {
    /// Create the pool, forcing the table dialect on the session config.
    pub fn new(mut config: SessionPoolConfig) -> Result<TableSessionPool> {
        config.session.sql_dialect = "table".into();
        Ok(TableSessionPool {
            pool: SessionPool::new(config)?,
        })
    }

    /// Acquire a table-dialect session guard.
    pub fn acquire(&self) -> Result<PooledSession<'_>> {
        self.pool.acquire()
    }

    /// Acquire a table-dialect session for writes to `device_id`,
    /// preferring an idle session already connected to the device's
    /// redirected endpoint — the table-model counterpart of
    /// [`SessionPool::acquire_for_device`].
    pub fn acquire_for_device(&self, device_id: &str) -> Result<PooledSession<'_>> {
        self.pool.acquire_for_device(device_id)
    }

    /// Convenience: acquire, run one non-query statement, release.
    pub fn execute_non_query(&self, sql: &str) -> Result<()> {
        self.pool.execute_non_query(sql)
    }

    pub fn live_count(&self) -> usize {
        self.pool.live_count()
    }

    pub fn idle_count(&self) -> usize {
        self.pool.idle_count()
    }

    pub fn close(&self) {
        self.pool.close()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::Endpoint;
    use std::net::TcpStream;

    /// Endpoint nothing listens on: connects are refused ~immediately.
    fn dead_endpoint_config() -> SessionConfig {
        SessionConfig {
            endpoints: vec![Endpoint::new("127.0.0.1", 1)],
            connect_timeout: Duration::from_millis(200),
            ..SessionConfig::default()
        }
    }

    fn live_server_available() -> bool {
        TcpStream::connect_timeout(
            &"127.0.0.1:6667".parse().unwrap(),
            Duration::from_millis(300),
        )
        .is_ok()
    }

    #[test]
    fn config_defaults() {
        let cfg = SessionPoolConfig::default();
        assert_eq!(cfg.max_size, 8);
        assert_eq!(cfg.min_size, 0);
        assert_eq!(cfg.acquire_timeout, Duration::from_secs(60));
        assert_eq!(cfg.max_idle_time, Duration::from_secs(60));
        assert_eq!(cfg.idle_sweep_interval, Duration::from_secs(30));
    }

    /// F5: a failed growth attempt must spend the acquire_timeout budget
    /// (waiting for a release) instead of failing instantly — the old
    /// behaviour returned the connect error in ~200us against a 300ms budget.
    #[test]
    fn growth_failure_spends_acquire_timeout_budget() {
        let cfg = SessionPoolConfig {
            max_size: 1,
            acquire_timeout: Duration::from_millis(300),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();
        let started = Instant::now();
        match pool.acquire() {
            Err(Error::Thrift(_)) => {}
            other => panic!("expected thrift connect error, got {other:?}"),
        }
        let elapsed = started.elapsed();
        assert!(
            elapsed >= Duration::from_millis(250),
            "acquire failed in {elapsed:?} instead of spending the budget"
        );
        assert_eq!(pool.live_count(), 0);
    }

    /// F6: waiters blocked on a full pool must be woken by `close()`
    /// promptly — the wakeup cannot depend on the blocking closeSession RPCs
    /// that run afterwards (the old code notified only after them).
    #[test]
    fn close_wakes_waiters_promptly() {
        let ep = fake_listener();
        let cfg = SessionPoolConfig {
            max_size: 1,
            acquire_timeout: Duration::from_secs(10),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = std::sync::Arc::new(SessionPool::new(cfg).unwrap());
        pool.inject_idle(injected_session(&ep));
        let _held = pool.acquire().unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        let waiter_pool = std::sync::Arc::clone(&pool);
        let waiter = std::thread::spawn(move || {
            let _ = tx.send(waiter_pool.acquire().map(|_| ()));
        });
        std::thread::sleep(Duration::from_millis(100));
        pool.close();
        match rx.recv_timeout(Duration::from_secs(2)) {
            Ok(Err(Error::Client(msg))) => assert!(msg.contains("closed"), "{msg}"),
            other => panic!("waiter should get the closed-pool error promptly, got {other:?}"),
        }
        waiter.join().expect("waiter thread");
    }

    /// F13: `acquire_timeout = Duration::MAX` must mean "wait without a
    /// deadline", not panic on `Instant + Duration` overflow. The close
    /// below is what releases the waiter.
    #[test]
    fn acquire_timeout_max_waits_until_closed_without_panicking() {
        let ep = fake_listener();
        let cfg = SessionPoolConfig {
            max_size: 1,
            acquire_timeout: Duration::MAX,
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = std::sync::Arc::new(SessionPool::new(cfg).unwrap());
        pool.inject_idle(injected_session(&ep));
        let _held = pool.acquire().unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        let waiter_pool = std::sync::Arc::clone(&pool);
        let waiter = std::thread::spawn(move || {
            let _ = tx.send(waiter_pool.acquire().map(|_| ()));
        });
        std::thread::sleep(Duration::from_millis(100));
        pool.close();
        match rx.recv_timeout(Duration::from_secs(2)) {
            Ok(Err(Error::Client(msg))) => assert!(msg.contains("closed"), "{msg}"),
            other => panic!("Duration::MAX waiter should wake on close, got {other:?}"),
        }
        waiter.join().expect("waiter thread");
    }

    #[test]
    fn min_greater_than_max_is_rejected() {
        let cfg = SessionPoolConfig {
            min_size: 9,
            max_size: 8,
            ..Default::default()
        };
        assert!(SessionPool::new(cfg).is_err());
    }

    #[test]
    fn exhausted_pool_times_out_with_typed_error() {
        // max_size 0: nothing idle, no growth allowed → deadline error.
        let cfg = SessionPoolConfig {
            max_size: 0,
            acquire_timeout: Duration::from_millis(50),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();
        let start = Instant::now();
        match pool.acquire() {
            Err(Error::Client(msg)) => assert!(msg.contains("pool exhausted"), "{msg}"),
            other => panic!("expected pool-exhausted error, got {other:?}"),
        }
        assert!(start.elapsed() >= Duration::from_millis(50));
    }

    #[test]
    fn dead_idle_sessions_are_evicted_on_acquire() {
        let cfg = SessionPoolConfig {
            max_size: 1,
            acquire_timeout: Duration::from_millis(50),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();
        // A never-opened session is dead (is_open() == false).
        pool.inject_idle(Session::new(dead_endpoint_config()));
        assert_eq!(pool.live_count(), 1);

        // Acquire evicts the dead session, then tries to open a fresh one,
        // which fails against the dead endpoint — a connect error, NOT
        // "pool exhausted".
        match pool.acquire() {
            Err(Error::Thrift(_)) => {}
            other => panic!("expected thrift connect error, got {other:?}"),
        }
        // The dead session and the failed growth slot are both released.
        assert_eq!(pool.live_count(), 0);
    }

    #[test]
    fn acquire_on_closed_pool_fails() {
        let cfg = SessionPoolConfig {
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();
        pool.close();
        match pool.acquire() {
            Err(Error::Client(msg)) => assert!(msg.contains("closed"), "{msg}"),
            other => panic!("expected closed-pool error, got {other:?}"),
        };
    }

    #[test]
    fn releasing_dead_session_shrinks_live_count() {
        let cfg = SessionPoolConfig {
            max_size: 1,
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();
        // Simulate a handed-out session dying before release.
        pool.live.fetch_add(1, Ordering::Relaxed);
        pool.release(Session::new(dead_endpoint_config()));
        assert_eq!(pool.live_count(), 0);
        let state = pool.state.lock().unwrap();
        assert!(state.idle.is_empty());
    }

    #[test]
    fn table_pool_forces_table_dialect() {
        let cfg = SessionPoolConfig {
            session: SessionConfig {
                sql_dialect: "tree".into(), // deliberately wrong
                ..dead_endpoint_config()
            },
            ..Default::default()
        };
        let pool = TableSessionPool::new(cfg).unwrap();
        assert_eq!(pool.pool.config.session.sql_dialect, "table");
    }

    /// A local listener that accepts and immediately drops connections:
    /// `Connection::open` succeeds (giving pool tests real TCP connections
    /// with distinct endpoints) while any RPC on them — like the swallowed
    /// `closeSession` at pool teardown — fails fast on EOF instead of
    /// blocking on a reply that never comes.
    fn fake_listener() -> Endpoint {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("local_addr").port();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                match stream {
                    Ok(s) => drop(s),
                    Err(_) => break,
                }
            }
        });
        Endpoint::new("127.0.0.1", port)
    }

    fn injected_session(endpoint: &Endpoint) -> Session {
        let mut session = Session::new(dead_endpoint_config());
        let connection = crate::connection::Connection::open(
            endpoint.clone(),
            &crate::connection::ConnectionOptions {
                connect_timeout: Duration::from_millis(500),
                ..Default::default()
            },
        )
        .expect("connect to test listener");
        session.test_inject_connection(connection);
        session
    }

    #[test]
    fn acquire_for_device_prefers_hinted_endpoint() {
        let ep_a = fake_listener();
        let ep_b = fake_listener();

        let cfg = SessionPoolConfig {
            max_size: 4,
            acquire_timeout: Duration::from_millis(50),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();

        // s1 (connected to A) holds the redirect hint pointing at B;
        // s2 is connected to B — the hinted target.
        let mut s1 = injected_session(&ep_a);
        s1.test_inject_redirect_hint("root.sg.d1", ep_b.clone());
        let s2 = injected_session(&ep_b);
        pool.inject_idle(s1);
        pool.inject_idle(s2);

        // Hinted device → the session on endpoint B, even though the
        // session on A is first in the idle queue.
        let guard = pool.acquire_for_device("root.sg.d1").unwrap();
        assert_eq!(guard.current_endpoint(), Some(&ep_b));
        drop(guard);

        // Unknown device → plain FIFO acquire (the session on A).
        let guard = pool.acquire_for_device("root.sg.unknown").unwrap();
        assert_eq!(guard.current_endpoint(), Some(&ep_a));
    }

    /// F12: when idle sessions hold conflicting hints for one device, the
    /// **newest** hint wins, not the session nearest the queue head.
    #[test]
    fn acquire_for_device_prefers_newest_conflicting_hint() {
        let ep_x = fake_listener();
        let ep_y = fake_listener();
        let ep_b = fake_listener();
        let ep_c = fake_listener();

        let cfg = SessionPoolConfig {
            max_size: 4,
            acquire_timeout: Duration::from_millis(50),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();

        // Older hint (inserted first, process-wide seq 1) points at B.
        let mut s_old_hint = injected_session(&ep_x);
        s_old_hint.test_inject_redirect_hint("root.sg.d1", ep_b.clone());
        // Newer hint (seq 2) points at C. Both hint-holders precede the
        // hinted sessions in the idle queue, so queue position would pick B.
        let mut s_new_hint = injected_session(&ep_y);
        s_new_hint.test_inject_redirect_hint("root.sg.d1", ep_c.clone());
        let s_on_b = injected_session(&ep_b);
        let s_on_c = injected_session(&ep_c);
        pool.inject_idle(s_old_hint);
        pool.inject_idle(s_new_hint);
        pool.inject_idle(s_on_b);
        pool.inject_idle(s_on_c);

        let guard = pool.acquire_for_device("root.sg.d1").unwrap();
        assert_eq!(guard.current_endpoint(), Some(&ep_c));
    }

    #[test]
    fn acquire_for_device_falls_back_when_no_session_matches_hint() {
        let ep_a = fake_listener();
        let ep_gone = Endpoint::new("10.255.255.1", 6667); // nobody connected here

        let cfg = SessionPoolConfig {
            max_size: 4,
            acquire_timeout: Duration::from_millis(50),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();
        let mut s1 = injected_session(&ep_a);
        s1.test_inject_redirect_hint("root.sg.d1", ep_gone);
        pool.inject_idle(s1);

        // Hint exists but no idle session is connected to that endpoint →
        // normal acquire semantics (FIFO hand-out of s1).
        let guard = pool.acquire_for_device("root.sg.d1").unwrap();
        assert_eq!(guard.current_endpoint(), Some(&ep_a));
    }

    /// Backdate an idle entry and the sweep clock so eviction tests are
    /// deterministic without long sleeps.
    fn backdate(pool: &SessionPool, entry_ages: Duration, sweep_age: Duration) {
        let mut state = pool.state.lock().unwrap();
        state.last_sweep = Instant::now() - sweep_age;
        for entry in &mut state.idle {
            entry.since = Instant::now() - entry_ages;
        }
    }

    /// Sessions idle past `max_idle_time` are closed by the sweep on the
    /// next acquire; the freed capacity is then used to (try to) grow.
    #[test]
    fn idle_sessions_are_evicted_on_acquire() {
        let ep = fake_listener();
        let cfg = SessionPoolConfig {
            max_size: 4,
            acquire_timeout: Duration::from_millis(50),
            max_idle_time: Duration::from_millis(5),
            idle_sweep_interval: Duration::from_millis(1),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();
        pool.inject_idle(injected_session(&ep));
        pool.inject_idle(injected_session(&ep));
        assert_eq!(pool.idle_count(), 2);

        // Exceed max_idle_time, then acquire: the sweep evicts both idle
        // sessions and the acquire grows — which fails against the dead
        // endpoint (a connect error, proving the idle queue really was
        // emptied rather than handed out).
        std::thread::sleep(Duration::from_millis(10));
        match pool.acquire() {
            Err(Error::Thrift(_)) => {}
            other => panic!("expected thrift connect error, got {other:?}"),
        }
        assert_eq!(pool.idle_count(), 0);
        assert_eq!(pool.live_count(), 0);
    }

    /// The sweep never shrinks the pool (idle + handed out) below
    /// `min_size`, evicting oldest-first.
    #[test]
    fn idle_sweep_respects_min_size_floor() {
        let ep = fake_listener();
        let cfg = SessionPoolConfig {
            max_size: 4,
            max_idle_time: Duration::from_millis(5),
            idle_sweep_interval: Duration::from_millis(1),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let mut pool = SessionPool::new(cfg).unwrap();
        pool.config.min_size = 2; // set post-new: eager open would need a server
        for _ in 0..3 {
            pool.inject_idle(injected_session(&ep));
        }
        backdate(&pool, Duration::from_millis(10), Duration::from_millis(10));

        let mut state = pool.state.lock().unwrap();
        let expired = pool.sweep_idle(&mut state);
        // All 3 exceed max_idle_time, but only 1 may go: 3 live - 1 = floor.
        assert_eq!(expired.len(), 1);
        assert_eq!(state.idle.len(), 2);
        drop(state);
        assert_eq!(pool.live_count(), 2);
    }

    /// The sweep is a no-op until `idle_sweep_interval` has passed since the
    /// previous sweep — even when idle sessions have already expired.
    #[test]
    fn idle_sweep_is_gated_by_interval() {
        let ep = fake_listener();
        let cfg = SessionPoolConfig {
            max_size: 4,
            max_idle_time: Duration::from_millis(1),
            idle_sweep_interval: Duration::from_secs(3600),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();
        pool.inject_idle(injected_session(&ep));
        // Entry long expired, but the last sweep (pool creation) is recent
        // relative to the huge interval → gated.
        backdate(&pool, Duration::from_secs(10), Duration::ZERO);
        let mut state = pool.state.lock().unwrap();
        assert!(pool.sweep_idle(&mut state).is_empty());
        assert_eq!(state.idle.len(), 1);

        // Once the interval has elapsed the same entry is evicted…
        state.last_sweep = Instant::now() - Duration::from_secs(3601);
        assert_eq!(pool.sweep_idle(&mut state).len(), 1);
        // …and last_sweep was refreshed, so an immediate re-sweep is gated.
        assert!(pool.sweep_idle(&mut state).is_empty());
    }

    /// Releasing a session also drives the sweep: the stale idle session is
    /// evicted while the just-released (fresh) one stays.
    #[test]
    fn release_triggers_sweep_but_keeps_fresh_session() {
        let ep = fake_listener();
        let cfg = SessionPoolConfig {
            max_size: 4,
            max_idle_time: Duration::from_millis(5),
            idle_sweep_interval: Duration::from_millis(1),
            session: dead_endpoint_config(),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();
        pool.inject_idle(injected_session(&ep));
        backdate(&pool, Duration::from_millis(10), Duration::from_millis(10));

        // Simulate returning a handed-out session (live already counted).
        pool.live.fetch_add(1, Ordering::Relaxed);
        pool.release(injected_session(&ep));
        assert_eq!(pool.idle_count(), 1, "stale evicted, fresh kept");
        assert_eq!(pool.live_count(), 1);
    }

    /// Live-server tests: acquire/release round-trip, reuse, and blocking
    /// hand-off. Skipped when no IoTDB server is reachable.
    #[test]
    fn live_pool_acquire_release_reuse() {
        if !live_server_available() {
            eprintln!("skipping live_pool_acquire_release_reuse: no server on 127.0.0.1:6667");
            return;
        }
        let cfg = SessionPoolConfig {
            max_size: 2,
            acquire_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let pool = SessionPool::new(cfg).unwrap();

        {
            let mut s1 = pool.acquire().unwrap();
            assert!(s1.is_open());
            let mut ds = s1.execute_query("SHOW DATABASES").unwrap();
            while ds.next_row().unwrap().is_some() {}
            drop(ds);
            let _s2 = pool.acquire().unwrap();
            assert_eq!(pool.live_count(), 2);
        }
        // Both released; a re-acquire reuses an idle session (no growth).
        let _s3 = pool.acquire().unwrap();
        assert_eq!(pool.live_count(), 2);
        drop(_s3);
        pool.close();
        assert_eq!(pool.live_count(), 0);
    }

    #[test]
    fn live_pool_waiter_wakes_on_release() {
        if !live_server_available() {
            eprintln!("skipping live_pool_waiter_wakes_on_release: no server on 127.0.0.1:6667");
            return;
        }
        let cfg = SessionPoolConfig {
            max_size: 1,
            acquire_timeout: Duration::from_secs(5),
            ..Default::default()
        };
        let pool = std::sync::Arc::new(SessionPool::new(cfg).unwrap());
        let guard = pool.acquire().unwrap();

        let p2 = pool.clone();
        let waiter = std::thread::spawn(move || p2.acquire().map(|s| s.is_open()));
        std::thread::sleep(Duration::from_millis(100));
        drop(guard); // wakes the waiter
        assert!(waiter.join().unwrap().unwrap());
        assert_eq!(pool.live_count(), 1);
    }
}
