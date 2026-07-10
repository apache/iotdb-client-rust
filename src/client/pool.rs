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
}

impl Default for SessionPoolConfig {
    fn default() -> Self {
        Self {
            session: SessionConfig::default(),
            max_size: 8,
            min_size: 0,
            acquire_timeout: Duration::from_secs(60),
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

/// Idle sessions plus the pool lifecycle flag, guarded by one mutex so
/// [`Condvar`] waiters observe both consistently.
struct PoolState {
    idle: VecDeque<Session>,
    closed: bool,
}

/// A pool of open tree-model [`Session`]s.
///
/// Sessions are created lazily up to `max_size` (after the eager
/// `min_size`); [`SessionPool::acquire`] blocks up to `acquire_timeout`
/// when the pool is exhausted. Dead sessions are discarded on acquire and
/// on release. The pool tracks the most recent `USE <db>` seen on any
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
            }),
            available: Condvar::new(),
            live: AtomicUsize::new(0),
            database: Mutex::new(database),
        };
        for _ in 0..pool.config.min_size {
            let session = pool.open_session()?;
            let mut state = pool.state.lock().expect("pool lock poisoned");
            state.idle.push_back(session);
            pool.live.fetch_add(1, Ordering::Relaxed);
        }
        Ok(pool)
    }

    /// Sessions alive right now (idle + handed out).
    pub fn live_count(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    /// Acquire a session, blocking up to `acquire_timeout` when the pool is
    /// at capacity with nothing idle. Dead idle sessions are discarded and
    /// the acquire retried; a fresh session is opened while under
    /// `max_size`.
    pub fn acquire(&self) -> Result<PooledSession<'_>> {
        let deadline = Instant::now() + self.config.acquire_timeout;
        let mut state = self.state.lock().expect("pool lock poisoned");
        loop {
            if state.closed {
                return Err(Error::Client("session pool is closed".into()));
            }
            // Idle session available → validate liveness, evict the dead.
            while let Some(session) = state.idle.pop_front() {
                if session.is_open() {
                    drop(state);
                    return self.hand_out(session);
                }
                self.live.fetch_sub(1, Ordering::Relaxed);
            }
            // Below capacity → grow lazily. Count the slot while still
            // holding the lock so concurrent acquires cannot overshoot.
            if self.live.load(Ordering::Relaxed) < self.config.max_size {
                self.live.fetch_add(1, Ordering::Relaxed);
                drop(state);
                match self.open_session() {
                    Ok(session) => return self.hand_out(session),
                    Err(e) => {
                        self.live.fetch_sub(1, Ordering::Relaxed);
                        self.available.notify_one();
                        return Err(e);
                    }
                }
            }
            // At capacity → wait for a release, bounded by the deadline.
            let now = Instant::now();
            if now >= deadline {
                return Err(Error::Client(format!(
                    "pool exhausted: no session available within {:?} ({} live, max {})",
                    self.config.acquire_timeout,
                    self.live.load(Ordering::Relaxed),
                    self.config.max_size
                )));
            }
            let (guard, _) = self
                .available
                .wait_timeout(state, deadline - now)
                .expect("pool lock poisoned");
            state = guard;
        }
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
        let drained = {
            let mut state = self.state.lock().expect("pool lock poisoned");
            state.closed = true;
            std::mem::take(&mut state.idle)
        };
        self.live.fetch_sub(drained.len(), Ordering::Relaxed);
        for mut session in drained {
            let _ = session.close();
        }
        self.available.notify_all();
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
    /// discarded, not returned to the pool.
    fn hand_out(&self, mut session: Session) -> Result<PooledSession<'_>> {
        let pool_db = self.database.lock().expect("pool lock poisoned").clone();
        if let Some(db) = pool_db {
            if session.database() != Some(db.as_str()) {
                if let Err(e) = session.execute_non_query(&format!("USE {db}")) {
                    let _ = session.close();
                    self.live.fetch_sub(1, Ordering::Relaxed);
                    self.available.notify_one();
                    return Err(e);
                }
            }
        }
        Ok(PooledSession {
            pool: self,
            session: Some(session),
        })
    }

    /// Return a session from a dropped guard. Dead sessions are discarded;
    /// live ones update the pool database and go back to the idle queue.
    fn release(&self, session: Session) {
        if !session.is_open() {
            self.live.fetch_sub(1, Ordering::Relaxed);
            self.available.notify_one();
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
            drop(state);
            self.live.fetch_sub(1, Ordering::Relaxed);
            let mut session = session;
            let _ = session.close();
        } else {
            state.idle.push_back(session);
            drop(state);
        }
        self.available.notify_one();
    }

    /// Test hook: push a pre-built session (possibly dead) into the idle
    /// queue, counting it as live.
    #[cfg(test)]
    fn inject_idle(&self, session: Session) {
        let mut state = self.state.lock().expect("pool lock poisoned");
        state.idle.push_back(session);
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

    /// Convenience: acquire, run one non-query statement, release.
    pub fn execute_non_query(&self, sql: &str) -> Result<()> {
        self.pool.execute_non_query(sql)
    }

    pub fn live_count(&self) -> usize {
        self.pool.live_count()
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
