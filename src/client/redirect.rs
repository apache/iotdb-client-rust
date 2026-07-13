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

//! Write-redirection cache (protocol spec §2/§3.7): a status-400
//! (`REDIRECTION_RECOMMEND`) write **succeeded**, and its `redirectNode`
//! recommends a better endpoint for that device. This cache remembers
//! device → endpoint hints with a TTL, mirroring the Node.js SDK's
//! `RedirectCache` (TTL 300 s, oldest-entry eviction when full).
//!
//! Routing honesty: a single [`crate::Session`] holds exactly one
//! connection, so the cache does **not** reroute inserts by itself — it
//! only records hints, exposed via [`crate::Session::redirect_hint`].
//! [`crate::SessionPool::acquire_for_device`] consults them to prefer an
//! idle session already connected to the hinted endpoint. Full per-device
//! connection routing (Node.js-style dedicated per-endpoint sessions) is
//! future work.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use crate::connection::Endpoint;
use crate::protocol::common::{TEndPoint, TSStatus};

/// Default hint lifetime, matching the Node.js SDK (300 s).
pub const DEFAULT_REDIRECT_TTL: Duration = Duration::from_secs(300);
/// Default capacity; the oldest entry is evicted when full.
pub const DEFAULT_REDIRECT_MAX_ENTRIES: usize = 1024;

/// TTL predicate, kept as a pure function so expiry logic is testable
/// without sleeping: an entry is expired once `elapsed` exceeds `ttl`.
/// A zero `ttl` disables expiry entirely (Node.js `ttl > 0` semantics).
fn is_expired(elapsed: Duration, ttl: Duration) -> bool {
    !ttl.is_zero() && elapsed > ttl
}

struct Entry {
    endpoint: Endpoint,
    inserted: Instant,
    /// Monotonic insertion counter — eviction order stays deterministic
    /// even when two inserts land on the same `Instant`.
    seq: u64,
}

/// Snapshot of a [`RedirectCache`]'s configuration and occupancy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedirectCacheStats {
    pub size: usize,
    pub max_entries: usize,
    pub ttl: Duration,
}

/// Device → endpoint hint cache fed by status-400 `redirectNode`s.
pub struct RedirectCache {
    entries: HashMap<String, Entry>,
    ttl: Duration,
    max_entries: usize,
    seq: u64,
}

impl Default for RedirectCache {
    fn default() -> Self {
        Self::new(DEFAULT_REDIRECT_TTL, DEFAULT_REDIRECT_MAX_ENTRIES)
    }
}

impl RedirectCache {
    /// A cache holding up to `max_entries` hints for `ttl` each. Zero `ttl`
    /// means hints never expire; zero `max_entries` disables the cache.
    pub fn new(ttl: Duration, max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            ttl,
            max_entries,
            seq: 0,
        }
    }

    /// The cached endpoint for `device_id`, or `None` when absent or
    /// expired (expired entries are removed on the way out).
    pub fn get(&mut self, device_id: &str) -> Option<Endpoint> {
        self.get_at(device_id, Instant::now())
    }

    /// [`RedirectCache::get`] against an explicit "now" — the seam the TTL
    /// tests use instead of sleeping.
    fn get_at(&mut self, device_id: &str, now: Instant) -> Option<Endpoint> {
        let entry = self.entries.get(device_id)?;
        if is_expired(now.saturating_duration_since(entry.inserted), self.ttl) {
            self.entries.remove(device_id);
            return None;
        }
        Some(entry.endpoint.clone())
    }

    /// Record (or refresh) the hint for `device_id`. When the cache is full
    /// and the device is new, the oldest-inserted entry is evicted first.
    pub fn put(&mut self, device_id: impl Into<String>, endpoint: Endpoint) {
        if self.max_entries == 0 {
            return;
        }
        let device_id = device_id.into();
        if self.entries.len() >= self.max_entries && !self.entries.contains_key(&device_id) {
            let oldest = self
                .entries
                .iter()
                .min_by_key(|(_, e)| e.seq)
                .map(|(k, _)| k.clone());
            if let Some(oldest) = oldest {
                self.entries.remove(&oldest);
            }
        }
        self.seq += 1;
        self.entries.insert(
            device_id,
            Entry {
                endpoint,
                inserted: Instant::now(),
                seq: self.seq,
            },
        );
    }

    /// Drop all hints.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    pub fn stats(&self) -> RedirectCacheStats {
        RedirectCacheStats {
            size: self.entries.len(),
            max_entries: self.max_entries,
            ttl: self.ttl,
        }
    }
}

/// Convert a redirect `TEndPoint` into a client [`Endpoint`], rejecting
/// nonsense (empty host, port outside `u16`).
fn endpoint_from_redirect(node: &TEndPoint) -> Option<Endpoint> {
    if node.ip.is_empty() {
        return None;
    }
    u16::try_from(node.port)
        .ok()
        .filter(|&p| p != 0)
        .map(|port| Endpoint::new(node.ip.clone(), port))
}

/// Harvest redirect hints from an insert response into the cache — called
/// on the raw `TSStatus` *before* it is collapsed into a `Result`, since
/// `check_status` treats 400 as plain success and discards the node.
///
/// A top-level `redirectNode` (single-device inserts and tablet writes)
/// applies to every device in the request; a `subStatus` list that pairs
/// 1:1 with the request's devices (multi-device `insertRecords`) is
/// harvested entry-by-entry.
pub(crate) fn record_redirects(cache: &mut RedirectCache, devices: &[&str], status: &TSStatus) {
    if let Some(subs) = status.sub_status.as_deref() {
        if subs.len() == devices.len() {
            for (device, sub) in devices.iter().zip(subs) {
                if let Some(endpoint) = sub.redirect_node.as_ref().and_then(endpoint_from_redirect)
                {
                    cache.put(*device, endpoint);
                }
            }
            return;
        }
    }
    if let Some(endpoint) = status
        .redirect_node
        .as_ref()
        .and_then(endpoint_from_redirect)
    {
        for device in devices {
            cache.put(*device, endpoint.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new("10.0.0.1", port)
    }

    #[test]
    fn ttl_predicate() {
        let ttl = Duration::from_secs(300);
        assert!(!is_expired(Duration::ZERO, ttl));
        assert!(!is_expired(ttl, ttl)); // boundary: exactly ttl is still fresh
        assert!(is_expired(ttl + Duration::from_millis(1), ttl));
        // Zero TTL disables expiry.
        assert!(!is_expired(Duration::from_secs(1 << 20), Duration::ZERO));
    }

    #[test]
    fn put_get_roundtrip_and_clear() {
        let mut cache = RedirectCache::default();
        assert_eq!(cache.get("root.sg.d1"), None);
        cache.put("root.sg.d1", ep(6667));
        assert_eq!(cache.get("root.sg.d1"), Some(ep(6667)));
        // Refresh overwrites.
        cache.put("root.sg.d1", ep(6668));
        assert_eq!(cache.get("root.sg.d1"), Some(ep(6668)));
        assert_eq!(cache.stats().size, 1);
        cache.clear();
        assert_eq!(cache.stats().size, 0);
        assert_eq!(cache.get("root.sg.d1"), None);
    }

    #[test]
    fn expired_entries_are_removed_on_get() {
        let mut cache = RedirectCache::new(Duration::from_secs(300), 16);
        cache.put("root.sg.d1", ep(6667));
        // Fresh "now": hit.
        assert_eq!(cache.get_at("root.sg.d1", Instant::now()), Some(ep(6667)));
        // A "now" past the TTL: miss, and the entry is gone afterwards.
        let later = Instant::now() + Duration::from_secs(301);
        assert_eq!(cache.get_at("root.sg.d1", later), None);
        assert_eq!(cache.stats().size, 0);
    }

    #[test]
    fn zero_ttl_never_expires() {
        let mut cache = RedirectCache::new(Duration::ZERO, 16);
        cache.put("root.sg.d1", ep(6667));
        let far_future = Instant::now() + Duration::from_secs(1 << 20);
        assert_eq!(cache.get_at("root.sg.d1", far_future), Some(ep(6667)));
    }

    #[test]
    fn eviction_drops_oldest_when_full() {
        let mut cache = RedirectCache::new(Duration::from_secs(300), 2);
        cache.put("d1", ep(1));
        cache.put("d2", ep(2));
        cache.put("d3", ep(3)); // evicts d1 (oldest insertion)
        assert_eq!(cache.get("d1"), None);
        assert_eq!(cache.get("d2"), Some(ep(2)));
        assert_eq!(cache.get("d3"), Some(ep(3)));
        assert_eq!(cache.stats().size, 2);

        // Refreshing an existing key does not evict.
        cache.put("d2", ep(22));
        assert_eq!(cache.get("d2"), Some(ep(22)));
        assert_eq!(cache.get("d3"), Some(ep(3)));

        // A refreshed key counts as newer: next eviction takes d3.
        cache.put("d4", ep(4));
        assert_eq!(cache.get("d3"), None);
        assert_eq!(cache.get("d2"), Some(ep(22)));
        assert_eq!(cache.get("d4"), Some(ep(4)));
    }

    #[test]
    fn zero_capacity_disables_cache() {
        let mut cache = RedirectCache::new(Duration::from_secs(300), 0);
        cache.put("d1", ep(1));
        assert_eq!(cache.get("d1"), None);
        assert_eq!(
            cache.stats(),
            RedirectCacheStats {
                size: 0,
                max_entries: 0,
                ttl: Duration::from_secs(300),
            }
        );
    }

    fn status(code: i32) -> TSStatus {
        TSStatus::new(code, None, None, None, None, None)
    }

    fn tendpoint(ip: &str, port: i32) -> TEndPoint {
        TEndPoint::new(ip.to_string(), port)
    }

    #[test]
    fn record_top_level_redirect_applies_to_all_devices() {
        let mut cache = RedirectCache::default();
        let mut s = status(400);
        s.redirect_node = Some(tendpoint("10.0.0.9", 6667));
        record_redirects(&mut cache, &["root.sg.d1", "root.sg.d2"], &s);
        assert_eq!(
            cache.get("root.sg.d1"),
            Some(Endpoint::new("10.0.0.9", 6667))
        );
        assert_eq!(
            cache.get("root.sg.d2"),
            Some(Endpoint::new("10.0.0.9", 6667))
        );
    }

    #[test]
    fn record_sub_status_redirects_per_device() {
        let mut cache = RedirectCache::default();
        let mut ok = status(200);
        ok.redirect_node = Some(tendpoint("10.0.0.7", 6667));
        let plain = status(200);
        let mut s = status(302);
        s.sub_status = Some(vec![Box::new(ok), Box::new(plain)]);
        record_redirects(&mut cache, &["d1", "d2"], &s);
        assert_eq!(cache.get("d1"), Some(Endpoint::new("10.0.0.7", 6667)));
        assert_eq!(cache.get("d2"), None);
    }

    #[test]
    fn record_ignores_absent_and_invalid_redirect_nodes() {
        let mut cache = RedirectCache::default();
        record_redirects(&mut cache, &["d1"], &status(200));
        let mut bad_port = status(400);
        bad_port.redirect_node = Some(tendpoint("10.0.0.9", 70000));
        record_redirects(&mut cache, &["d1"], &bad_port);
        let mut empty_ip = status(400);
        empty_ip.redirect_node = Some(tendpoint("", 6667));
        record_redirects(&mut cache, &["d1"], &empty_ip);
        assert_eq!(cache.stats().size, 0);
    }
}
