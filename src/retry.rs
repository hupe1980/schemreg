//! Retry policy for transient registry failures.
//!
//! Applies to every HTTP request issued by the Confluent and Apicurio clients.
//! The defaults are tuned for a schema registry sitting behind a load balancer:
//! short enough that a producer's first send is not stalled by a blip, long
//! enough that a rolling restart is ridden out rather than amplified.

use std::time::Duration;

/// Default number of retries after the initial attempt.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// Default base delay; doubles on each subsequent attempt.
pub const DEFAULT_BASE_BACKOFF: Duration = Duration::from_millis(100);

/// Default ceiling on any single delay, including a server-supplied `Retry-After`.
pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(60);

/// How long to wait before a retry, and how many times to try at all.
///
/// # Back-off shape
///
/// The uncapped delay for attempt *n* (0-based) is `base × 2ⁿ`, capped at
/// [`max_backoff`](Self::max_backoff). With jitter enabled — the default — the
/// actual delay is drawn from the upper half of that interval
/// (`temp/2 + random(0, temp/2)`, the "equal jitter" strategy).
///
/// Jitter matters more than it looks. Without it, every client that saw the
/// same 503 retries at exactly 100 ms, 200 ms, 400 ms — reconverging into
/// synchronised waves that hit the registry precisely while it is recovering.
/// Jitter spreads them out. It is deliberately *equal* rather than *full*
/// jitter so that the first retry still waits a meaningful minimum instead of
/// occasionally firing immediately.
///
/// # `Retry-After`
///
/// A server-supplied `Retry-After` is **never** jittered and never shortened:
/// the server asked for at least that long, so the value is used as-is (still
/// subject to [`max_backoff`](Self::max_backoff), which bounds a hostile or
/// mistaken header). Both the delta-seconds and the HTTP-date forms of
/// RFC 9110 §10.2.3 are understood.
///
/// # Example
///
/// ```rust
/// use std::time::Duration;
/// use schemreg::RetryPolicy;
///
/// // Fail fast: one retry, 50 ms base, no jitter (deterministic tests).
/// let policy = RetryPolicy::new()
///     .max_retries(1)
///     .base_backoff(Duration::from_millis(50))
///     .jitter(false);
///
/// // Or disable retrying entirely.
/// let none = RetryPolicy::none();
/// assert_eq!(none.max_retries_value(), 0);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetryPolicy {
    max_retries: u32,
    base_backoff: Duration,
    max_backoff: Duration,
    honor_retry_after: bool,
    jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            base_backoff: DEFAULT_BASE_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            honor_retry_after: true,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// A policy with the documented defaults: 3 retries, 100 ms base, 60 s cap,
    /// `Retry-After` honoured, jitter on.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A policy that never retries. The first failure propagates to the caller.
    ///
    /// Use this when the calling layer already implements retry and budget
    /// accounting, so the two do not multiply.
    #[must_use]
    pub fn none() -> Self {
        Self {
            max_retries: 0,
            ..Self::default()
        }
    }

    /// Number of retries **after** the initial attempt. `0` disables retrying.
    #[must_use]
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Base delay, doubled on each subsequent attempt.
    #[must_use]
    pub fn base_backoff(mut self, base: Duration) -> Self {
        self.base_backoff = base;
        self
    }

    /// Ceiling on any single delay, including a server-supplied `Retry-After`.
    #[must_use]
    pub fn max_backoff(mut self, max: Duration) -> Self {
        self.max_backoff = max;
        self
    }

    /// Whether to obey a server-supplied `Retry-After` header (default: `true`).
    #[must_use]
    pub fn honor_retry_after(mut self, honor: bool) -> Self {
        self.honor_retry_after = honor;
        self
    }

    /// Whether to apply equal jitter to computed back-off (default: `true`).
    ///
    /// Turn this off for deterministic tests. Leave it on in production.
    #[must_use]
    pub fn jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// The configured retry count.
    #[must_use]
    pub fn max_retries_value(&self) -> u32 {
        self.max_retries
    }

    /// Compute the delay before the retry that follows attempt `attempt`
    /// (0-based), given an optional server-supplied `Retry-After` in milliseconds.
    pub(crate) fn delay_for(&self, attempt: u32, retry_after_ms: Option<u64>) -> Duration {
        let max_ms = as_millis_u64(self.max_backoff);

        if self.honor_retry_after
            && let Some(server_ms) = retry_after_ms
        {
            // Never jittered, never shortened — but still bounded, so a hostile
            // or mistaken `Retry-After: 86400` cannot wedge the caller.
            return Duration::from_millis(server_ms.min(max_ms));
        }

        let base_ms = as_millis_u64(self.base_backoff);
        // Saturating shift: attempt is bounded by max_retries in practice, but
        // a caller-supplied max_retries of 64+ must not wrap.
        let uncapped = base_ms.saturating_mul(1u64.checked_shl(attempt).unwrap_or(u64::MAX));
        let capped = uncapped.min(max_ms);

        if !self.jitter || capped == 0 {
            return Duration::from_millis(capped);
        }

        // Equal jitter: half the interval fixed, half random.
        let half = capped / 2;
        Duration::from_millis(half + random_u64_below(half.saturating_add(1)))
    }
}

/// `Duration::as_millis` saturating into `u64`.
fn as_millis_u64(d: Duration) -> u64 {
    u64::try_from(d.as_millis()).unwrap_or(u64::MAX)
}

/// A uniformly distributed value in `[0, bound)`, or `0` when `bound == 0`.
///
/// Uses `RandomState`'s per-process random seed — the same source `HashMap`
/// relies on for HashDoS resistance — so no `rand` dependency is needed for
/// what is only ever back-off jitter.
fn random_u64_below(bound: u64) -> u64 {
    if bound == 0 {
        return 0;
    }
    use std::hash::{BuildHasher, Hasher};
    let seed = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    seed % bound
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_the_documented_values() {
        let p = RetryPolicy::new();
        assert_eq!(p.max_retries_value(), 3);
        assert_eq!(p.base_backoff, Duration::from_millis(100));
        assert_eq!(p.max_backoff, Duration::from_secs(60));
        assert!(p.honor_retry_after);
        assert!(p.jitter);
    }

    #[test]
    fn backoff_doubles_without_jitter() {
        let p = RetryPolicy::new().jitter(false);
        assert_eq!(p.delay_for(0, None), Duration::from_millis(100));
        assert_eq!(p.delay_for(1, None), Duration::from_millis(200));
        assert_eq!(p.delay_for(2, None), Duration::from_millis(400));
        assert_eq!(p.delay_for(3, None), Duration::from_millis(800));
    }

    #[test]
    fn backoff_is_capped() {
        let p = RetryPolicy::new()
            .jitter(false)
            .max_backoff(Duration::from_millis(250));
        assert_eq!(p.delay_for(10, None), Duration::from_millis(250));
    }

    #[test]
    fn extreme_attempt_counts_do_not_overflow() {
        let p = RetryPolicy::new().jitter(false);
        // Shifting by >= 64 must saturate, not wrap to a tiny delay.
        assert_eq!(p.delay_for(64, None), Duration::from_secs(60));
        assert_eq!(p.delay_for(u32::MAX, None), Duration::from_secs(60));
    }

    #[test]
    fn jitter_stays_in_the_upper_half_of_the_interval() {
        let p = RetryPolicy::new();
        for attempt in 0..4 {
            let uncapped = 100u64 << attempt;
            for _ in 0..200 {
                let ms = p.delay_for(attempt, None).as_millis() as u64;
                assert!(
                    ms >= uncapped / 2 && ms <= uncapped,
                    "attempt {attempt}: {ms}ms outside [{}, {uncapped}]",
                    uncapped / 2
                );
            }
        }
    }

    #[test]
    fn jitter_actually_varies() {
        let p = RetryPolicy::new();
        let samples: std::collections::HashSet<u128> =
            (0..100).map(|_| p.delay_for(3, None).as_millis()).collect();
        assert!(
            samples.len() > 1,
            "jitter must produce more than one value across 100 draws"
        );
    }

    #[test]
    fn retry_after_wins_and_is_never_jittered() {
        let p = RetryPolicy::new();
        for _ in 0..50 {
            assert_eq!(p.delay_for(0, Some(2_000)), Duration::from_millis(2_000));
        }
    }

    #[test]
    fn retry_after_is_still_bounded_by_max_backoff() {
        let p = RetryPolicy::new().max_backoff(Duration::from_secs(5));
        // A server asking for a full day must not wedge the caller.
        assert_eq!(p.delay_for(0, Some(86_400_000)), Duration::from_secs(5));
    }

    #[test]
    fn retry_after_can_be_ignored() {
        let p = RetryPolicy::new().honor_retry_after(false).jitter(false);
        assert_eq!(p.delay_for(0, Some(9_000)), Duration::from_millis(100));
    }

    #[test]
    fn none_policy_disables_retrying() {
        assert_eq!(RetryPolicy::none().max_retries_value(), 0);
    }

    #[test]
    fn zero_base_backoff_is_handled() {
        let p = RetryPolicy::new().base_backoff(Duration::ZERO);
        assert_eq!(p.delay_for(0, None), Duration::ZERO);
        assert_eq!(p.delay_for(5, None), Duration::ZERO);
    }
}
