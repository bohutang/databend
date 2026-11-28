// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::catch_unwind;
use databend_common_base::runtime::take_alloc_error_panic;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::Mutex;

type Job = Box<dyn FnOnce() + Send>;

struct IdleWorkers {
    closed: bool,
    // Oldest first. Each sender belongs to one parked thread.
    workers: Vec<(thread::ThreadId, mpsc::SyncSender<Option<Job>>)>,
}

/// Reuse threads, not execution slots. Each submitted loop gets a worker even if every
/// existing worker is blocked waiting for its query's processors.
pub(crate) struct QueryWorkerPool {
    shared: Arc<WorkerState>,
    next_thread_id: AtomicUsize,
}

// Workers own this state, not the pool handle registered with GlobalInstance.
struct WorkerState {
    idle: Mutex<IdleWorkers>,
    max_idle: usize,
    idle_timeout: Duration,
}

impl QueryWorkerPool {
    pub(crate) fn new(max_idle: usize, idle_timeout: Duration) -> Arc<Self> {
        Arc::new(Self {
            next_thread_id: AtomicUsize::new(0),
            shared: Arc::new(WorkerState {
                idle: Mutex::new(IdleWorkers {
                    closed: false,
                    workers: Vec::with_capacity(max_idle),
                }),
                max_idle,
                idle_timeout,
            }),
        })
    }

    pub(super) fn spawn<T: Send + 'static>(
        &self,
        f: impl FnOnce() -> Result<T> + Send + 'static,
    ) -> Result<mpsc::Receiver<Result<T>>> {
        let tracked = ThreadTracker::tracking_function(f);
        let (tx, rx) = mpsc::channel();
        let job: Job = Box::new(move || {
            let result = catch_unwind(tracked).flatten();
            // These allocator diagnostics are thread-local, outside TrackingPayload.
            ThreadTracker::replace_error_message(None);
            take_alloc_error_panic();
            ThreadTracker::flush_worker_memory();
            let _ = tx.send(result);
        });

        {
            let mut idle = self.shared.idle.lock();
            if let Some((_, worker)) = idle.workers.pop() {
                // The parked worker cannot retire until it acquires this same lock.
                worker
                    .try_send(Some(job))
                    .map_err(|_| ErrorCode::Internal("Idle pipeline worker unavailable"))?;
                return Ok(rx);
            }
        }

        let shared = self.shared.clone();
        let mut builder = thread::Builder::new().name(self.thread_name());
        if cfg!(debug_assertions) {
            builder = builder.stack_size(5 * 1024 * 1024);
        }
        // Thread::named_spawn inherits the submitter's tracking payload for the entire
        // thread lifetime. Here only `tracked` may inherit it; idle workers must not.
        builder
            .spawn(move || {
                ThreadTracker::init();
                let (sender, inbox) = mpsc::sync_channel(1);
                job();
                while let Some(job) = shared.next_job(&sender, &inbox) {
                    job();
                }
            })
            .map_err(|err| ErrorCode::Internal(format!("Cannot start pipeline worker: {err}")))?;
        Ok(rx)
    }

    fn thread_name(&self) -> String {
        // Test fixtures key their GlobalInstance registry by the parent thread's name.
        #[cfg(debug_assertions)]
        if matches!(std::env::var("UNIT_TEST").as_deref(), Ok("TRUE"))
            && let Some(name) = thread::current().name()
        {
            return name.to_owned();
        }
        let id = self.next_thread_id.fetch_add(1, Ordering::Relaxed);
        format!("PipeWkr-{id}")
    }
}

impl Drop for QueryWorkerPool {
    fn drop(&mut self) {
        let mut idle = self.shared.idle.lock();
        idle.closed = true;
        for (_, worker) in idle.workers.drain(..) {
            let _ = worker.try_send(None);
        }
    }
}

impl WorkerState {
    fn next_job(
        &self,
        sender: &mpsc::SyncSender<Option<Job>>,
        inbox: &mpsc::Receiver<Option<Job>>,
    ) -> Option<Job> {
        // Dropping the job box or an undeliverable result can free query-owned
        // memory after the pre-completion flush. Do not park with that owner.
        ThreadTracker::flush_worker_memory();
        let id = thread::current().id();
        {
            let mut idle = self.idle.lock();
            if idle.closed || idle.workers.len() == self.max_idle {
                return None;
            }
            idle.workers.push((id, sender.clone()));
        }

        if let Ok(job) = inbox.recv_timeout(self.idle_timeout) {
            return job;
        }
        // Timeout races with assignment and shutdown. Whoever removes this entry
        // owns the decision. A dispatcher removes AND sends under the lock, so a
        // claimed worker must consume that message rather than discard the job.
        let mut idle = self.idle.lock();
        if let Some(index) = idle.workers.iter().position(|(worker, _)| *worker == id) {
            idle.workers.remove(index);
            return None;
        }
        inbox
            .try_recv()
            .expect("claimed worker must have a message")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::thread::ThreadId;
    use std::time::Instant;

    use databend_common_base::runtime::MemStat;
    use databend_common_base::runtime::MemStatBuffer;

    use super::*;

    fn wait_for_idle(pool: &QueryWorkerPool, waiting: usize) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while pool.shared.idle.lock().workers.len() != waiting {
            assert!(Instant::now() < deadline, "expected {waiting} idle workers");
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn failed_query_does_not_follow_a_reused_thread() -> Result<()> {
        let pool = QueryWorkerPool::new(1, Duration::from_secs(30));
        let (thread_id, first_id) = mpsc::channel();
        let first_mem = MemStat::create("query-a".into());
        let interrupt = Arc::new(AtomicBool::new(true));
        let weak = Arc::downgrade(&interrupt);
        {
            let mut payload = ThreadTracker::new_tracking_payload();
            payload.query_id = Some("a".into());
            payload.processor_interrupt = Some(interrupt);
            payload.mem_stat = Some(first_mem.clone());
            let _guard = ThreadTracker::tracking(payload);
            let result = pool.spawn(move || -> Result<()> {
                assert!(Arc::ptr_eq(ThreadTracker::mem_stat().unwrap(), &first_mem));
                assert_eq!(ThreadTracker::query_id().unwrap(), "a");
                assert!(ThreadTracker::is_interrupted());
                ThreadTracker::replace_error_message(Some("a's allocation error".into()));
                thread_id.send(thread::current().id()).unwrap();
                Err(ErrorCode::AbortedQuery("cancel a"))
            })?;
            assert_eq!(
                result.recv().unwrap().unwrap_err().code(),
                ErrorCode::ABORTED_QUERY
            );
        }
        wait_for_idle(&pool, 1);
        assert!(weak.upgrade().is_none(), "idle thread retained query A");
        let mut payload = ThreadTracker::new_tracking_payload();
        let second_mem = MemStat::create("query-b".into());
        payload.query_id = Some("b".into());
        payload.mem_stat = Some(second_mem.clone());
        let _guard = ThreadTracker::tracking(payload);
        let second_id = pool.spawn(move || {
            assert_eq!(ThreadTracker::query_id().unwrap(), "b");
            assert!(Arc::ptr_eq(ThreadTracker::mem_stat().unwrap(), &second_mem));
            assert!(!ThreadTracker::is_interrupted());
            assert!(ThreadTracker::replace_error_message(None).is_none());
            Ok(thread::current().id())
        })?;
        assert_eq!(first_id.recv().unwrap(), second_id.recv().unwrap()?);
        Ok(())
    }

    #[test]
    fn idle_limit_does_not_queue_running_workers_and_idle_workers_retire() -> Result<()> {
        let pool = QueryWorkerPool::new(1, Duration::from_millis(100));
        let (release, blocked) = mpsc::channel();
        let a = pool.spawn(move || {
            blocked.recv_timeout(Duration::from_secs(5)).unwrap();
            Ok(thread::current().id())
        })?;
        let id = || -> Result<ThreadId> { Ok(thread::current().id()) };
        let b = pool.spawn(id)?.recv().unwrap()?;
        release.send(()).unwrap();
        let a = a.recv().unwrap()?;
        assert_ne!(a, b);
        // The worker loops hold the shared state until they actually exit.
        let deadline = Instant::now() + Duration::from_secs(5);
        while Arc::strong_count(&pool.shared) > 1 {
            assert!(Instant::now() < deadline, "worker failed to retire");
            thread::sleep(Duration::from_millis(1));
        }
        let c = pool.spawn(id)?.recv().unwrap()?;
        assert!(c != a && c != b);
        Ok(())
    }

    #[test]
    fn concurrent_workers_have_distinct_pool_names() -> Result<()> {
        // With no fixture name to inherit, exercise production naming even when a
        // concurrent debug TestFixture has set the process-wide UNIT_TEST flag.
        thread::Builder::new()
            .spawn(|| {
                let pool = QueryWorkerPool::new(2, Duration::from_secs(30));
                let (release, blocked) = mpsc::channel();
                let name = || thread::current().name().unwrap().to_owned();
                let first = pool.spawn(move || {
                    blocked.recv_timeout(Duration::from_secs(5)).unwrap();
                    Ok(name())
                })?;
                let second = pool
                    .spawn(move || Ok(name()))?
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()?;
                release.send(()).unwrap();
                let first = first.recv_timeout(Duration::from_secs(5)).unwrap()?;
                assert!(first.starts_with("PipeWkr-") && second.starts_with("PipeWkr-"));
                assert_ne!(first, second);
                Ok(())
            })
            .unwrap()
            .join()
            .unwrap()
    }

    #[test]
    fn worker_accounts_memory_before_reporting_completion() -> Result<()> {
        let pool = QueryWorkerPool::new(1, Duration::from_secs(30));
        let stat = MemStat::create("allocation".into());
        let worker_stat = stat.clone();
        let result = pool.spawn(move || -> Result<()> {
            MemStatBuffer::current().alloc(&worker_stat, 1024).unwrap();
            Err(ErrorCode::AbortedQuery("cancel after allocation"))
        })?;
        assert_eq!(
            result.recv().unwrap().unwrap_err().code(),
            ErrorCode::ABORTED_QUERY
        );
        assert_eq!(stat.get_memory_usage(), 1024);
        assert_eq!(Arc::strong_count(&stat), 1);
        stat.record_memory::<false>(-1024, 0).unwrap();
        Ok(())
    }

    #[test]
    fn balanced_allocations_do_not_pin_a_finished_query() -> Result<()> {
        let pool = QueryWorkerPool::new(1, Duration::from_secs(30));
        let stat = MemStat::create("balanced".into());
        let worker_stat = stat.clone();
        pool.spawn(move || {
            MemStatBuffer::current().alloc(&worker_stat, 1024).unwrap();
            MemStatBuffer::current().dealloc(&worker_stat, 1024);
            Ok(())
        })?
        .recv()
        .unwrap()?;
        assert_eq!(stat.get_memory_usage(), 0);
        assert_eq!(
            Arc::strong_count(&stat),
            1,
            "worker still holds a MemStat with zero pending bytes"
        );
        Ok(())
    }

    #[test]
    fn discarded_result_is_accounted_before_worker_parks() -> Result<()> {
        struct ResultAllocation(Arc<MemStat>);
        impl Drop for ResultAllocation {
            fn drop(&mut self) {
                MemStatBuffer::current().dealloc(&self.0, 1024);
            }
        }
        let pool = QueryWorkerPool::new(1, Duration::from_secs(30));
        let stat = MemStat::create("discarded result".into());
        let worker_stat = stat.clone();
        let (release, blocked) = mpsc::channel();
        let result = pool.spawn(move || {
            blocked.recv_timeout(Duration::from_secs(5)).unwrap();
            MemStatBuffer::current().alloc(&worker_stat, 1024).unwrap();
            Ok(ResultAllocation(worker_stat))
        })?;
        drop(result);
        release.send(()).unwrap();
        wait_for_idle(&pool, 1);
        assert_eq!(stat.get_memory_usage(), 0);
        assert_eq!(Arc::strong_count(&stat), 1);
        Ok(())
    }

    #[test]
    fn dropping_pool_wakes_idle_workers_without_aborting_active_jobs() -> Result<()> {
        thread_local! { static THREAD_LIFETIME: Arc<()> = Arc::new(()); }
        let pool = QueryWorkerPool::new(1, Duration::from_secs(30));
        let (release, blocked) = mpsc::channel();
        let active = pool.spawn(move || {
            blocked.recv_timeout(Duration::from_secs(5)).unwrap();
            Ok(42)
        })?;
        let idle_thread = pool
            .spawn(|| Ok(THREAD_LIFETIME.with(Arc::downgrade)))?
            .recv()
            .unwrap()?;
        wait_for_idle(&pool, 1);
        drop(pool);
        let deadline = Instant::now() + Duration::from_secs(1);
        while idle_thread.strong_count() != 0 {
            assert!(
                Instant::now() < deadline,
                "idle worker survived pool shutdown"
            );
            thread::sleep(Duration::from_millis(1));
        }
        release.send(()).unwrap();
        assert_eq!(active.recv_timeout(Duration::from_secs(1)).unwrap()?, 42);
        Ok(())
    }

    #[test]
    fn shutdown_preserves_jobs_already_assigned_to_idle_workers() -> Result<()> {
        let pool = QueryWorkerPool::new(1, Duration::from_secs(30));
        pool.spawn(|| Ok(()))?.recv().unwrap()?;
        wait_for_idle(&pool, 1);
        let result = pool.spawn(|| Ok(42))?;
        drop(pool);
        assert_eq!(result.recv_timeout(Duration::from_secs(1)).unwrap()?, 42);
        Ok(())
    }

    #[test]
    fn burst_workers_retire_while_serial_queries_continue() -> Result<()> {
        const BURST: usize = 8;
        let timeout = Duration::from_millis(500);
        let pool = QueryWorkerPool::new(BURST, timeout);
        // A weak TLS marker counts actual thread exits, not just workers momentarily
        // absent from the idle list while executing a job.
        thread_local! { static LIFETIME: Arc<()> = Arc::new(()); }
        let mut burst = Vec::new();
        for _ in 0..BURST {
            let (release, blocked) = mpsc::channel();
            let done = pool.spawn(move || {
                blocked.recv_timeout(Duration::from_secs(5)).unwrap();
                Ok(LIFETIME.with(Arc::downgrade))
            })?;
            burst.push((release, done));
        }
        let mut lifetimes = Vec::new();
        for (release, done) in burst {
            release.send(()).unwrap();
            lifetimes.push(done.recv_timeout(Duration::from_secs(5)).unwrap()?);
        }
        wait_for_idle(&pool, BURST);

        let deadline = Instant::now() + timeout * 4;
        while lifetimes.iter().filter(|w| w.strong_count() != 0).count() > 1 {
            assert!(
                Instant::now() < deadline,
                "surplus threads kept alive by serial queries"
            );
            pool.spawn(|| Ok(()))?
                .recv_timeout(Duration::from_secs(5))
                .unwrap()?;
            thread::sleep(timeout / 32);
        }
        Ok(())
    }

    #[test]
    fn submissions_racing_idle_expiry_complete() -> Result<()> {
        let pool = QueryWorkerPool::new(2, Duration::from_micros(100));
        for _ in 0..64 {
            let results = (0..8)
                .map(|n| pool.spawn(move || Ok(n)))
                .collect::<Result<Vec<_>>>()?;
            for (n, result) in results.into_iter().enumerate() {
                assert_eq!(result.recv_timeout(Duration::from_secs(5)).unwrap()?, n);
            }
            thread::sleep(Duration::from_micros(100));
        }
        Ok(())
    }
}
