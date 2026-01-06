use bytes::Bytes;
use parking_lot::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DropPolicy {
    DropOldest,
    DropNewest,
    Disconnect,
}

impl Default for DropPolicy {
    fn default() -> Self {
        DropPolicy::DropOldest
    }
}

pub struct BackpressureConfig {
    pub buffer_size: usize,
    pub slow_consumer_threshold: usize,
    pub drop_policy: DropPolicy,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            buffer_size: 1024,
            slow_consumer_threshold: 512,
            drop_policy: DropPolicy::DropOldest,
        }
    }
}

pub struct ClientBuffer {
    buffer: Mutex<VecDeque<Bytes>>,
    capacity: usize,
    slow_threshold: usize,
    drop_policy: DropPolicy,
    pending: AtomicUsize,
    is_slow: AtomicBool,
    disconnected: AtomicBool,
    notify: Notify,
}

impl ClientBuffer {
    pub fn new(config: &BackpressureConfig) -> Arc<Self> {
        Arc::new(Self {
            buffer: Mutex::new(VecDeque::with_capacity(config.buffer_size)),
            capacity: config.buffer_size,
            slow_threshold: config.slow_consumer_threshold,
            drop_policy: config.drop_policy,
            pending: AtomicUsize::new(0),
            is_slow: AtomicBool::new(false),
            disconnected: AtomicBool::new(false),
            notify: Notify::new(),
        })
    }

    pub fn push(&self, msg: Bytes) -> PushResult {
        if self.disconnected.load(Ordering::Relaxed) {
            return PushResult::Disconnected;
        }

        let mut buffer = self.buffer.lock();
        let current_len = buffer.len();

        if current_len >= self.slow_threshold && !self.is_slow.load(Ordering::Relaxed) {
            self.is_slow.store(true, Ordering::Relaxed);
            metrics::counter!("certstream_slow_consumers_detected").increment(1);
        }

        if current_len >= self.capacity {
            match self.drop_policy {
                DropPolicy::DropOldest => {
                    buffer.pop_front();
                    buffer.push_back(msg);
                    self.notify.notify_one();
                    metrics::counter!("certstream_messages_dropped", "policy" => "oldest").increment(1);
                    PushResult::DroppedOldest
                }
                DropPolicy::DropNewest => {
                    metrics::counter!("certstream_messages_dropped", "policy" => "newest").increment(1);
                    PushResult::DroppedNewest
                }
                DropPolicy::Disconnect => {
                    self.disconnected.store(true, Ordering::Relaxed);
                    self.notify.notify_one();
                    metrics::counter!("certstream_slow_consumer_disconnects").increment(1);
                    PushResult::Disconnected
                }
            }
        } else {
            buffer.push_back(msg);
            self.pending.fetch_add(1, Ordering::Relaxed);
            self.notify.notify_one();
            PushResult::Queued
        }
    }

    pub fn pop(&self) -> Option<Bytes> {
        let mut buffer = self.buffer.lock();
        let msg = buffer.pop_front();
        if msg.is_some() {
            self.pending.fetch_sub(1, Ordering::Relaxed);
            if buffer.len() < self.slow_threshold / 2 && self.is_slow.load(Ordering::Relaxed) {
                self.is_slow.store(false, Ordering::Relaxed);
            }
        }
        msg
    }

    pub async fn wait_for_message(&self) {
        self.notify.notified().await;
    }

    pub fn is_disconnected(&self) -> bool {
        self.disconnected.load(Ordering::Relaxed)
    }

    pub fn is_slow(&self) -> bool {
        self.is_slow.load(Ordering::Relaxed)
    }

    pub fn pending_count(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    pub fn mark_disconnected(&self) {
        self.disconnected.store(true, Ordering::Relaxed);
        self.notify.notify_one();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushResult {
    Queued,
    DroppedOldest,
    DroppedNewest,
    Disconnected,
}

pub struct FlowController {
    buffers: parking_lot::RwLock<Vec<Arc<ClientBuffer>>>,
    config: BackpressureConfig,
}

impl FlowController {
    pub fn new(config: BackpressureConfig) -> Arc<Self> {
        Arc::new(Self {
            buffers: parking_lot::RwLock::new(Vec::new()),
            config,
        })
    }

    pub fn register_client(&self) -> Arc<ClientBuffer> {
        let buffer = ClientBuffer::new(&self.config);
        self.buffers.write().push(buffer.clone());
        buffer
    }

    pub fn unregister_client(&self, buffer: &Arc<ClientBuffer>) {
        buffer.mark_disconnected();
        self.buffers.write().retain(|b| !Arc::ptr_eq(b, buffer));
    }

    pub fn broadcast(&self, msg: Bytes) {
        let buffers = self.buffers.read();
        for buffer in buffers.iter() {
            if !buffer.is_disconnected() {
                buffer.push(msg.clone());
            }
        }
    }

    pub fn slow_consumer_count(&self) -> usize {
        self.buffers.read().iter().filter(|b| b.is_slow()).count()
    }

    pub fn active_client_count(&self) -> usize {
        self.buffers.read().iter().filter(|b| !b.is_disconnected()).count()
    }

    pub fn cleanup_disconnected(&self) {
        self.buffers.write().retain(|b| !b.is_disconnected());
    }
}
