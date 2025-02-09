use std::{collections::VecDeque, marker::PhantomData, ops::Deref, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}};

use overseer::{access::WatcherBehaviour, models::Value};
use tokio::sync::Notify;



pub struct WatchServer;
pub struct WatchClient;

enum HoldingInner {
    /// An ordered watcher returns things in the order of
    /// which they came.
    Ordered(Mutex<VecDeque<Option<Arc<Value>>>>),
    /// An eager watcher does not care for this.
    Eager(Mutex<Option<Arc<Value>>>)

}

struct WatcherInner {
    inner: HoldingInner,
    wakeup: Notify,
    killed: AtomicBool,
}

/// The [Watcher] struct lets us notify subscribers of changes.
pub struct Watcher<S> {
    /// The inner structure of the watcher.
    inner: Arc<WatcherInner>,
    /// The type which allows restricting the struct
    /// methods.
    side: PhantomData<S>
}



impl Watcher<WatchServer> {
    /// This method notifies all of the watchers.
    pub fn notify_coordinated<I, D>(witer: I, value: Option<Arc<Value>>)
    where 
        I: Iterator<Item = D>,
        D: Deref<Target = Watcher<WatchServer>>
    {
        let mut signals = Vec::with_capacity(witer.size_hint().0);
        
        // Load all the watchers without triggering them.
        for watch_ref in witer {
            watch_ref.wake_without_notify(value.clone());
            signals.push(watch_ref.inner.clone());
        }

        // Trigger all the watchers.
        for signal in signals {
            signal.wakeup.notify_waiters();
        }
    }
}

impl Watcher<()> {
    /// Returns a split watcher. One of these is for
    /// the client and there other is for the server.
    pub fn new(class: WatcherBehaviour) -> (Watcher<WatchClient>, Watcher<WatchServer>) {


        let inner = Arc::new(WatcherInner {
            inner: match class {
                WatcherBehaviour::Eager => HoldingInner::Eager(Mutex::default()),
                WatcherBehaviour::Ordered => HoldingInner::Ordered(Mutex::default()),
            },
            killed: AtomicBool::new(false),
            wakeup: Notify::new()
        });

        (
            Watcher {
                inner: Arc::clone(&inner),
                side: PhantomData
            },
            Watcher {
                inner,
                side: PhantomData
            }
        )
    }
}



impl Watcher<WatchClient> {
    pub async fn force_recv(&self) -> Option<Arc<Value>> {
        match &self.inner.inner {
            HoldingInner::Eager(value) => {
                value.lock().unwrap().take()
            },
            HoldingInner::Ordered(value) => {
                value.lock().unwrap().pop_front()?
            }
        } 
    }
    pub async fn wait(&self) -> Option<Arc<Value>> {
        match &self.inner.inner {
            HoldingInner::Eager(value) => {
                if value.lock().unwrap().is_some() {
                    value.lock().unwrap().take()
                } else {
                    self.inner.wakeup.notified().await;
                    value.lock().unwrap().take()
                }
            },
            HoldingInner::Ordered(value) => {
                if !value.lock().unwrap().is_empty() {
                    value.lock().unwrap().pop_front()?
                } else {
                    self.inner.wakeup.notified().await;
                    value.lock().unwrap().pop_front()?
                }
            }
        }
    }
    pub fn is_killed(&self) -> bool {
        self.inner.killed.load(Ordering::Acquire)
    }
    
}



impl Watcher<WatchServer> {
    fn wake_without_notify(&self, nvalue: Option<Arc<Value>>) {
        match &self.inner.inner {
            HoldingInner::Eager(value) => {
                *value.lock().unwrap() = nvalue;
                
            },
            HoldingInner::Ordered(value) => {
                
                value.lock().unwrap().push_back(nvalue);
            }
        }
    }
    pub fn wake(&self, nvalue: Option<Arc<Value>>) {
        self.wake_without_notify(nvalue);
        self.inner.wakeup.notify_waiters();
    }
    pub fn kill(&self) {
        self.inner.killed.store(true, Ordering::SeqCst);
        self.wake(None);
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use overseer::models::Value;

    use crate::database::watcher::{Watcher, WatcherBehaviour};


    #[tokio::test]
    pub async fn check_watcher_correctness_ordered() {
        let (client, server) = Watcher::new(WatcherBehaviour::Ordered);
        server.wake(None);
        server.wake(Some(Value::Integer(0).into()));
        assert!(client.wait().await.is_none());
        // assert_eq!(client.wait().await.unwrap().as_integer().unwrap(), 0);
    }

    #[tokio::test]
    pub async fn check_watcher_correctness_eager() {
        let (client, server) = Watcher::new(WatcherBehaviour::Eager);
        server.wake(None);
        server.wake(Some(Value::Integer(0).into()));
        assert_eq!(client.wait().await.unwrap().as_integer().unwrap(), 0);
    }

    #[tokio::test]
    pub async fn check_watcher_notify_synchronize() {
        let (client_1, server_1) = Watcher::new(WatcherBehaviour::Eager);
        let (client_2, server_2) = Watcher::new(WatcherBehaviour::Eager);
        

        Watcher::notify_coordinated([server_1, server_2].iter(), Some(Arc::new(Value::Integer(45))));

        assert_eq!(client_1.wait().await.unwrap().as_integer().unwrap(), 45);
        assert_eq!(client_2.wait().await.unwrap().as_integer().unwrap(), 45);
        
    }
}