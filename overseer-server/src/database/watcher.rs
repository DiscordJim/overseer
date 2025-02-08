use std::{collections::VecDeque, marker::PhantomData, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}};

use overseer::{access::WatcherBehaviour, models::Value};
use tokio::sync::Notify;


pub struct WatchServer;
pub struct WatchClient;

// TODO: Way to notify all watchers at the same time.



type EagerInner = Arc<Mutex<Option<Arc<Value>>>>;
type OrderedInner = Arc<Mutex<VecDeque<Option<Arc<Value>>>>>;

pub enum Watcher<S> {
    /// An ordered watcher returns things in the order of
    /// which they came.
    Ordered {
        value: OrderedInner,
        wakeup: Arc<Notify>,
        killed: Arc<AtomicBool>,
        side: PhantomData<S>
    },
    /// An eager watcher does not care for this.
    Eager {
        value: EagerInner,
        wakeup: Arc<Notify>,
        killed: Arc<AtomicBool>,
        side: PhantomData<S>
    }
}


impl Watcher<()> {
    /// Returns a split watcher. One of these is for
    /// the client and there other is for the server.
    pub fn new(class: WatcherBehaviour) -> (Watcher<WatchClient>, Watcher<WatchServer>) {

        let wakeup = Arc::new(Notify::new());
        let killed = Arc::new(AtomicBool::new(false));
        match class {
            WatcherBehaviour::Eager => {
                let value: EagerInner = EagerInner::default();

                (
                    Watcher::Eager { value: Arc::clone(&value), wakeup: Arc::clone(&wakeup), killed: Arc::clone(&killed), side: PhantomData },
                    Watcher::Eager { value, wakeup, killed, side: PhantomData }
                )

            },
            WatcherBehaviour::Ordered => {
                let value: OrderedInner = OrderedInner::default();

                (
                    Watcher::Ordered { value: Arc::clone(&value), side: PhantomData, wakeup: Arc::clone(&wakeup), killed: Arc::clone(&killed) },
                    Watcher::Ordered { value, side: PhantomData, wakeup, killed }
                )
            }
        }
    }
}



impl Watcher<WatchClient> {
    pub async fn force_recv(&self) -> Option<Arc<Value>> {
        match self {
            Self::Eager { value, .. } => {
                value.lock().unwrap().take()
            },
            Self::Ordered { value, .. } => {
                value.lock().unwrap().pop_front()?
            }
        } 
    }
    pub async fn wait(&self) -> Option<Arc<Value>> {
        match self {
            Self::Eager { value, wakeup, .. } => {
                if value.lock().unwrap().is_some() {
                    value.lock().unwrap().take()
                } else {
                    wakeup.notified().await;
                    value.lock().unwrap().take()
                }
            },
            Self::Ordered { value, wakeup, .. } => {
                if !value.lock().unwrap().is_empty() {
                    value.lock().unwrap().pop_front()?
                } else {
                    wakeup.notified().await;
                    value.lock().unwrap().pop_front()?
                }
            }
        }
    }
    pub fn is_killed(&self) -> bool {
        match self {
            Self::Eager { killed, .. } | Self::Ordered { killed, .. } => {
                killed.load(Ordering::Acquire)
            }
        }
    }
    
}

impl Watcher<WatchServer> {
    pub fn wake(&self, nvalue: Option<Arc<Value>>) {
        match self {
            Self::Eager { value, wakeup, .. } => {
                *value.lock().unwrap() = nvalue;
                wakeup.notify_waiters();
                
            },
            Self::Ordered { value, wakeup, .. } => {
                value.lock().unwrap().push_back(nvalue);
                wakeup.notify_waiters();
            }
        }
    }
    pub fn kill(&self) {
        
        match self {
            Self::Eager { killed, .. } | Self::Ordered { killed, .. } => {
                killed.store(true, Ordering::SeqCst);
                println!("Killed: {:?}", killed.load(Ordering::SeqCst));
                self.wake(None);
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use overseer::models::Value;

    use crate::database::watcher::{Watcher, WatcherBehaviour};


    #[tokio::test]
    pub async fn check_watcher_correctness_ordered() {
        let (client, server) = Watcher::new(WatcherBehaviour::Ordered);
        server.wake(None);
        server.wake(Some(Value::Integer(0).into()));
        assert!(client.wait().await.is_none());
        assert_eq!(client.wait().await.unwrap().as_integer().unwrap(), 0);
    }

    #[tokio::test]
    pub async fn check_watcher_correctness_eager() {
        let (client, server) = Watcher::new(WatcherBehaviour::Eager);
        server.wake(None);
        server.wake(Some(Value::Integer(0).into()));
        assert_eq!(client.wait().await.unwrap().as_integer().unwrap(), 0);
    }
}