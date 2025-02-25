use std::{cell::{Cell, RefCell, UnsafeCell}, collections::VecDeque, future::Future, marker::PhantomData, ops::Deref, rc::Rc, sync::{atomic::{AtomicBool, Ordering}, Arc, Mutex}, task::{LocalWaker, Poll, RawWaker, RawWakerVTable, Waker}};
use overseer::{access::WatcherBehaviour, models::Value};




pub struct WatchServer;
pub struct WatchClient;

enum HoldingInner {
    /// An ordered watcher returns things in the order of
    /// which they came.
    Ordered(RefCell<VecDeque<Option<Arc<Value>>>>),
    /// An eager watcher does not care for this.
    Eager(RefCell<Option<Arc<Value>>>)

}



struct WatcherInner {
    inner: HoldingInner,
    wakeup: UnsafeCell<Option<LocalWaker>>,
    /// If the watcher is dead.
    killed: Cell<bool>,
    
    /// If we can wakeup.
    ready: Cell<bool>
}

impl Future for &WatcherInner {
    type Output = ();
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        // If we are ready to go, unset and let's go!
        if self.ready.get() {
            self.ready.set(false);
            return Poll::Ready(())
        }

        if unsafe { &*self.wakeup.get() }.is_none() {
            // If there is no waker then set it.
            *unsafe { &mut *self.wakeup.get() } = Some(cx.local_waker().clone());
            // Re-poll the future.
            self.poll(cx)
        } else {
            // We have a waker and are just waiting for the flag to be set.
            Poll::Pending
        }
    }
}

impl WatcherInner {
    pub fn wake(&self) {
        self.ready.set(true);
        if let Some(inner) = unsafe { &mut *self.wakeup.get() }.take() {
            inner.wake();
        }
    }
}

/// The [Watcher] struct lets us notify subscribers of changes.
pub struct Watcher<S> {
    /// The inner structure of the watcher.
    inner: Rc<WatcherInner>,
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
            signals.push(Rc::clone(&watch_ref.inner));
        }

        // Trigger all the watchers.
        for signal in signals {
            signal.wake();
        }
    }
}

impl Watcher<()> {
    /// Returns a split watcher. One of these is for
    /// the client and there other is for the server.
    pub fn new(class: WatcherBehaviour) -> (Watcher<WatchClient>, Watcher<WatchServer>) {


        let inner = Rc::new(WatcherInner {
            inner: match class {
                WatcherBehaviour::Eager => HoldingInner::Eager(RefCell::default()),
                WatcherBehaviour::Ordered => HoldingInner::Ordered(RefCell::default()),
            },
            killed: Cell::new(false),
            wakeup: UnsafeCell::new(None),
            ready: Cell::new(false)
        });

        (
            Watcher {
                inner: Rc::clone(&inner),
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
                value.borrow_mut().take()
            },
            HoldingInner::Ordered(value) => {
                value.borrow_mut().pop_front()?
            }
        } 
    }
    pub async fn wait(&self) -> Option<Arc<Value>> {
        match &self.inner.inner {
            HoldingInner::Eager(value) => {
                if value.borrow_mut().is_some() {
                    value.borrow_mut().take()
                } else {
                    (&*self.inner).await;
                    value.borrow_mut().take()
                }
            },
            HoldingInner::Ordered(value) => {
                if !value.borrow().is_empty() {
                    value.borrow_mut().pop_front()?
                } else {
                    (&*self.inner).await;
                    value.borrow_mut().pop_front()?
                }
            }
        }
    }
    pub fn is_killed(&self) -> bool {
        self.inner.killed.get()
    }
    
}



impl Watcher<WatchServer> {
    fn wake_without_notify(&self, nvalue: Option<Arc<Value>>) {
        match &self.inner.inner {
            HoldingInner::Eager(value) => {
                *value.borrow_mut() = nvalue;
                
            },
            HoldingInner::Ordered(value) => {
                
                value.borrow_mut().push_back(nvalue);
            }
        }
    }
    pub fn wake(&self, nvalue: Option<Arc<Value>>) {
        self.wake_without_notify(nvalue);
        self.inner.wake();
    }
    pub fn kill(&self) {
        self.inner.killed.set(true);
        self.wake(None);
    }
}


#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use overseer::models::Value;

    use crate::database::watcher::{Watcher, WatcherBehaviour};


    #[monoio::test]
    pub async fn check_watcher_correctness_ordered() {
        let (client, server) = Watcher::new(WatcherBehaviour::Ordered);
        server.wake(None);
        server.wake(Some(Value::Integer(0).into()));
        assert!(client.wait().await.is_none());
    }

    #[monoio::test(enable_timer = true)]
    pub async fn test_wakeup_mechanism_basic() {
        // Configure an eager watcher. We will do a basic two-shot receive.
        let (client, server) = Watcher::new(WatcherBehaviour::Ordered);
        monoio::spawn(async move {
            server.wake(Some(Arc::new(Value::Integer(2))));
            server.wake(Some(Arc::new(Value::Integer(4))));
        });
        assert_eq!(&*client.wait().await.unwrap(), &Value::Integer(2));
        assert_eq!(&*client.wait().await.unwrap(), &Value::Integer(4));
    }

    #[monoio::test(enable_timer = true)]
    pub async fn test_wakeup_mechanism_twotailed() {
        // Configure an eager watcher. We will do a basic two-shot receive.
        let (client_a, server_a) = Watcher::new(WatcherBehaviour::Ordered);
        let (client_b, server_b) = Watcher::new(WatcherBehaviour::Ordered);
        monoio::spawn(async move {
            server_a.wake(Some(Arc::new(Value::Integer(2))));
            assert_eq!(&*client_b.wait().await.unwrap(), &Value::Integer(3));
            server_a.wake(Some(Arc::new(Value::Integer(5))));
        });
        assert_eq!(&*client_a.wait().await.unwrap(), &Value::Integer(2));
        server_b.wake(Some(Arc::new(Value::Integer(3))));
        assert_eq!(&*client_a.wait().await.unwrap(), &Value::Integer(5));
    }

    #[monoio::test]
    pub async fn check_watcher_correctness_eager() {
        let (client, server) = Watcher::new(WatcherBehaviour::Eager);
        server.wake(None);
        server.wake(Some(Value::Integer(0).into()));
        assert_eq!(client.wait().await.unwrap().as_integer().unwrap(), 0);
    }

    #[monoio::test]
    pub async fn check_watcher_notify_synchronize() {
        let (client_1, server_1) = Watcher::new(WatcherBehaviour::Eager);
        let (client_2, server_2) = Watcher::new(WatcherBehaviour::Eager);
        

        Watcher::notify_coordinated([server_1, server_2].iter(), Some(Arc::new(Value::Integer(45))));

        assert_eq!(client_1.wait().await.unwrap().as_integer().unwrap(), 45);
        assert_eq!(client_2.wait().await.unwrap().as_integer().unwrap(), 45);
        
    }

    /// This test checks if notifications actually work.
    #[monoio::test]
    pub async fn check_watcher_notify_integrity() {
        let (client_1, server_1) = Watcher::new(WatcherBehaviour::Eager);
        server_1.wake_without_notify(Some(Arc::new(Value::Integer(2))));
        assert_eq!(client_1.wait().await.unwrap().as_integer().unwrap(), 2);

        server_1.wake(Some(Arc::new(Value::Integer(4))));
        assert_eq!(client_1.wait().await.unwrap().as_integer().unwrap(), 4);

    }


}