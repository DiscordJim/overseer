use std::{collections::VecDeque, sync::{Arc, Mutex}};

use overseer::models::Value;
use tokio::sync::Notify;



#[derive(Clone)]
pub struct Watcher {
    triggered: Arc<Mutex<VecDeque<Option<Arc<Value>>>>>,
    wakeup: Arc<Notify>
}

impl Watcher {
    pub fn new() -> Self {
        Self {
            triggered: Arc::default(),
            wakeup: Arc::default()
        }
    }
    pub async fn wait(&self) -> Option<Arc<Value>> {
        if !self.triggered.lock().unwrap().is_empty() {
            self.triggered.lock().unwrap().pop_front()?
        } else {
            self.wakeup.notified().await;
            let popped = self.triggered.lock().unwrap().pop_front()?;
            popped
        }
    }
    pub fn wake(&self, value: Option<Arc<Value>>) {
        self.triggered.lock().unwrap().push_back(value);
        self.wakeup.notify_one();
    }
}


#[cfg(test)]
mod tests {
    use overseer::models::Value;

    use crate::database::watcher::Watcher;


    #[tokio::test]
    pub async fn check_watcher_correctness() {
        let watcher = Watcher::new();
        watcher.wake(None);
        watcher.wake(Some(Value::Integer(0).into()));
        assert!(watcher.wait().await.is_none());
        assert_eq!(watcher.wait().await.unwrap().as_integer().unwrap(), 0);
    }
}