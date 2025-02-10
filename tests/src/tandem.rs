

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use overseer::{access::{WatcherActivity, WatcherBehaviour}, models::{Key, Value}};
    use overseer_client::Client;
    use overseer_server::net::Driver;
    use tokio::sync::Notify;

    
    #[tokio::test]
    pub async fn test_client_server() {
        let td = tempfile::tempdir().unwrap();

        let driver = Driver::start("127.0.0.1:0", td.path(), "db").await.unwrap();
        let port = driver.port();

        // Launch a client, verify it starts off as none.
        let client = Client::new(format!("127.0.0.1:{port}")).await.unwrap();
        assert!(client.get(Key::from_str("hello")).await.unwrap().is_none());

        // Insert the key.
        client.insert(Key::from_str("hello"), Value::Integer(23)).await.unwrap();
        assert_eq!(client.get(Key::from_str("hello")).await.unwrap().unwrap(), Value::Integer(23));

        // Delete the key.
        client.delete(Key::from_str("hello")).await.unwrap();
        assert!(client.get(Key::from_str("hello")).await.unwrap().is_none());
    }

    #[tokio::test]
    pub async fn test_client_subscription() {
        let td = tempfile::tempdir().unwrap();

        let driver = Driver::start("127.0.0.1:0", td.path(), "db").await.unwrap();
        let port = driver.port();

        let signal = Arc::new(Notify::new());
        

        tokio::spawn({
            let signal = Arc::clone(&signal);
            async move {
                // Launch a client, verify it starts off as none.
                let client = Client::new(format!("127.0.0.1:{port}")).await.unwrap();
                signal.notify_waiters();

                // Update the value.
                let value = client.insert(Key::from_str("hello"), Value::Integer(2)).await.unwrap();
                assert_eq!(value, Some(Value::Integer(2)));

                // Delete the value.
                client.delete(Key::from_str("hello")).await.unwrap();
             
            }
        });

          // Launch a client, verify it starts off as none.
        let client = Client::new(format!("127.0.0.1:{port}")).await.unwrap();
        
        // Wait for the subscribe.
        signal.notified().await;
        let link = client.subscribe(Key::from_str("hello"), WatcherActivity::Lazy, WatcherBehaviour::Ordered).await.unwrap();
        
        // Check for notifications.
        assert_eq!(link.get().await, None);
        assert_eq!(link.wait_on_update().await, Some(Value::Integer(2)));
        assert_eq!(link.wait_on_update().await, None);
    }

    #[tokio::test]
    pub async fn test_client_connection_breakoff() {
        let td = tempfile::tempdir().unwrap();

        let driver = Driver::start("127.0.0.1:0", td.path(), "db").await.unwrap();
        let port = driver.port();

        // Launch a client, verify it starts off as none.
        let client = Client::new(format!("127.0.0.1:{port}")).await.unwrap();
        assert!(client.get(Key::from_str("hello")).await.unwrap().is_none());

        // Insert the key.
        client.insert(Key::from_str("hello"), Value::Integer(23)).await.unwrap();
        assert_eq!(client.get(Key::from_str("hello")).await.unwrap().unwrap(), Value::Integer(23));

        client.reset_connection().await.unwrap();

        // Insert the key.
        client.insert(Key::from_str("hello"), Value::Integer(25)).await.unwrap();
        assert_eq!(client.get(Key::from_str("hello")).await.unwrap().unwrap(), Value::Integer(25));

        // Delete the key.
        client.delete(Key::from_str("hello")).await.unwrap();
        assert!(client.get(Key::from_str("hello")).await.unwrap().is_none());

    }
}