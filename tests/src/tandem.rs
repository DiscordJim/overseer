

#[cfg(test)]
mod tests {
    use overseer::models::{Key, Value};
    use overseer_client::Client;
    use overseer_server::net::Driver;

    
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

        tokio::spawn({

            async move {

            }
        });

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
}