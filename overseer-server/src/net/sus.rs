

#[cfg(test)]
mod tests {

    #[monoio::test]
    pub async fn bro() {
        monoio::fs::write("plain.txt", "hello how are you?".as_bytes()).await.0.unwrap();

    }
}