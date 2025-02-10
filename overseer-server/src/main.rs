

fn main() {

    tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .with_target(false)
    .init();
    tracing::info!("Hello");
    
    println!("Hello, world!");
}
