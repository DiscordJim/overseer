use std::{net::SocketAddr, sync::Arc};

use tokio::{io::{AsyncRead, AsyncReadExt}, net::{TcpListener, TcpStream, ToSocketAddrs}};

use crate::database::Database;

use super::error::NetworkError;



pub struct Driver {
    internal: Arc<DriverInternal>
    
}

struct DriverInternal {
    database: Database,
    stream: TcpListener
}

impl DriverInternal {
    pub fn handle(&self, data: &[u8]) {

    }
}

impl Driver {
    pub async fn start<A: ToSocketAddrs>(addr: A) -> Result<Self, NetworkError> {

        let internal = Arc::new(DriverInternal {
            database: Database::new(),
            stream: TcpListener::bind(addr).await?
        });



        
        
        tokio::spawn({
            let internal = Arc::clone(&internal);
            async move {
                loop {
                    let (sock, addr) = internal.stream.accept().await?;
                    tokio::spawn(handle_client(sock, addr, Arc::clone(&internal)));
                }
                Ok::<(), NetworkError>(())
            }
        });


        Ok(Self {
            internal: Arc::clone(&internal)
        })
    }
}


async fn handle_client(socket: TcpStream, address: SocketAddr, internal: Arc<DriverInternal>) {
    
}
