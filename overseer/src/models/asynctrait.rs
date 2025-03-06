use std::{future::Future, io::{Cursor, Read}, pin::Pin, task::{Context, Poll}};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};

use crate::error::NetworkError;







#[async_trait::async_trait(?Send)]
pub trait LocalReadAsync: Sized {
    async fn read_exact<T: AsMut<[u8]>>(&mut self, mut buffer: T) -> std::io::Result<usize>;
    async fn read_u8(&mut self) -> std::io::Result<u8> {
        let mut single = [0u8; 1];
        self.read_exact(&mut single).await?;
        Ok(single[0])
    }
    async fn read_u32(&mut self) -> std::io::Result<u32> {
        let mut single = [0u8; 4];
        self.read_exact(&mut single).await?;
        Ok(u32::from_be_bytes(single))
    }
}


#[async_trait::async_trait(?Send)]
pub trait LocalWriteAsync {
    async fn write_all(&mut self, buffer: Vec<u8>) -> std::io::Result<()>;
    async fn write_u8(&mut self, data: u8) -> std::io::Result<()> {
        self.write_all([data].to_vec()).await?;
        Ok(())
    }
    async fn write_u32(&mut self, data: u32) -> std::io::Result<()> {
        self.write_all(data.to_be_bytes().to_vec()).await?;
        Ok(())
    }
}


// impl LocalReadAsync for Cursor<Vec<u8>> {
//     async fn read_exact<T: AsMut<[u8]>>(&mut self, mut buffer: T) -> Result<usize, NetworkError> {
//         Read::read_exact(self, buffer.as_mut())?;
//         Ok(0)
//     }
// }

#[async_trait::async_trait(?Send)]
impl<S: AsyncReadExt + Unpin + Sized> LocalReadAsync for S {
    async fn read_exact<T: AsMut<[u8]>>(&mut self, mut buffer: T) -> std::io::Result<usize> {
        Ok(AsyncReadExt::read_exact(self, buffer.as_mut()).await?)
    }
    // async fn read_exact<B: AsMut<[u8]>>(&mut self, mut buffer: B) -> Result<usize, NetworkError> {
    //     Ok(AsyncReadExt::read_exact(self, buffer.as_mut()).await?)
    // }
    // fn read_exact<J: AsMut<[u8]>>(&mut self, mut buffer: J) -> impl Future<Output = Result<usize, NetworkError>> {
    //     AsyncReadExt::read_exact(self, buffer.as_mut())
    // }
}

#[async_trait::async_trait(?Send)]
impl<S: AsyncWriteExt + Unpin + Sized> LocalWriteAsync for S {
    async fn write_all(&mut self, buffer: Vec<u8>) -> std::io::Result<()> {
        Ok(AsyncWriteExt::write_all(self, &buffer).await?)
    }
    // async fn read_exact<B: AsMut<[u8]>>(&mut self, mut buffer: B) -> Result<usize, NetworkError> {
    //     Ok(AsyncReadExt::read_exact(self, buffer.as_mut()).await?)
    // }
    // fn read_exact<J: AsMut<[u8]>>(&mut self, mut buffer: J) -> impl Future<Output = Result<usize, NetworkError>> {
    //     AsyncReadExt::read_exact(self, buffer.as_mut())
    // }
}