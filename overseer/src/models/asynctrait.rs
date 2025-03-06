use std::{fmt::Debug, ops};

use monoio::buf::{IoBuf, IoBufMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};


// pub trait Bidirectional<O> {
//     fn forwards(self) -> O;
//     fn backwards(o: O) -> Self; 
// }

// impl<B, O> Bidirectional<B> for O
// where 
//     B: Bidirectional<O>
// {
//     fn forwards(self) -> B {
//         B::backwards(self)
//     }
//     fn backwards(o: B) -> Self {
//         B::forwards(o)
//     }

// }

// impl Bidirectional<IoBufferMut> for Vec<u8> {
//     fn backwards(o: IoBufferMut) -> Self {
//         if let IoBufferMut::Heap(h) = o {
//             h
//         } else {
//             panic!("Tried to flip an IO buffer illegally.")
//         }
//     }
//     fn forwards(self) -> IoBufferMut {
//         IoBufferMut::Heap(self)
//     }
// }

// impl<const N: usize> Bidirectional<IoBufferMut<N>> for [u8; N] {
//     fn backwards(o: IoBufferMut<N>) -> Self {
//         if let IoBufferMut::Slice(h) = o {
//             h
//         } else {
//             panic!("Tried to flip an IO buffer illegally.")
//         }
//     }
//     fn forwards(self) -> IoBufferMut<N> {
//         IoBufferMut::Slice(self)
//     }
// }


pub enum IoBufferMut<const N: usize = 0> {
    Heap(Vec<u8>),
    Slice(&'static [u8; N])
}

impl Into<IoBufferMut<0>> for Vec<u8> {
    fn into(self) -> IoBufferMut<0> {
        IoBufferMut::Heap(self)
    }
}
impl<const N: usize> Into<IoBufferMut<N>> for &'static [u8; N] {
    fn into(self) -> IoBufferMut<N> {
        IoBufferMut::Slice(self)
    }
}

impl<const N: usize> IoBufferMut<N> {
    pub fn slice(self) -> &'static [u8; N] {
        if let Self::Slice(s) = self {
            s
        } else {
            panic!("Illegal buffer conversion....");
        }
    }
    pub fn heap(self) -> Vec<u8> {
        if let Self::Heap(h) = self {
            h
        } else {
            panic!("Illegal buffer conversion....");
        }
    }
}




// pub trait IoBufferMut: Debug {
//     fn as_mut(&mut self) -> Option<&mut [u8]>;
// }

// impl<const N: usize> IoBufferMut for [u8; N] {
//     fn as_mut(&mut self) -> Option<&mut [u8]> {
//         Some(self)
//     }
// }

// impl IoBufferMut for Vec<u8> {
//     fn as_mut(&mut self) -> Option<&mut [u8]> {
//         Some(&mut *self)
//     }
// }

// unsafe impl<T: IoBufMut> IoBufferMut for T {
//     fn as_mut(&mut self) -> Option<&mut [u8]> {
//         unsafe { self.write_ptr() }
//     }
// }





#[async_trait::async_trait(?Send)]
pub trait LocalReadAsync: Sized {
    async fn read_exact(&mut self, buffer: Vec<u8>) -> std::io::Result<(Vec<u8>, usize)>;
    async fn read_u8(&mut self) -> std::io::Result<u8> {
        let single = [0u8; 1];
        let (single, _) = self.read_exact(single.to_vec()).await?;
        // println!("SINGLE: {:?}", single);
        Ok(single[0])
    }
    async fn read_u32(&mut self) -> std::io::Result<u32> {
        let (d, _) = self.read_exact(vec![0u8; 4]).await?;
        Ok(u32::from_be_bytes(d[0..4].try_into().unwrap()))
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




#[async_trait::async_trait(?Send)]
impl<S: AsyncReadExt + Unpin + Sized> LocalReadAsync for S {
    async fn read_exact(&mut self, mut buffer: Vec<u8>) -> std::io::Result<(Vec<u8>, usize)> {

        let r= AsyncReadExt::read_exact(self, buffer.as_mut()).await?;
        // println!("Read: {} {:?}", r, buffer);
        Ok((buffer, r))
    }
}

#[async_trait::async_trait(?Send)]
impl<S: AsyncWriteExt + Unpin + Sized> LocalWriteAsync for S {
    async fn write_all(&mut self, buffer: Vec<u8>) -> std::io::Result<()> {
        Ok(AsyncWriteExt::write_all(self, &buffer).await?)
    }
}