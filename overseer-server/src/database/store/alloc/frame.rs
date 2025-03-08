use core::slice;
use std::{cell::Cell, marker::PhantomData, mem::{self, ManuallyDrop}, ops::{Deref, DerefMut}, rc::{Rc, Weak}};

use monoio::buf::{IoBuf, IoBufMut};

use super::error::FrameAllocatorError;

pub struct FrameAllocator {
    /// This is wrapped in an unsafe cell because directly
    /// accessing this array is incredibly dangerous.
    buffer: Box<[u8]>,
    frames: usize,
    size: usize,
    

    /// The permits array prevents frames from being
    /// checked out multiple times.
    permits: Vec<Rc<Cell<bool>>>
}

pub struct BoxPtr<'a> {
    pointer: *mut u8,
    length: usize,
    _life: PhantomData<&'a ()>
}

unsafe impl IoBuf for BoxPtr<'static> {
    fn read_ptr(&self) -> *const u8 {
        self.pointer as *const u8
    }
    fn bytes_init(&self) -> usize {
        self.length
    }
}

unsafe impl IoBufMut for BoxPtr<'static> {
    fn bytes_total(&mut self) -> usize {
        self.length
    }
    fn write_ptr(&mut self) -> *mut u8 {
        self.pointer
    }
    unsafe fn set_init(&mut self, pos: usize) {}
}


type Result<O> = core::result::Result<O, FrameAllocatorError>;

impl FrameAllocator {
    pub fn new(frames: usize, size: usize) -> Result<Self> {
        if size % 2 != 0 {
            return Err(FrameAllocatorError::BadFrameSize);
        }

   

   


        Ok(Self {
            buffer: vec![0u8; frames * size].into_boxed_slice(),
            frames,
            permits: vec![Rc::new(Cell::new(false)); frames],
            size
        })
    }
    fn available(&self, index: usize) -> bool {
        !self.permits[index].get()
    }
    pub fn get_frame<'a, 'b: 'a>(&'b self, index: usize) -> Result<Frame<'a>> {
        if index > self.frames {
            // check if the index is within bounds.
            return Err(FrameAllocatorError::FrameOutOfBounds);
        } else if !self.available(index) {
            return Err(FrameAllocatorError::FrameInUse);
        } else {
            let start = index * self.size;

         
            

            // let cell = Rc::new(Cell::new(true));
            self.permits[index].set(true);

            Ok(Frame {
                buffer: ManuallyDrop::new(BoxPtr {
                    pointer: self.buffer[start..].as_ptr() as *mut u8,
                    length: self.size,
                    _life: PhantomData
                }),
                license: self.permits[index].clone()
            })
            
        }
    }
}


pub struct Frame<'a> {
    buffer: ManuallyDrop<BoxPtr<'a>>,
    license: Rc<Cell<bool>>
}


impl Drop for Frame<'_> {
    fn drop(&mut self) {
        self.license.set(false);
        self.fill(0);
    }
}

impl Deref for Frame<'_> {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        unsafe { slice::from_raw_parts(self.buffer.pointer as *const u8, self.buffer.length) }
    }
}

impl DerefMut for Frame<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
    
        unsafe { slice::from_raw_parts_mut(self.buffer.pointer, self.buffer.length) }
    }
}


#[cfg(test)]
mod tests {
    use crate::database::store::alloc::error::FrameAllocatorError;

    use super::FrameAllocator;



    #[test]
    pub fn test_make_allocator() {
        let frames = FrameAllocator::new(2, 2).unwrap();

        let mut frame = frames.get_frame(0).unwrap();
        assert_eq!(&*frame, &[0, 0]);
        frame[0] = 1;
        assert_eq!(&*frame, &[1, 0]);
        assert!(matches!(frames.get_frame(0).err().unwrap(), FrameAllocatorError::FrameInUse));

        drop(frame);

        let frame = frames.get_frame(0).unwrap();
        assert_eq!(&*frame, &[0, 0]);

        // drop(frames);

        

    }
}