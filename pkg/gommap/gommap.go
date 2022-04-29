// This package offers the MMap type that manipulates a memory mapped file or
// device.
//
// IMPORTANT NOTE (1): The MMap type is backed by an unsafe memory region,
// which is not covered by the normal rules of Go's memory management. If a
// slice is taken out of it, and then the memory is explicitly unmapped through
// one of the available methods, both the MMap value itself and the slice
// obtained will now silently point to invalid memory.  Attempting to access
// data in them will crash the application.
package gommap

import (
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

// The MMap type represents a memory mapped file or device. The slice offers
// direct access to the memory mapped content.
//
// IMPORTANT: Please see note in the package documentation regarding the way
// in which this type behaves.
type MMap []byte

// Map creates a new mapping in the virtual address space of the calling process.
// This function will attempt to map the entire file by using the fstat system
// call with the provided file descriptor to discover its length.
func Map(fd uintptr, prot ProtFlags, flags MapFlags) (MMap, error) {
	mmap, err := MapAt(0, fd, 0, -1, prot, flags)
	return mmap, err
}

// MapRegion creates a new mapping in the virtual address space of the calling
// process, using the specified region of the provided file or device. If -1 is
// provided as length, this function will attempt to map until the end of the
// provided file descriptor by using the fstat system call to discover its
// length.
func MapRegion(fd uintptr, offset, length int64, prot ProtFlags, flags MapFlags) (MMap, error) {
	mmap, err := MapAt(0, fd, offset, length, prot, flags)
	return mmap, err
}

// MapAt creates a new mapping in the virtual address space of the calling
// process, using the specified region of the provided file or device. The
// provided addr parameter will be used as a hint of the address where the
// kernel should position the memory mapped region. If -1 is provided as
// length, this function will attempt to map until the end of the provided
// file descriptor by using the fstat system call to discover its length.
func MapAt(addr uintptr, fd uintptr, offset, length int64, prot ProtFlags, flags MapFlags) (MMap, error) {
	if length == -1 {
		var stat syscall.Stat_t
		if err := syscall.Fstat(int(fd), &stat); err != nil {
			return nil, err
		}
		length = stat.Size
	}
	addr, err := mmap_syscall(addr, uintptr(length), uintptr(prot), uintptr(flags), fd, offset)
	if err != syscall.Errno(0) {
		return nil, err
	}
	mmap := MMap{}

	dh := (*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	dh.Data = addr
	dh.Len = int(length) // Hmmm.. truncating here feels like trouble.
	dh.Cap = dh.Len
	return mmap, nil
}

// UnsafeUnmap deletes the memory mapped region defined by the mmap slice. This
// will also flush any remaining changes, if necessary.  Using mmap or any
// other slices based on it after this method has been called will crash the
// application.
// UnsafeUnmap删除mmap片定义的内存映射区域。如果需要，这还将刷新任何剩余的更改。在调用该方法之后使用mmap或基于它的任何其他片将导致应用程序崩溃。
func (mmap MMap) UnsafeUnmap() error {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	// 参数是起始地址，以及长度
	// syscall.Munmap(mmap)
	_, _, err := syscall.Syscall(syscall.SYS_MUNMAP, uintptr(rh.Data), uintptr(rh.Len), 0)
	if err != 0 {
		return err
	}
	return nil
}

// Sync flushes changes made to the region determined by the mmap slice
// back to the device. Without calling this method, there are no guarantees
// that changes will be flushed back before the region is unmapped.  The
// flags parameter specifies whether flushing should be done synchronously
// (before the method returns) with MS_SYNC, or asynchronously (flushing is just
// scheduled) with MS_ASYNC.
// Sync将由mmap确定的区域所做的更改刷新回设备。
// 如果不调用此方法，则不能保证在取消映射区域之前将更改刷新回设备。
// flags参数指定使用MS_SYNC是同步(在方法返回之前)刷新，还是使用MS_ASYNC异步(只是计划刷新)。
func (mmap MMap) Sync(flags SyncFlags) error {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	_, _, err := syscall.Syscall(syscall.SYS_MSYNC, uintptr(rh.Data), uintptr(rh.Len), uintptr(flags))
	if err != 0 {
		return err
	}
	return nil
}

// Advise advises the kernel about how to handle the mapped memory
// region in terms of input/output paging within the memory region
// defined by the mmap slice.
// Advise建议内核根据mmap片定义的内存区域内的输入/输出分页来处理映射的内存区域。
// https://www.jianshu.com/p/965b1ed71ae4
func (mmap MMap) Advise(advice AdviseFlags) error {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	_, _, err := syscall.Syscall(syscall.SYS_MADVISE, uintptr(rh.Data), uintptr(rh.Len), uintptr(advice))
	if err != 0 {
		return err
	}
	return nil
}

// Protect changes the protection flags for the memory mapped region
// defined by the mmap slice.
// Protect改变mmap片定义的内存映射区域的保护标志。
// PROT_NONE PROT_READ PROT_WRITE PROT_EXEC
func (mmap MMap) Protect(prot ProtFlags) error {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	_, _, err := syscall.Syscall(syscall.SYS_MPROTECT, uintptr(rh.Data), uintptr(rh.Len), uintptr(prot))
	if err != 0 {
		return err
	}
	return nil
}

// Lock locks the mapped region defined by the mmap slice,
// preventing it from being swapped out.
// Lock锁定mmap片定义的映射区域，防止它被交换出去。
func (mmap MMap) Lock() error {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	_, _, err := syscall.Syscall(syscall.SYS_MLOCK, uintptr(rh.Data), uintptr(rh.Len), 0)
	if err != 0 {
		return err
	}
	return nil
}

// Unlock unlocks the mapped region defined by the mmap slice,
// allowing it to swap out again.
// Unlock解锁由mmap片定义的映射区域，允许它再次换出。
func (mmap MMap) Unlock() error {
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	_, _, err := syscall.Syscall(syscall.SYS_MUNLOCK, uintptr(rh.Data), uintptr(rh.Len), 0)
	if err != 0 {
		return err
	}
	return nil
}

// IsResident returns a slice of booleans informing whether the respective
// memory page in mmap was mapped at the time the call was made.
// IsResident返回一个布尔值片，通知mmap中对应的内存页在调用时是否被映射。
func (mmap MMap) IsResident() ([]bool, error) {
	pageSize := os.Getpagesize()                            // 返回底层的系统内存页的大小。
	result := make([]bool, (len(mmap)+pageSize-1)/pageSize) // 页的数量
	rh := *(*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	resulth := *(*reflect.SliceHeader)(unsafe.Pointer(&result))
	_, _, err := syscall.Syscall(syscall.SYS_MINCORE, uintptr(rh.Data), uintptr(rh.Len), uintptr(resulth.Data))
	for i := range result {
		*(*uint8)(unsafe.Pointer(&result[i])) &= 1
	}
	if err != 0 {
		return nil, err
	}
	return result, nil
}
