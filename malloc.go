package offheap

import (
	"fmt"
	"offheap/pkg/gommap"
	"os"
	"syscall"
)

// The MmapMalloc struct represents either an anonymous, private
// region of memory (if path was "", or a memory mapped file if
// path was supplied to Malloc() at creation.
//
// Malloc() creates and returns an MmapMalloc struct, which can then
// be later Free()-ed. Malloc() calls request memory directly
// from the kernel via mmap(). Memory can optionally be backed
// by a file for simplicity/efficiency of saving to disk.
//
// For use when the Go GC overhead is too large, and you need to move
// the hash table off-heap.
//
// mappmalloc结构体代表一个匿名的，私有的内存区域(如果path是""，或者一个内存映射文件，如果path在Malloc()创建时被提供。
// Malloc()创建并返回一个MmapMalloc结构体，该结构体随后可以被Free()。Malloc()通过mmap()直接从内核调用请求内存。内存可以选择由文件支持，以简化/高效地保存到磁盘。
// 当Go GC开销太大，你需要将散列表移出堆时使用。
type MmapMalloc struct {
	Path         string      // 文件路径
	File         *os.File    // 已打开的文件对象
	Fd           int         // 已打开的文件描述符
	FileBytesLen int64       // 文件映射的大小
	BytesAlloc   int64       // 保存非负长度的内存/文件大小
	MMap         gommap.MMap // equiv to Mem, just avoids casts everywhere.
	Mem          []byte      // equiv to Mmap
}

// TruncateTo enlarges or shortens the file backing the
// memory map to be size newSize bytes. It only impacts
// the file underlying the mapping, not
// the mapping itself at this point.
// TruncateTo将支持内存映射的文件放大或缩短为size newSize字节。
// 在这一点上，它只影响映射之下的文件，而不是映射本身。
func (mm *MmapMalloc) TruncateTo(newSize int64) {
	if mm.File == nil {
		panic("cannot call TruncateTo() on a non-file backed MmapMalloc.")
	}
	err := syscall.Ftruncate(int(mm.File.Fd()), newSize)
	if err != nil {
		panic(err)
	}
}

// Free releases the memory allocation back to the OS by removing
// the (possibly anonymous and private) memroy mapped file that
// was backing it. Warning: any pointers still remaining will crash
// the program if dereferenced.
// Free通过删除(可能是匿名和私有的)内存映射文件来释放内存分配给操作系统。
// 警告:如果取消引用，任何剩余的指针将导致程序崩溃。
func (mm *MmapMalloc) Free() {
	// 关闭文件
	if mm.File != nil {
		mm.File.Close()
	}
	err := mm.MMap.UnsafeUnmap()
	if err != nil {
		panic(err)
	}
}

// Malloc creates a new memory region that is provided directly
// by OS via the mmap() call, and is thus not scanned by the Go
// garbage collector.
//
// If path is not empty then we memory map to the given path.
// Otherwise it is just like a call to malloc(): an anonymous memory allocation,
// outside the realm of the Go Garbage Collector.
// If numBytes is -1, then we take the size from the path file's size. Otherwise
// the file is expanded or truncated to be numBytes in size. If numBytes is -1
// then a path must be provided; otherwise we have no way of knowing the size
// to allocate, and the function will panic.
//
// The returned value's .Mem member holds a []byte pointing to the returned memory (as does .MMap, for use in other gommap calls).
// Malloc()创建了一个新的内存区域，该内存区域是由操作系统通过mmap()调用直接提供的，因此不会被Go垃圾收集器扫描。
// 如果path不为空，则内存映射到给定的路径。
// 否则，它就像调用malloc():一个匿名的内存分配，超出了Go垃圾收集器的范围。
// 如果numBytes为-1，则取路径文件的大小。否则，文件将被扩展或截断为numBytes大小。如果numBytes为-1，则必须提供路径;否则，我们就无法知道要分配的大小，函数就会陷入恐慌。
// 返回值的.mem成员持有一个指向返回内存的[]byte指针(与.mmap一样，用于其他gommap调用)。
func Malloc(numBytes int64, path string) *MmapMalloc {

	// 返回的结构体
	mm := MmapMalloc{
		Path: path,
	}

	// MAP_SHARED
	// 分享该映射。映射的更新对映射此文件的其他进程可见，并被带到 底层文件。在调用msync（2） 或munmap（）之前，文件可能不会实际更新。
	// MAP_PRIVATE
	// 创建一个私人写时复制映射。映射的更新对映射相同文件的其他进程不可见，而且并不是 通向底层文件。
	// MAP_ANON
	// 表明进行的是匿名映射（不涉及具体的文件名，避免了文件的创建及打开，很显然只能用于具有亲缘关系的进程间通信）。

	flags := syscall.MAP_SHARED // 默认shared
	if path == "" {
		// 非文件映射
		flags = syscall.MAP_ANON | syscall.MAP_PRIVATE
		mm.Fd = -1

		if numBytes < 0 {
			panic("numBytes was negative but path was also empty: don't know how much to allocate!")
		}

	} else {
		// 是目录
		if dirExists(mm.Path) {
			panic(fmt.Sprintf("path '%s' already exists as a directory, so cannot be used as a memory mapped file.", mm.Path))
		}

		// 文件不存在，创建文件
		if !fileExists(mm.Path) {
			file, err := os.Create(mm.Path)
			if err != nil {
				panic(err)
			}
			mm.File = file
		} else {
			// 文件已存在，直接打开
			file, err := os.OpenFile(mm.Path, os.O_RDWR, 0777)
			if err != nil {
				panic(err)
			}
			mm.File = file
		}
		// 文件描述符
		mm.Fd = int(mm.File.Fd())
	}

	// 用户进程的内存页分为两种：
	// 与文件关联的内存（比如程序文件、数据文件所对应的内存页）
	// 与文件无关的内存（比如进程的堆栈，用 malloc 申请的内存）
	// 前者称为 file-backed pages，后者称为 anonymous pages。
	// File-backed pages 在发生换页(page-in 或 page-out)时，是从它对应的文件读入或写出；
	// anonymous pages 在发生换页时，是对交换区进行读 /写操作。

	sz := numBytes
	if path != "" {
		// file-backed memory
		if numBytes < 0 {
			// 由文件描述符取得文件状态
			var stat syscall.Stat_t
			if err := syscall.Fstat(mm.Fd, &stat); err != nil {
				panic(err)
			}
			sz = stat.Size

		} else {
			// set to the size requested
			// 参数fd指定的文件大小改为参数length指定的大小。
			err := syscall.Ftruncate(mm.Fd, numBytes)
			if err != nil {
				panic(err)
			}
		}
		mm.FileBytesLen = sz
	}
	// INVAR: sz holds non-negative length of memory/file.
	// sz保存非负长度的内存/文件长度
	mm.BytesAlloc = sz

	// 设置权限 可读可写
	prot := syscall.PROT_READ | syscall.PROT_WRITE

	vprintf("\n ------->> path = '%v',  mm.Fd = %v, with flags = %x, sz = %v,  prot = '%v'\n", path, mm.Fd, flags, sz, prot)

	// 创建内存映射
	var mmap []byte
	var err error
	if mm.Fd == -1 {
		flags = syscall.MAP_ANON | syscall.MAP_PRIVATE
		mmap, err = syscall.Mmap(-1, 0, int(sz), prot, flags)
	} else {
		flags = syscall.MAP_SHARED
		mmap, err = syscall.Mmap(mm.Fd, 0, int(sz), prot, flags)
	}
	if err != nil {
		panic(err)
	}

	// duplicate member to avoid casts all over the place.
	// 重复成员，以避免到处进行类型转换。
	mm.MMap = mmap
	mm.Mem = mmap

	return &mm
}

// BlockUntilSync returns only once the file is synced to disk.
// 只在文件同步到磁盘后返回。
func (mm *MmapMalloc) BlockUntilSync() {
	mm.MMap.Sync(gommap.MS_SYNC)
}

// BackgroundSync schedules a sync to disk, but may return before it is done.
// Without a call to either BackgroundSync() or BlockUntilSync(), there
// is no guarantee that file has ever been written to disk at any point before
// the munmap() call that happens during Free(). See the man pages msync(2)
// and mmap(2) for details.
// BackgroundSync()调度磁盘同步，但可能在完成之前返回。
// 如果没有调用BackgroundSync()或BlockUntilSync()，不能保证在Free()期间发生的munmap()调用之前，文件曾经被写入磁盘。
// 请参阅手册msync(2)和mmap(2)的详细信息。
// 进程在映射空间的对共享内容的改变并不直接写回到磁盘文件中，往往在调用munmap()后才执行该操作。
func (mm *MmapMalloc) BackgroundSync() {
	mm.MMap.Sync(gommap.MS_ASYNC)
}

// Growmap grows a memory mapping
func Growmap(oldmap *MmapMalloc, newSize int64) (newmap *MmapMalloc, err error) {

	if oldmap.Path == "" || oldmap.Fd <= 0 {
		return nil, fmt.Errorf("oldmap must be mapping to " +
			"actual file, so we can grow it")
	}
	if newSize <= oldmap.BytesAlloc || newSize <= oldmap.FileBytesLen {
		return nil, fmt.Errorf("mapping in Growmap must be larger than before")
	}

	newmap = &MmapMalloc{}
	newmap.Path = oldmap.Path
	newmap.Fd = oldmap.Fd

	// set to the size requested
	err = syscall.Ftruncate(newmap.Fd, newSize)
	if err != nil {
		panic(err)
		return nil, fmt.Errorf("syscall.Ftruncate to grow %v -> %v"+
			" returned error: '%v'", oldmap.BytesAlloc, newSize, err)
	}
	newmap.FileBytesLen = newSize
	newmap.BytesAlloc = newSize

	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_SHARED

	p("\n ------->> path = '%v',  newmap.Fd = %v, with flags = %x, sz = %v,  prot = '%v'\n", newmap.Path, newmap.Fd, flags, newSize, prot)

	mmap, err := syscall.Mmap(newmap.Fd, 0, int(newSize), prot, flags)
	if err != nil {
		panic(err)
		return nil, fmt.Errorf("syscall.Mmap returned error: '%v'", err)
	}

	// duplicate member to avoid casts all over the place.
	newmap.MMap = mmap
	newmap.Mem = mmap

	return newmap, nil
}
