package offheap

import (
	"encoding/binary"
	"fmt"
	"unsafe"
)

//go:generate msgp

// Copyright (C) 2015 by Jason E. Aten, Ph.D.

// MetadataHeaderMaxBytes metadata serialization size can never grow bigger
// than MetadataHeaderMaxBytes (currently one 4K page), without impacting
// backwards compatibility. We reserve this many bytes
// at the beginning of the memory mapped file for the metadata.
// 元数据序列化的大小永远不能超过MetadataHeaderMaxBytes(目前一个4K页面)，而不会影响向后兼容性。我们在内存映射文件的开头为元数据保留了这么多字节。
const MetadataHeaderMaxBytes = 4096

// HashTable represents the off-heap hash table.
// Create a new one with NewHashTable(), then use
// Lookup(), Insert(), and DeleteKey() on it.
// HashTable is meant to be customized by the
// user, to reflect your choice of key and value
// types. See StringHashTable and ByteKeyHashTable
// for examples of this specialization process.
// HashTable表示堆外哈希表。
// 使用NewHashTable()创建一个新的表，然后对其使用Lookup()， Insert()和DeleteKey()。
// HashTable是由用户定制的，以反映你对键和值类型的选择。参见StringHashTable和ByteKeyHashTable以了解这种专门化过程的例子。
type HashTable struct {
	// 固定值
	MagicNumber   int        // distinguish between reading from empty file versus an on-disk HashTable.
	Cells         uintptr    `msg:"-"` // OffheapCells[0]的位置 起始位置，用来计算cell的位置下标
	CellSz        uint64     // Cell类型占用的字节数
	ArraySize     uint64     // 存储的元素最大数量
	Population    uint64     // 已经存储的数量
	ZeroUsed      bool       // 0号位是否已使用
	ZeroCell      Cell       // 0号位数据
	OffheapHeader []byte     `msg:"-"` // 堆外内存的起始地址
	OffheapCells  []byte     `msg:"-"` // 堆外内存的cell元素起始地址
	Mmm           MmapMalloc `msg:"-"` // 堆外内存区域
}

// NewHashTable Create a new hash table, able to hold initialSize count of keys.
// 创建一个新的哈希表
func NewHashTable(initialSize int64) *HashTable {
	return NewHashFileBacked(initialSize, "")
}

func NewHashFileBacked(initialSize int64, filepath string) *HashTable {
	//fmt.Printf("\n\n  NewHashFileBacked called!\n\n")
	t := HashTable{
		MagicNumber: 3030675468910466832,
		CellSz:      uint64(unsafe.Sizeof(Cell{})), // 返回类型v本身数据所占用的字节数
	}

	// off-heap and off-gc version
	if initialSize < 0 {
		t.Mmm = *Malloc(-1, filepath) // 如果不指定filepath会panic
	} else {
		t.ArraySize = uint64(initialSize)
		t.Mmm = *Malloc(t.bytesFromArraySizeAndHeader(t.ArraySize), filepath)
	}
	//p("&t.Mmm.Mem[0] = %p\n", &t.Mmm.Mem[0])
	t.OffheapHeader = t.Mmm.Mem
	t.OffheapCells = t.Mmm.Mem[MetadataHeaderMaxBytes:]     // 空出来MetadataHeaderMaxBytes个字节用来存储元数据
	t.Cells = (uintptr)(unsafe.Pointer(&t.OffheapCells[0])) // 第0个cell位置

	// test deserialize
	// 从header中的元数据中反序列化数据，文件映射
	t2 := HashTable{}
	_, err := t2.UnmarshalMsg(t.OffheapHeader)
	if err != nil {
		//fmt.Printf("UnmarshalMsg err = %v\n", err)
		// common to not be able to deserialize 0 bytes, don't worry
		// 一般不能反序列化0字节，不用担心
	} else {
		//fmt.Printf("\n deserialized okay! t2=%#v\n t=%#v\n", t2, t)
		// no error during de-serialize; okay but verify our MagicNumer too.
		// 反序列化期间没有错误;但也要验证我们的magicnumber。
		if t2.MagicNumber == t.MagicNumber {
			// okay to copy in, we are restoring an existing table from disk
			// 好的，复制进来，我们正在从磁盘恢复一个现有的表
			t.Population = t2.Population
			t.ZeroUsed = t2.ZeroUsed
			t.ZeroCell = t2.ZeroCell
		}
	}
	// old: off-gc but still on-heap version
	//	t.ArraySize = initialSize
	//	t.Offheap = make([]byte, t.ArraySize*t.CellSz)
	//	t.Cells = (uintptr)(unsafe.Pointer(&t.Offheap[0]))

	// on-heap version:
	//Cells:     make([]Cell, initialSize),

	return &t
}

// Key_t is the basic type for keys. Users of the library will
// probably redefine this.
// 是键的基本类型。库的用户可能会重新定义它。
type Key_t [64]byte

// Val_t is the basic type for values stored in the cells in the table.
// Users of the library will probably redefine this to be a different
// size at the very least.
// 是存储在表中单元格中的值的基本类型。
// 标准库的用户可能会重新定义它，使其至少具有不同的大小。
type Val_t [56]byte

// Cell is the basic payload struct, stored inline in the HashTable. The
// cell is returned by the fundamental Lookup() function. The member
// Value is where the value that corresponds to the key (in ByteKey)
// is stored. Both the key (in ByteKey) and the value (in Value) are
// stored inline inside the hashtable, so that all storage for
// the hashtable is in the same offheap segment. The uint64 key given
// to fundamental Insert() method is stored in UnHashedKey. The hashed value of the
// UnHashedKey is not stored in the Cell, but rather computed as needed
// by the basic Insert() and Lookup() methods.
// Cell是基本的有效负载结构，内联存储在哈希表中。
// cell由基本的Lookup()函数返回。
// 成员Value是与键(在ByteKey中)对应的值存储的地方。
// 键(在ByteKey中)和值(在value中)都内联存储在哈希表中，这样哈希表的所有存储都在同一个堆外段中。
// 给Insert()方法的uint64的键值存储在unhashhedkey中。
// UnHashedKey的散列值不存储在Cell中，而是根据基本的Insert()和Lookup()方法的需要进行计算。
type Cell struct {
	UnHashedKey uint64
	ByteKey     Key_t
	Value       Val_t // customize this to hold your value's data type entirely here. 自定义它以完全保存值的数据类型。
}

/*
SetValue stores any value v in the Cell. Note that
users of the library will need to extend this for
their type. Only strings of length less than 56,
and integers are handled by default.
*/
// SetValue在cell中存储任何值v。注意，库的用户需要为他们的类型扩展这个。默认情况下，只处理长度小于56的字符串和整数。
func (cell *Cell) SetValue(v interface{}) {
	switch a := v.(type) {
	case string:
		cell.SetString(a)
	case int:
		cell.SetInt(a)
	default:
		panic("unsupported type")
	}
}

// ZeroValue sets the cell's value to all zeros.
// 将单元格的值全部设置为零。
func (cell *Cell) ZeroValue() {
	for i := range cell.Value[:] {
		cell.Value[i] = 0
	}
}

// SetString stores string s (up to val_t length, currently 56 bytes) in cell.Value.
// 将字符串s(最大长度为val_t，目前为56字节)存储在cell.Value中。
func (cell *Cell) SetString(s string) {
	copy(cell.Value[:], []byte(s))
}

// GetString retreives a string value from the cell.Value.
func (cell *Cell) GetString() string {
	return string([]byte(cell.Value[:]))
}

// SetInt stores an integer value in the cell.
// 在单元格中存储整数值。
func (cell *Cell) SetInt(n int) {
	binary.LittleEndian.PutUint64(cell.Value[:8], uint64(n))
}

// GetInt retreives an integer value from the cell.
func (cell *Cell) GetInt() int {
	return int(binary.LittleEndian.Uint64(cell.Value[:8]))
}

// SetInt sets an int value for Val_t v.
func (v *Val_t) SetInt(n int) {
	binary.LittleEndian.PutUint64((*v)[:8], uint64(n))
}

// GetInt gets an int value for Val_t v.
func (v *Val_t) GetInt() int {
	return int(binary.LittleEndian.Uint64((*v)[:8]))
}

// SetString sets a string value for Val_t v.
func (v *Val_t) SetString(s string) {
	copy((*v)[:], []byte(s))
}

// GetString retreives a string value for Val_t v.
func (v *Val_t) GetString() string {
	return string([]byte((*v)[:]))
}

// Save syncs the memory mapped file to disk using MmapMalloc::BlockUntilSync().
// If background is true, the save using BackgroundSync() instead of blocking.
// 使用MmapMalloc::BlockUntilSync()保存内存映射文件到磁盘。
// 如果background为true，保存使用BackgroundSync()而不是阻塞。
func (t *HashTable) Save(background bool) error {
	// 序列话头信息
	bts, err := t.MarshalMsg(nil)
	if err != nil {
		return fmt.Errorf("offheap.HashTable.Save() error: serialization error from msgp.MarshalMsg(): '%s'", err)
	}

	// 头信息长度超过标准长度
	if len(bts) > MetadataHeaderMaxBytes {
		return fmt.Errorf("offheap.HashTable.Save() error: serialization too long: len(bts)==%d is > MetadataHeaderMaxBytes==%d, so serializing the HashTable metadata would overwrite the cell contents; rather than corrupting the cell data we are returning an error here.", len(bts), MetadataHeaderMaxBytes)
	}

	// 复制到t.OffheapHeader
	nw := copy(t.OffheapHeader, bts)
	_ = nw
	//fmt.Printf("saved bts header of size %v by writing %v = '%s'. t.OffheapHeader = %p, t.OffheapCells = %p, &t.Mmm.Mem[0] = %p\n", len(bts), nw, string(bts), &t.OffheapHeader[0], &t.OffheapCells[0], &t.Mmm.Mem[0])

	// 写入文件
	if background {
		t.Mmm.BackgroundSync()
		//fmt.Printf("\ndone with call to initiate background sync.\n")
	} else {
		t.Mmm.BlockUntilSync()
		//fmt.Printf("\ndone with block until sync\n")
	}
	return nil
}

// CellAt CellAt: fetch the cell at a given index. E.g. t.CellAt(pos) replaces t.Cells[pos]
// 在给定索引处获取单元格。例如:t.CellAt(pos)取代t.Cells[pos]
func (t *HashTable) CellAt(pos uint64) *Cell {

	// off heap version
	return (*Cell)(unsafe.Pointer(uintptr(t.Cells) + uintptr(pos*t.CellSz)))

	// on heap version, back when t.Cells was []Cell
	//return &(t.Cells[pos])
}

// DestroyHashTable frees the memory-mapping, returning the
// memory containing the hash table and its cells to the OS.
// By default the save-to-file-on-disk functionality in malloc.go is
// not used, but that can be easily activated. See malloc.go.
// Deferencing any cells/pointers into the hash table after
// destruction will result in crashing your process, almost surely.
// DestroyHashTable释放内存映射，将包含哈希表及其单元格的内存返回给OS。
// 默认情况下，malloc中的“保存到磁盘上文件”功能。Go没有被使用，但是可以很容易的被激活。看malloc.go。
// 几乎可以肯定的是，在销毁后将任何单元格/指针延迟到哈希表中会导致进程崩溃。
func (t *HashTable) DestroyHashTable() {
	t.Mmm.Free()
}

// Lookup a cell based on a uint64 key value. Returns nil if key not found.
// 根据uint64的键值查找单元格。如果没有找到键，则返回nil。
func (t *HashTable) Lookup(key uint64) *Cell {

	var cell *Cell

	if key == 0 {
		if t.ZeroUsed {
			return &t.ZeroCell
		}
		return nil

	} else {
		//p("for t = %p, t.ArraySize = %v", t, t.ArraySize)
		h := integerHash(uint64(key)) % t.ArraySize

		for {
			cell = t.CellAt(h)
			if cell.UnHashedKey == key { // 找到了
				return cell
			}
			if cell.UnHashedKey == 0 {
				return nil
			}
			// hash冲突了
			h++
			if h == t.ArraySize {
				h = 0
			}
		}
	}
}

// Insert a key and get back the Cell for that key, so
// as to enable assignment of Value within that Cell, for
// the specified key. The 2nd return value is false if
// key already existed (and thus required no addition); if
// the key already existed you can inspect the existing
// value in the *Cell returned.
// 插入一个键，并返回该键的Cell，以便在该Cell中为指定的键赋值。
// 如果key已经存在，则第二个返回值为false(因此不需要添加);
// 如果键已经存在，可以在返回的*Cell中检查现有值。
func (t *HashTable) Insert(key uint64) (*Cell, bool) {

	vprintf("\n ---- Insert(%v) called with t = \n", key)
	vdump(t)

	defer func() {
		vprintf("\n ---- Insert(%v) done, with t = \n", key)
		vdump(t)
	}()

	var cell *Cell

	if key != 0 {

		for {
			// 计算bucket
			h := integerHash(uint64(key)) % t.ArraySize

			for {
				// 获取cell
				cell = t.CellAt(h)
				if cell.UnHashedKey == key {
					// already exists 已存在，直接返回
					return cell, false
				}

				// 说明不存在数据，cell中数据全是0值
				if cell.UnHashedKey == 0 {
					// 负载因子75%，扩容
					if (t.Population+1)*4 >= t.ArraySize*3 {
						vprintf("detected (t.Population+1)*4 >= t.ArraySize*3, i.e. %v >= %v, calling Repop with double the size\n", (t.Population+1)*4, t.ArraySize*3)
						t.Repopulate(t.ArraySize * 2)
						// resized, so start all over
						break
					}

					// 不需要扩容
					t.Population++         // 增加计数
					cell.UnHashedKey = key // 设置hashKey
					return cell, true
				}

				// hash冲突了 开放地址法-线性探测法
				h++
				if h == t.ArraySize {
					h = 0
				}

			}
		}
	} else {
		// key==0的单独处理，不经过hash运算
		wasNew := false
		if !t.ZeroUsed {
			wasNew = true
			t.ZeroUsed = true
			t.Population++ // 增加计数
			// 扩容
			if t.Population*4 >= t.ArraySize*3 {
				t.Repopulate(t.ArraySize * 2)
			}
		}
		return &t.ZeroCell, wasNew
	}

}

// InsertIntValue inserts value under key in the table.
// 在表的键下插入值。
func (t *HashTable) InsertIntValue(key uint64, value int) bool {
	cell, ok := t.Insert(key)
	cell.SetValue(value)
	return ok
}

// DeleteCell deletes the cell pointed to by cell.
// 删除指向的单元格。
func (t *HashTable) DeleteCell(cell *Cell) {
	// 0号位
	if cell == &t.ZeroCell {
		// Delete zero cell
		if !t.ZeroUsed {
			panic("deleting zero element when not used")
		}
		t.ZeroUsed = false // 标记0号位未使用
		cell.ZeroValue()   // 清空数据
		t.Population--     // 计数
		return

	} else {

		// 计算位置
		pos := uint64((uintptr(unsafe.Pointer(cell)) - uintptr(unsafe.Pointer(t.Cells))) / uintptr(unsafe.Sizeof(Cell{})))

		// Delete from regular Cells 从常规单元格中删除
		// 要删除的位置超过范围了
		if pos < 0 || pos >= t.ArraySize {
			panic(fmt.Sprintf("cell out of bounds: pos %v was < 0 or >= t.ArraySize == %v", pos, t.ArraySize))
		}

		// 删除的是未使用的
		if t.CellAt(pos).UnHashedKey == 0 {
			panic("zero UnHashedKey in non-zero Cell!")
		}

		// Remove this cell by shuffling neighboring Cells so there are no gaps in anyone's probe chain
		// 通过移动相邻的cell来移除这个cell，这样任何人的探测链中都不会有空隙
		// 要看下一个是否是当前cell或者之前的cell存在hash冲突
		nei := pos + 1
		if nei >= t.ArraySize {
			nei = 0
		}
		var neighbor *Cell
		var circular_offset_ideal_pos int64
		var circular_offset_ideal_nei int64
		var cellPos *Cell

		for {
			neighbor = t.CellAt(nei)

			if neighbor.UnHashedKey == 0 {
				// There's nobody to swap with. Go ahead and clear this cell, then return
				// 下一个位置未使用，就直接清空pos的cell，后面没有hash冲突
				cellPos = t.CellAt(pos)
				cellPos.UnHashedKey = 0
				cellPos.ZeroValue() // 清空
				t.Population--      // 计数
				return
			}

			// 理想的位置
			ideal := integerHash(neighbor.UnHashedKey) % t.ArraySize
			// 假设跟pos,hash冲突，如果冲突计算需要移动的距离
			// -----p-----
			// ------2i---
			if pos >= ideal {
				circular_offset_ideal_pos = int64(pos) - int64(ideal)
			} else {
				// pos < ideal, so pos - ideal is negative, wrap-around has happened.
				circular_offset_ideal_pos = int64(t.ArraySize) - int64(ideal) + int64(pos)
			}

			// 假设自身hash冲突，如果冲突计算需要移动的距离
			if nei >= ideal {
				circular_offset_ideal_nei = int64(nei) - int64(ideal)
			} else {
				// nei < ideal, so nei - ideal is negative, wrap-around has happened.
				circular_offset_ideal_nei = int64(t.ArraySize) - int64(ideal) + int64(nei)
			}

			// 自己冲突也包含跟上一个cell冲突，所以以circular_offset_ideal_nei为主
			if circular_offset_ideal_pos < circular_offset_ideal_nei {
				// Swap with neighbor, then make neighbor the new cell to remove.
				// 与邻居交换，然后使邻居成为要移除的新单元。自己本身冲突了，填充要删除的cell，保持连续。
				// neighbor是pos后面的，并不一定是相邻的
				*t.CellAt(pos) = *neighbor
				pos = nei
			}
			// 这里说明相邻的未发生冲突，如果相邻的冲突circular_offset_ideal_nei(自身冲突距离)>circular_offset_ideal_pos(上一个冲突距离)
			// 检查是否跟后面的hash冲突
			nei++
			if nei >= t.ArraySize {
				nei = 0
			}
		}
	}

}

// Clear does not resize the table, but zeroes-out all entries.
// Clear不会调整表的大小，但清除所有条目。
func (t *HashTable) Clear() {
	// (Does not resize the array)
	// Clear regular Cells

	// 清空存储中的数据 每个字节都置为0
	for i := range t.OffheapCells {
		t.OffheapCells[i] = 0
	}
	// 设置计数
	t.Population = 0

	// Clear zero cell
	t.ZeroUsed = false
	t.ZeroCell.ZeroValue()
}

// Compact will compress the hashtable so that it is at most
// 75% full.
// 扩容
func (t *HashTable) Compact() {
	t.Repopulate(upper_power_of_two((t.Population*4 + 3) / 3))
}

// DeleteKey will delete the contents of the cell associated with key.
// 将删除与key关联的单元格的内容。
func (t *HashTable) DeleteKey(key uint64) {
	value := t.Lookup(key)
	if value != nil {
		t.DeleteCell(value)
	}
}

// Repopulate expands the hashtable to the desiredSize count of cells.
// 将哈希表扩展为所需的单元格大小计数。 元素数量
func (t *HashTable) Repopulate(desiredSize uint64) {

	//p("top of Repopulate(%v)", desiredSize)
	vprintf("\n ---- Repopulate called with t = \n")
	vdump(t)

	// 大小必须是2的倍数
	if desiredSize&(desiredSize-1) != 0 {
		panic("desired size must be a power of 2")
	}
	if t.Population*4 > desiredSize*3 {
		panic("must have t.Population * 4  <= desiredSize * 3")
	}

	// Allocate new table
	// TODO: implement growmap for mmap backed resizing.
	// 新创建个table，但是不再支持文件映射
	s := NewHashTable(int64(desiredSize))

	// 设置zero信息
	s.ZeroUsed = t.ZeroUsed
	if t.ZeroUsed {
		s.ZeroCell = t.ZeroCell
		s.Population++
	}

	// Iterate through old table t, copy into new table s.
	// 遍历旧表t，复制到新表s。
	var c *Cell
	for i := uint64(0); i < t.ArraySize; i++ {
		c = t.CellAt(i)
		vprintf("\n in oldCell copy loop, at i = %v, and c = '%#v'\n", i, c)
		if c.UnHashedKey != 0 {
			// Insert this element into new table
			// 只复制已经存储过的cell
			cell, ok := s.Insert(c.UnHashedKey)
			if !ok {
				panic(fmt.Sprintf("key '%v' already exists in fresh table s: should be impossible", c.UnHashedKey))
			}
			*cell = *c // 设置新table中的cell
		}
	}

	vprintf("\n ---- Done with Repopulate, now s = \n")
	vdump(s)

	t.DestroyHashTable()

	*t = *s
	//p("bottom of Repopulate(), now t.ArraySize = %v, t = %p", t.ArraySize, t)
}

/*
Iterator

sample use: given a HashTable h, enumerate h's contents with:

    for it := offheap.NewIterator(h); it.Cur != nil; it.Next() {
      found = append(found, it.Cur.UnHashedKey)
    }
*/
type Iterator struct {
	Tab *HashTable
	Pos int64
	Cur *Cell // will be set to nil when done with iteration.
}

// NewIterator creates a new iterator for HashTable tab.
func (tab *HashTable) NewIterator() *Iterator {
	it := &Iterator{
		Tab: tab,
		Cur: &tab.ZeroCell,
		Pos: -1, // means we are at the ZeroCell to start with
	}

	if it.Tab.Population == 0 {
		it.Cur = nil
		it.Pos = -2
		return it
	}

	if !it.Tab.ZeroUsed {
		it.Next()
	}

	return it
}

// Done checks to see if we have already iterated through all cells
// in the table. Equivalent to checking it.Cur == nil.
func (it *Iterator) Done() bool {
	if it.Cur == nil {
		return true
	}
	return false
}

// Next advances the iterator so that it.Cur points to the next
// filled cell in the table, and returns that cell. Returns nil
// once there are no more cells to be visited.
func (it *Iterator) Next() *Cell {

	// Already finished?
	if it.Cur == nil {
		return nil
	}

	// Iterate through the regular Cells
	it.Pos++
	for uint64(it.Pos) != it.Tab.ArraySize {
		it.Cur = it.Tab.CellAt(uint64(it.Pos))
		if it.Cur.UnHashedKey != 0 {
			return it.Cur
		}
		it.Pos++
	}

	// Finished
	it.Cur = nil
	it.Pos = -2
	return nil
}

// Dump provides a diagnostic dump of the full HashTable contents.
func (t *HashTable) Dump() {
	for i := uint64(0); i < t.ArraySize; i++ {
		cell := t.CellAt(i)
		fmt.Printf("dump cell %d: \n cell.UnHashedKey: '%v'\n cell.ByteKey: '%s'\n cell.Value: '%#v'\n ===============", i, cell.UnHashedKey, string(cell.ByteKey[:]), cell.Value)
	}
}

// InsertBK is the insert function for []byte keys.
// By default only len(Key_t) bytes are used in the key.
// 是字节键的插入函数。
// 默认情况下，key只使用len(Key_t)字节。
func (t *HashTable) InsertBK(bytekey []byte, value interface{}) bool {
	return ((*ByteKeyHashTable)(t)).InsertBK(bytekey, value)
}

func (t *HashTable) LookupBK(bytekey []byte) (Val_t, bool) {
	return ((*ByteKeyHashTable)(t)).LookupBK(bytekey)
}

func (t *HashTable) LookupBKInt(bytekey []byte) (int, bool) {
	v, found := ((*ByteKeyHashTable)(t)).LookupBK(bytekey)
	if !found {
		return -1, found
	}
	return v.GetInt(), true
}

// 计算arraySize个元素数量占用的内存大小
func (t *HashTable) bytesFromArraySizeAndHeader(arraySize uint64) int64 {
	return int64(arraySize*t.CellSz) + MetadataHeaderMaxBytes
}
