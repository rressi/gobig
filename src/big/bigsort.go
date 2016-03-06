package big

import (
	"sort"
)


// StringSlice attaches the methods of RadixSortable to []string, sorting in increasing order.
type StringSlice []string

func (p StringSlice) Len() int {
	return len(p)
}
func (p StringSlice) Less(i, j int) bool {
	return p[i] < p[j]
}
func (p StringSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p StringSlice) Get(i int) interface{} {
	return p[i]
}
func (p StringSlice) New() RadixSortable {
	return StringSlice(make([]string, 0))
}
func (p StringSlice) Append(obj interface{}) RadixSortable {
	return append(p, obj.(string))
}
func (p StringSlice) RadixKey(i int) (key int) {

	item := p[i]
	switch(len(item)) {
	case 0:
		key = 0
	case 1:
		key =  (1 << 16) * int(item[0])
	case 2:
		key =  (1 << 16) * int(item[0]) + (1 << 8) * int(item[1])
	default:
		key =  (1 << 16) * int(item[0]) + (1 << 8) * int(item[1]) + int(item[2])
	}

	return int(key)
}


type RadixSortable interface {

	sort.Interface

	// Returns the element at the given position
	Get(i int ) interface{}

	// Creates an empty container
	New() RadixSortable

	// Appends an element
	Append(obj interface{}) RadixSortable

	// Returns the radix-key of the passed element.
	RadixKey(i int) int
}


func RadixSort(objects RadixSortable) <-chan RadixSortable {

	out := make(chan RadixSortable)

	// Concurrently sorts each bucket:
	go func() {

		// For small arrays simply uses sort.Sort from another go-routine:
		const MIN_SIZE = 10000
		numObjects := objects.Len()
		if numObjects < MIN_SIZE {

			sortedObjects := objects.New()
			for i := 0; i < numObjects; i++ {
				sortedObjects = sortedObjects.Append(objects.Get(i))
			}

			sort.Sort(sortedObjects)
			out <- sortedObjects

		} else {

			type bucket struct {
				items RadixSortable
				pos int
				sorted bool
			}

			// Collects our buckets:
			buckets := make(map [int]*bucket)
			for i := 0; i < numObjects; i++ {
				obj := objects.Get(i)
				key := objects.RadixKey(i)
				buc, ok := buckets[key]
				if ok {
					buc.items = buc.items.Append(obj)
				} else {
					buc = &bucket{}
					buc.items = objects.New()
					buc.items = buc.items.Append(obj)
					buckets[key] = buc
				}
			}
			numBuckets := len(buckets)

			// Sorts buckets concurrently:
			bucketSorted := make(chan *bucket)
			for _, buc := range(buckets) {
				go func(buc *bucket) {
					sort.Sort(sort.Interface(buc.items))
					bucketSorted <- buc
				}(buc)
			}

			// Assigns to each bucket its positional order:
			keys := make([]int, 0, numBuckets)
			for key := range(buckets) {
				keys = append(keys, key)
			}
			sort.Ints(keys)
			for i, key := range(keys) {
				buckets[key].pos = i
			}

			// Collects sorted buckets:
			var nextToReturn int
			for i := 0; i < numBuckets; i++ {
				buc := <- bucketSorted
				buc.sorted = true

				if buc.pos == nextToReturn {
					out <- buc.items
					nextToReturn++

					// Tries to emit as much bucket as possible to the main routine:
					pos := buc.pos + 1
					for pos < numBuckets {
						buc = buckets[keys[pos]]
						if !buc.sorted {
							break
						}
						out <- buc.items
						nextToReturn++
						pos++
					}
				}
			}
			if nextToReturn < numBuckets {
				panic("We miss some bucket!")
			}
		}

		close(out)
	}()

	return out
}

