package big

import (
	"sort"
	"fmt"
)


// StringSlice attaches the methods of RadixSortable to []string, sorting in increasing order.
type StringSlice []string

func (p StringSlice) Len() int { return len(p) }
func (p StringSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p StringSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
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

func RadixSortStrings(items []string) <-chan int {

	return RadixSort(StringSlice(items))
}

// ---------------------------------------------------------------------------------------------------------------------

type RadixSortable interface {

	sort.Interface

	// Returns the radix-key of the passed element.
	RadixKey(i int) int
}


func RadixSort(objects RadixSortable) <-chan int {

	out := make(chan int, objects.Len())

	// Concurrently sorts each bucket:
	go func() {
		defer close(out)

		if objects == nil {
			return
		}

		const MIN_SIZE = 10000

		numObjects := objects.Len()
		switch {

		// Nothing to sort:
		case numObjects == 0:
			break

		// For a simple problem, a simple solution:
		case numObjects < MIN_SIZE:
			buc := newBucket(objects)
			for index := 0; index < numObjects; index++ {
				buc.items = append(buc.items, index)
			}
			sort.Sort(buc)
			for _, index := range(buc.items) {
				out <- index
			}
			break

		// Lets scale up:
		default:
			radixSort(out, objects)

		}
	}()

	return out
}

// ---------------------------------------------------------------------------------------------------------------------

type bucket struct {
	objects RadixSortable
	items []int
	pos int
	sorted bool
}

func (p bucket) Len() int {
	return len(p.items)
}
func (p bucket) Less(i, j int) bool {
	i1 := p.items[i]
	j1 := p.items[j]
	return p.objects.Less(i1, j1)
}
func (p bucket) Swap(i, j int) {
	p.items[i], p.items[j] = p.items[j], p.items[i]
}

func newBucket(objects RadixSortable) *bucket {

	return &bucket { objects: objects,
		         items:   make([]int, 0),
		         pos:     0,
		         sorted:  false }
}

// ---------------------------------------------------------------------------------------------------------------------

func radixSort(out chan int, objects RadixSortable) {

	numObjects := objects.Len()
	fmt.Println("Num objects:", numObjects)

	// Generates buckets:
	buckets := make(map [int]*bucket)
	for i := 0; i < numObjects; i++ {
		key := objects.RadixKey(i)
		buc, ok := buckets[key]
		if ok {
			buc.items = append(buc.items, i)
		} else {
			buc = newBucket(objects)
			buc.items = append(buc.items, i)
			buckets[key] = buc
		}
	}
	numBuckets := len(buckets)
	fmt.Println("Num buckets:", numBuckets)

	// Sorts buckets concurrently:
	bucketSorted := make(chan *bucket, numBuckets)
	for _, buc := range(buckets) {
		go func(buc *bucket) {
			sort.Sort(*buc)
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
	for numBuckets > 0 {
		buc := <- bucketSorted
		buc.sorted = true
		numBuckets--

		bucketPos := buc.pos
		if bucketPos != nextToReturn {
			continue
		}

		// Returns all indices from current bucket:
		for _, index := range(buc.items) {
			out <- index
		}
		nextToReturn++

		// Tries to emit as much bucket as possible to the main routine:
		for bucketPos += 1; bucketPos < numBuckets; bucketPos++ {
			buc = buckets[keys[bucketPos]]
			if !buc.sorted {
				break
			}
			for _, index := range(buc.items) {
				out <- index
			}
			nextToReturn++
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------

