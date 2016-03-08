package big

import (
	"sort"
	"sync"
)

var sortTraceFunc func(format string, a ...interface{})

func sortTrace(format string, a ...interface{}) {
	if sortTraceFunc != nil {
		sortTraceFunc(format+"\n", a...)
	}
}

// StringSlice attaches the methods of RadixSortable to []string, sorting in increasing order.
type StringSlice []string

func (p StringSlice) Len() int           { return len(p) }
func (p StringSlice) Less(i, j int) bool {
	if len(p[i]) <= 8 && len(p[j]) <= 8 {
		return false
	}
	// sortTrace("[%s] vs [%s]", p[i], p[j])
	return p[i] < p[j]
}
func (p StringSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p StringSlice) RadixKey(i int) (key uint64) {

	item := p[i]
	var multiplier uint64 = 1 << 56
	for j := 0; j < 8 && j < len(item); j++ {
		key += uint64(item[j]) * multiplier
		multiplier >>= 8
	}

	return key
}

func RadixSortStrings(items []string) <-chan int {

	return RadixSort(StringSlice(items))
}

// ---------------------------------------------------------------------------------------------------------------------

type RadixSortable interface {
	sort.Interface

	// Returns the radix-key of the passed element.
	RadixKey(i int) uint64
}

func RadixSort(objects RadixSortable) <-chan int {

	out := make(chan int, objects.Len())

	const isBigProblem = 10000

	// Concurrently sorts each bucket:
	go func() {
		defer close(out)

		if objects == nil {
			return
		}

		numObjects := objects.Len()
		if numObjects == 0 {
			return
		}

		radixSort(out, objects, numObjects >= isBigProblem)
	}()

	return out
}

// ---------------------------------------------------------------------------------------------------------------------

type index struct {
	key uint64
	idx int
}

type bucket struct {
	hash    int
	objects RadixSortable
	indices []index
}

func (p bucket) Len() int {
	return len(p.indices)
}
func (p bucket) Less(i, j int) bool {

	i1 := p.indices[i]
	j1 := p.indices[j]

	switch {
	case i1.key < j1.key:
		return true
	case i1.key > j1.key:
		return false
	default:
		// Slow but extremely uncommon case:
		// sortTrace("Comparing: %v vs %v", i1.idx, j1.idx)
		return p.objects.Less(i1.idx, j1.idx)
	}
}
func (p bucket) Swap(i, j int) {
	// Swaps the indices, not the original objects
	p.indices[i], p.indices[j] = p.indices[j], p.indices[i]
}

type ByHash []bucket

func (p ByHash) Len() int           { return len(p) }
func (p ByHash) Less(i, j int) bool { return p[i].hash < p[j].hash }
func (p ByHash) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// ---------------------------------------------------------------------------------------------------------------------

func radixSort(out chan int, objects RadixSortable, isBigProblem bool) {

	const BUCKETS_PER_RUNNER = 256

	numObjects := objects.Len()
	sortTrace("Num objects: %v", numObjects)

	var maxBuckets int
	if isBigProblem {
		maxBuckets = 1 << 24
	} else {
		maxBuckets = 1 << 16
	}
	sortTrace("Max buckets: %v", maxBuckets)

	// Generates keys:
	bucketMap := make(map[int][]index)
	func() {
		for i := 0; i < numObjects; i++ {
			key := objects.RadixKey(i)
			hash := int(key & uint64(maxBuckets - 1))
			bucketMap[hash] = append(bucketMap[hash], index{key: key, idx: i})
		}
	}() // func1

	// Fills buckets:
	var buckets []bucket
	var numBuckets int
	func() {
		for hash, indices := range bucketMap {
			buckets = append(buckets, bucket{
				hash:    hash,
				objects: objects,
				indices: indices})
		}
		sort.Sort(ByHash(buckets))
		numBuckets = len(buckets)
	}() // func2
	sortTrace("Num buckets: %v", numBuckets)

	// Sorts buckets concurrently:
	numRunners := numBuckets / BUCKETS_PER_RUNNER
	sortTrace("Num runners: %v", numRunners)
	switch {

	// Is it better to go serial?
	case numRunners <= 1:
		// Lets sort it directly!
		sortTrace("Serial algorithm chosen")
		for _, buc := range buckets {
			switch buc.Len() {
			case 1:
				out <- buc.indices[0].idx
			default:
				sort.Sort(buc)
				for _, index := range buc.indices {
					out <- index.idx
				}
			}
		}

	// Parallel algorithm
	default:
		sortTrace("Parallel algorithm chosen")

		// Parallel sort:
		var wg sync.WaitGroup
		wg.Add(numBuckets)
		for i := 0; i < numBuckets; i += BUCKETS_PER_RUNNER {
			go func(from int) {
				to := from + BUCKETS_PER_RUNNER
				if to > numBuckets {
					to = numBuckets
				}
				for _, buc := range buckets[from:to] {
					sort.Sort(buc)
					wg.Done()
				}

			}(i) // func3
		}
		wg.Wait()

		// Returns sorted indices:
		func() {
			for _, buc := range buckets {
				for _, index := range buc.indices {
					out <- index.idx
				}
			}
		}() // func4
	}
}

// ---------------------------------------------------------------------------------------------------------------------
