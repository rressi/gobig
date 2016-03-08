package big

import (
	"sort"
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
func (p StringSlice) Less(i, j int) bool { return p[i] < p[j] }
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
	objects RadixSortable
	items   []index
	sorted  bool
}

func (p bucket) Len() int {
	return len(p.items)
}
func (p bucket) Less(i, j int) bool {

	i1 := p.items[i]
	j1 := p.items[j]
	// sortTrace("Comparing: %v vs %v", i1, j1)

	switch {
	case i1.key < j1.key:
		return true
	case i1.key > j1.key:
		return false
	default:
		// Slow but extremely uncommon case:
		return p.objects.Less(i1.idx, j1.idx)
	}
}
func (p bucket) Swap(i, j int) {
	// Swaps the indices, not the original objects
	p.items[i], p.items[j] = p.items[j], p.items[i]
}

// ---------------------------------------------------------------------------------------------------------------------

func radixSort(out chan int, objects RadixSortable, isBigProblem bool) {

	const BUCKETS_PER_RUNNER = 256

	numObjects := objects.Len()
	sortTrace("Num objects: %v", numObjects)

	var numBuckets int
	if isBigProblem {
		numBuckets = 1 << 16
	} else {
		numBuckets = 1 << 8
	}
	sortTrace("Num buckets: %v", numBuckets)

	// Fills buckets:
	buckets := make([]bucket, numBuckets)
	func() {
		for i := 0; i < numObjects; i++ {
			key := objects.RadixKey(i)
			bucketPos := key & uint64(numBuckets-1)
			bucketPt := &buckets[bucketPos]
			bucketPt.items = append(bucketPt.items, index{key: key, idx: i})
		}
	}() // func1

	// Strips empty buckets:
	func() {
		j := 0
		for _, buc := range buckets {
			bucketSize := buc.Len()
			if bucketSize > 0 {
				buc.objects = objects
				buckets[j] = buc
				j++
			}
		}
		numBuckets = j
		buckets = buckets[:j]
	}() // func2
	sortTrace("Buckets to sort: %v", numBuckets)

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
				out <- buc.items[0].idx
			default:
				sort.Sort(buc)
				for _, index := range buc.items {
					out <- index.idx
				}
			}
		}

	// Parallel algorithm
	default:
		sortTrace("Parallel algorithm chosen")

		// Spares go-routines:
		bucketSorted := make(chan int, numBuckets)
		for i := 0; i < numBuckets; i += BUCKETS_PER_RUNNER {
			go func(from int) {
				to := from + BUCKETS_PER_RUNNER
				if to > numBuckets {
					to = numBuckets
				}
				for j, buc := range buckets[from:to] {
					sort.Sort(buc)
					bucketSorted <- from + j
				}
			}(i) // func3
		}

		// Collects sorted buckets:
		var nextToReturn int
		func() {
			bucketPos := 0
			bucketPt := &buckets[bucketPos]
			for i := 0; i < numBuckets; i++ {

				// If possible returns all sorted indices from current bucket:
				if bucketPos == nextToReturn {
					for bucketPt.sorted {
						for _, index := range bucketPt.items {
							out <- index.idx
						}
						// sortTrace("Bucket done: %v", bucketPos)
						nextToReturn++
						bucketPos++
						bucketPt = &buckets[bucketPos]
					}
				}

				// Fetches next sorted bucket:
				bucketPos := <-bucketSorted
				bucketPt := &buckets[bucketPos]
				bucketPt.sorted = true
				// sortTrace("Received bucket: %v", bucketPos)
			}
		}() // func4
		sortTrace("Returning %v buckets later", numBuckets - nextToReturn)

		// Returns missing buckets:
		func() {
			for i := nextToReturn; i < numBuckets; i++ {
				for _, index := range buckets[i].items {
					out <- index.idx
				}
				// sortTrace("Bucket completed later: %v", i)
			}
		}() // func5
	}
}

// ---------------------------------------------------------------------------------------------------------------------
