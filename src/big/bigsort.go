package big

import (
	"sort"
	"fmt"
)

var sortTraces = false
func sortTrace(format string, a ...interface{}) {
	if sortTraces {
		fmt.Printf(format + "\n", a...)
	}
}

// StringSlice attaches the methods of RadixSortable to []string, sorting in increasing order.
type StringSlice []string

func (p StringSlice) Len() int           { return len(p) }
func (p StringSlice) Less(i, j int) bool { return p[i] < p[j] }
func (p StringSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p StringSlice) RadixKey(i int) (key uint32) {

	item := p[i]
	var multiplier uint32 = 1 << 24
	for j := 0; j < 4 && j < len(item); j++ {
		key += uint32(item[j]) * multiplier
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
	RadixKey(i int) uint32
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
	key uint32
	idx int
}

type bucket struct {
	objects RadixSortable
	items   []index
	sorted  bool
}

func (p *bucket) Len() int {
	return len(p.items)
}
func (p *bucket) Less(i, j int) bool {

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
func (p *bucket) Swap(i, j int) {
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
	for i := 0; i < numBuckets; i++ {
		buckets[i].objects = objects
	}
	for i := 0; i < numObjects; i++ {
		key := objects.RadixKey(i)
		buc := &buckets[key & uint32(numBuckets - 1)]
		buc.items = append(buc.items, index{key:key, idx:i})
	}

	// Sorts buckets concurrently:
	numRunners := numBuckets / BUCKETS_PER_RUNNER
	sortTrace("Num runners: %v", numRunners)
	switch numRunners {

	// One runner?
	case 1:
		// Lets sort it directly!
		for i := range buckets {
			bucketPt := &buckets[i]
			bucketSize := bucketPt.Len()
			switch bucketSize {
			case  0:
			case  1:
				out <- bucketPt.items[0].idx
			default:
				sort.Sort(bucketPt)
				for _, index := range bucketPt.items {
					out <- index.idx
				}
			}
		}

	default:
		// Spares concurrent sorters:
		bucketSorted := make(chan int, numBuckets)
		for i := 0; i < numBuckets; i += BUCKETS_PER_RUNNER {
			go func(from int) {
				to := from + BUCKETS_PER_RUNNER
				for j := from; j < to; j++ {
					bucketPt := &buckets[j]
					sort.Sort(bucketPt)
					bucketSorted <- j
				}
			}(i)
		}

		// Collects sorted buckets:
		var nextToReturn int
		for i := 0; i < numBuckets; i++ {

			// Fetches next sorted bucket:
			bucketPos := <-bucketSorted
			bucketPt := &buckets[bucketPos]
			bucketPt.sorted = true
			// sortTrace("Sorted bucket: %v", bucketPos)

			// If possible returns all sorted indices from current bucket:
			if bucketPos == nextToReturn {
				for bucketPt.sorted {
					bucketPt = &buckets[bucketPos]
					for _, index := range bucketPt.items {
						out <- index.idx
					}
					// sortTrace("Bucket completed: %v", bucketPos)
					nextToReturn++
					bucketPos++
				}
			}
		}

		// Returns missing buckets:
		for i := nextToReturn; i < numBuckets; i++ {
			bucketPt := &buckets[i]
			for _, index := range bucketPt.items {
				out <- index.idx
			}
			// sortTrace("Bucket completed later: %v", i)
		}

	}
}

// ---------------------------------------------------------------------------------------------------------------------
