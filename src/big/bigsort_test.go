package big


import (
	"sort"
	"testing"
	"os"
	"bufio"
)

func TestRadixSortStrings(t *testing.T) {


	testItems := func(items StringSlice) {
		sortedItems := RadixSort(items)
		last := ""

		i := 0
		for bucket := range(sortedItems) {

			num := bucket.Len()
			if num < 1 {
				t.Errorf("Bucket %v is empty (%v)", i, bucket)
				i++
				continue
			}
			// fmt.Println(i)
			// fmt.Println("   Len:  ", num)

			first := bucket.Get(0).(string)
			if first < last {
				t.Errorf("Bucket %v is misplaced (%v)", i, bucket)
			}
			if (!sort.IsSorted(bucket)) {
				t.Errorf("Bucket %v is not sorted (%v)", i, bucket)
			}
			last = bucket.Get(bucket.Len() - 1).(string)
			i++

			// fmt.Println("   First:", first)
			// fmt.Println("   Last: ", last)
		}
	}

	testItems(loadLines("../../small.txt", t))
	testItems(loadLines("../../big.txt", t))
}


func loadLines(path string, t *testing.T) StringSlice {

	items := make([]string, 0)
	fd, err := os.Open(path)
	if err != nil {
		workDirectory, _ := os.Getwd()
		t.Fatalf("Cannot open file (cwd is '%v'): %v", workDirectory, err)
	}
	defer fd.Close()

	scan := bufio.NewScanner(fd)
	for scan.Scan() {
		items = append(items, scan.Text())
	}
	err = scan.Err()
	if err != nil {
		t.Fatalf("Cannot scan file: %v", err)
	}

	return StringSlice(items)
}