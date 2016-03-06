package big


import (
	"testing"
	"os"
	"bufio"
)

func TestRadixSortStrings(t *testing.T) {


	testItems := func(items []string) {
		out := RadixSortStrings(items)

		positions := make(map[int]bool)
		prevItem := ""

		for index := range(out) {

			if index >= len(items) {
				t.Errorf("Invalid index %i", index)
			}

			item := items[index]
			if item < prevItem {
				t.Errorf("Misplaced index %i", index)
			}

			_, found := positions[index]
			if found {
				t.Errorf("Index %i repeated", index)
			}

			positions[index] = true
		}
	}

	testItems(loadLines("../../small.txt", t))
	testItems(loadLines("../../big.txt", t))
}


func loadLines(path string, t *testing.T) []string {

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

	return items
}
