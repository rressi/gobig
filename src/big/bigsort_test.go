package big

import (
	"bufio"
	"os"
	"testing"
	"sort"
)

var cachedFiles map[string][]string = make(map[string][]string)

const SMALL_FILE = "../../small.txt"
const BIG_FILE = "../../big.txt"

func TestRadixSortStrings(t *testing.T) {

	testItems := func(items []string) {
		out := RadixSortStrings(items)

		positions := make(map[int]bool)
		prevItem := ""

		for index := range out {

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

	testItems(loadLines(SMALL_FILE, t))
	testItems(loadLines(BIG_FILE, t))
}

// ---------------------------------------------------------------------------------------------------------------------

func BenchmarkRadixSort_1k(b *testing.B) {
	lines := loadLines(BIG_FILE, b)[:1000]

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _ = range RadixSortStrings(lines) {
			// Nothing to do.
		}
	}
}

func BenchmarkRadixSort_10k(b *testing.B) {
	lines := loadLines(BIG_FILE, b)[:10000]

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _ = range RadixSortStrings(lines) {
			// Nothing to do.
		}
	}
}

func BenchmarkRadixSort_100k(b *testing.B) {
	lines := loadLines(BIG_FILE, b)[:100000]

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _ = range RadixSortStrings(lines) {
			// Nothing to do.
		}
	}
}

func BenchmarkRadixSort_1M(b *testing.B) {
	lines := loadLines(BIG_FILE, b)[:1000000]

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _ = range RadixSortStrings(lines) {
			// Nothing to do.
		}
	}
}

func BenchmarkRadixSort_10M(b *testing.B) {
	lines := loadLines(BIG_FILE, b)[:10000000]

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for _ = range RadixSortStrings(lines) {
			// Nothing to do.
		}
	}
}

// ---------------------------------------------------------------------------------------------------------------------

func BenchmarkSort_1k(b *testing.B) {

	inputs := prepareInputs(b.N, 1000, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sort.Strings(inputs[n])
	}
}

func BenchmarkSort_10k(b *testing.B) {

	inputs := prepareInputs(b.N, 10000, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sort.Strings(inputs[n])
	}
}

func BenchmarkSort_100k(b *testing.B) {

	inputs := prepareInputs(b.N, 100000, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sort.Strings(inputs[n])
	}
}

func BenchmarkSort_1M(b *testing.B) {

	inputs := prepareInputs(b.N, 1000000, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sort.Strings(inputs[n])
	}
}

func BenchmarkSort_10M(b *testing.B) {

	inputs := prepareInputs(b.N, 10000000, b)

	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		sort.Strings(inputs[n])
	}
}

func prepareInputs(numTests, numElements int, b *testing.B) [][]string {

	lines := loadLines(BIG_FILE, b)

	inputs := make([][]string, numTests)
	for n := 0; n < b.N; n++ {
		linesN := make([]string, 0, numElements)
		for _, line := range (lines[:numElements]) {
			linesN = append(linesN, line)
		}
		inputs[n] = linesN
	}

	return inputs
}

// ---------------------------------------------------------------------------------------------------------------------

type FatalCaller interface {
	Fatalf(format string, args ...interface{})
}

// ---------------------------------------------------------------------------------------------------------------------

func loadLines(path string, fatal FatalCaller) []string {

	items, ok := cachedFiles[path]
	if ok {
		return items
	}

	fd, err := os.Open(path)
	if err != nil {
		workDirectory, _ := os.Getwd()
		fatal.Fatalf("Cannot open file (cwd is '%v'): %v", workDirectory, err)
	}
	defer fd.Close()

	scan := bufio.NewScanner(fd)
	for scan.Scan() {
		items = append(items, scan.Text())
	}
	err = scan.Err()
	if err != nil {
		fatal.Fatalf("Cannot scan file: %v", err)
	}

	cachedFiles[path] = items

	return items
}
