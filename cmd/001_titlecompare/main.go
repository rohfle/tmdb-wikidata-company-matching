package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/adrg/strutil"
	"github.com/adrg/strutil/metrics"
)

const MAX_RESULTS = 5

var COMPARE_METRIC = metrics.Levenshtein{
	CaseSensitive: true,
	InsertCost:    1,
	ReplaceCost:   2,
	DeleteCost:    1,
}

type JSONLines struct {
	toClose []io.ReadCloser
	scanner *bufio.Scanner
}

func (jl *JSONLines) Load(path string) error {
	if len(jl.toClose) > 0 {
		jl.Close()
	}

	var reader io.ReadCloser
	reader, err := os.Open(os.Args[1])
	if err != nil {
		return err
	}
	jl.toClose = append(jl.toClose, reader)

	if strings.HasSuffix(path, ".gz") {
		reader, err = gzip.NewReader(reader)
		if err != nil {
			return err
		}
		jl.toClose = append(jl.toClose, reader)
	}
	jl.scanner = bufio.NewScanner(reader)

	return nil
}

func (jl *JSONLines) Close() {
	for idx := len(jl.toClose) - 1; idx >= 0; idx-- {
		jl.toClose[idx].Close()
	}
	jl.toClose = nil
	jl.scanner = nil
}

func (jl *JSONLines) Next(item interface{}) error {
	if !jl.scanner.Scan() {
		err := jl.scanner.Err()
		jl.Close()
		if err == nil {
			return io.EOF
		}
		return fmt.Errorf("scanner: %w", err)
	}

	line := jl.scanner.Bytes()

	err := json.Unmarshal(line, item)
	if err != nil {
		jl.Close()
		return fmt.Errorf("json: %w", err)
	}
	return nil
}

type WikidataItem struct {
	ID             string
	Name           string
	NormalizedName string
}

type TMDBItem struct {
	ID             int64
	Name           string
	NormalizedName string
}

type Result struct {
	Score float64
	Item  *WikidataItem
}

type PossibleMatch struct {
	TMDB    *TMDBItem
	Options []*Result
}

func normalizeName(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, ".", "")
	s = strings.ReplaceAll(s, " ltd", " ")
	s = strings.ReplaceAll(s, " limited", " ")
	s = strings.ReplaceAll(s, " gmbh", " ")
	s = strings.ReplaceAll(s, " productions", " PROD")
	s = strings.ReplaceAll(s, " production", " PROD")
	s = strings.ReplaceAll(s, " international", " INT")
	s = strings.ReplaceAll(s, " corporation", " CORP")
	s = strings.ReplaceAll(s, " entertainment", " ENM")
	return s
}

func stringInSlice(key string, values []string) bool {
	for _, v := range values {
		if key == v {
			return true
		}
	}
	return false
}

func loadTmdbItems(path string) ([]*TMDBItem, error) {
	var items []*TMDBItem

	var tmdbData JSONLines
	err := tmdbData.Load(os.Args[1])
	if err != nil {
		return nil, fmt.Errorf("load: %w", err)
	}

	defer tmdbData.Close()

	for {
		var item TMDBItem
		if err := tmdbData.Next(&item); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("next: %w", err)
		}

		item.NormalizedName = normalizeName(item.Name)
		items = append(items, &item)
	}

	return items, nil
}

func loadWikidataItems(path string) ([]*WikidataItem, error) {
	var seenIDs []string

	var items []*WikidataItem

	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// remember to close the file at the end of the program
	defer f.Close()

	// read csv values using csv.Reader
	csvReader := csv.NewReader(f)

	for {
		line, err := csvReader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		item := WikidataItem{
			ID:   filepath.Base(line[0]),
			Name: line[1],
		}

		if stringInSlice(item.ID, seenIDs) {
			continue
		}

		item.NormalizedName = normalizeName(item.Name)
		seenIDs = append(seenIDs, item.ID)
		items = append(items, &item)
	}

	return items, nil
}

func compare(a string, b string) float64 {
	lena := len(a)
	lenb := len(b)
	if lena > (lenb+10) || lena < (lenb-10) {
		return 0 // Hard fail
	}
	score := strutil.Similarity(a, b, &COMPARE_METRIC)
	return score
}

func joinTheDots(tmdbItems []*TMDBItem, wikidataItems []*WikidataItem) []*PossibleMatch {
	// var tmdbNoMatch []*TMDBItem
	var matches []*PossibleMatch = make([]*PossibleMatch, 0, len(tmdbItems)/2)

	for idx, titem := range tmdbItems {
		if idx%1000 == 0 {
			fmt.Printf("%d: %d %s\n", idx, titem.ID, titem.Name)
		}
		var topResults []*Result = make([]*Result, 0, MAX_RESULTS)
		for _, witem := range wikidataItems {
			score := compare(titem.NormalizedName, witem.NormalizedName)
			if score < 0.5 { // Not a chance
				continue
			}
			result := &Result{
				Item:  witem,
				Score: score,
			}
			topResults = addToTopN(topResults, result, MAX_RESULTS)
		}

		resultsLength := len(topResults)
		if resultsLength == 0 || topResults[0].Score < 0.65 {
			continue
		}

		if topResults[0].Score == 1.0 && resultsLength == 1 {
			// definite match, remove from wikidata
			wikidataItems = remove(wikidataItems, topResults[0].Item)
		}

		// fmt.Printf("TMDB[%d]: %s\n", titem.ID, titem.Name)
		// for _, result := range topResults {
		// 	fmt.Printf("    %0.6f - %s: %s\n", result.Score, result.Item.ID, result.Item.Name)
		// }
		// fmt.Println()

		matches = append(matches, &PossibleMatch{
			TMDB:    titem,
			Options: topResults,
		})
	}

	return matches
}

func remove[T comparable](slice []T, s T) []T {
	for idx, item := range slice {
		if s == item {
			return append(slice[:idx], slice[idx+1:]...)
		}
	}
	return slice
}

// Add to slice, keep the top scores only
// As a side effect the result will be sorted if this is used from a blank haystack
func addToTopN(haystack []*Result, needle *Result, limit int) []*Result {
	for idx, result := range haystack {
		if result.Score < needle.Score {
			haystack[idx] = needle
			needle = result
		}
	}
	if len(haystack) < limit {
		haystack = append(haystack, needle)
	}
	return haystack
}

func saveMatches(path string, matches []*PossibleMatch) error {
	header := []string{"tmdbID", "tmdbName"}
	for i := 1; i <= MAX_RESULTS; i++ {
		prefix := fmt.Sprintf("result%d", i)
		header = append(header, prefix+"Score", prefix+"ID", prefix+"Name")
	}

	f, err := os.Create(path)
	if err != nil {
		return err
	}

	defer f.Close()

	w := csv.NewWriter(f)
	w.Write(header)

	for _, match := range matches {
		var row = []string{
			strconv.FormatInt(match.TMDB.ID, 10),
			match.TMDB.Name,
		}

		for _, result := range match.Options {
			row = append(row, fmt.Sprintf("%0.6f", result.Score), result.Item.ID, result.Item.Name)
		}

		for left := MAX_RESULTS - len(match.Options) - 1; left >= 0; left-- {
			row = append(row, "", "", "")
		}
		w.Write(row)
	}

	w.Flush()

	return w.Error()
}

// FUTURE - compare ids of movies / tv to correlate
func main() {
	if len(os.Args) < 4 {
		fmt.Println("Usage: go run main.go <tmdb data> <wikidata data> <output>")
		os.Exit(1)
	}

	tmdbItems, err := loadTmdbItems(os.Args[1])
	if err != nil {
		fmt.Println("Error while loading tmdb data:", err)
		return
	}

	wikidataItems, err := loadWikidataItems(os.Args[2])
	if err != nil {
		fmt.Println("Error while loading wikidata:", err)
		return
	}

	matches := joinTheDots(tmdbItems, wikidataItems)

	sort.Slice(matches, func(i int, j int) bool {
		return matches[i].Options[0].Score > matches[j].Options[0].Score
	})

	err = saveMatches(os.Args[3], matches)
	if err != nil {
		fmt.Println("Error while saving matches:", err)
		return
	}
}
