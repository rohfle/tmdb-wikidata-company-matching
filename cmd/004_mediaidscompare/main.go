package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
)

type Item struct {
	TmdbID       string
	CompanyName  string
	Possibilites []*Possibility
}

type Possibility struct {
	WikidataID  string
	CompanyName string
	Score       float64
}

type Match struct {
	TmdbID              string
	TmdbCompanyName     string
	WikidataID          string
	WikidataCompanyName string
	NameScore           float64
	MappingScore        float64
	MapMatchCount       int
	TmdbMapCount        int
	WikidataMapCount    int
	TotalScore          float64
}

func (m Match) String() string {
	return fmt.Sprintf("%s [%s] <=> [%s] %s (match: %d, counts: (%d, %d), score: %0.4f * %0.4f = %0.4f)",
		m.TmdbCompanyName,
		m.TmdbID,
		m.WikidataID,
		m.WikidataCompanyName,
		m.MapMatchCount,
		m.TmdbMapCount,
		m.WikidataMapCount,
		m.NameScore,
		m.MappingScore,
		m.TotalScore)
}

type Media struct {
	Movies []int64
	TV     []int64
	Count  int
}

func dumbInSlice(haystack []string, needle string) bool {
	for _, n := range haystack {
		if n == needle {
			return true
		}
	}
	return false
}

func loadCompareCSV(path string) ([]*Item, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	_, err = csvReader.Read() // ignore header
	if err != nil {
		log.Fatal(err)
	}

	var results []*Item

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		item := &Item{
			TmdbID:      record[0],
			CompanyName: record[1],
		}

		for idx := 2; idx < len(record)-2; idx += 3 {
			scoreStr := record[idx]
			if scoreStr == "" {
				break
			}
			wdID := record[idx+1]
			wdName := record[idx+2]
			score, err := strconv.ParseFloat(scoreStr, 64)
			if err != nil {
				log.Printf("invalid score found in compare csv: %s", scoreStr)
				break
			}
			item.Possibilites = append(item.Possibilites, &Possibility{
				WikidataID:  wdID,
				CompanyName: wdName,
				Score:       score,
			})
		}

		results = append(results, item)
	}

	return results, err
}

func loadMediaMappingCSV(path string) (map[string]*Media, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	_, err = csvReader.Read() // ignore header
	if err != nil {
		log.Fatal(err)
	}

	var results = make(map[string]*Media)

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		if record[2] == "" {
			continue
		}

		tmdbID, err := strconv.ParseInt(record[2], 10, 64)
		if err != nil {
			log.Printf("invalid score found in mapping csv: %s", record[2])
			continue
		}
		cID := record[0]
		tmdbType := record[3]

		media, exists := results[cID]
		if !exists {
			media = &Media{}
			results[cID] = media
		}

		if tmdbType == "movie" {
			media.Movies = append(media.Movies, tmdbID)
		} else if tmdbType == "tv" {
			media.TV = append(media.TV, tmdbID)
		} else {
			log.Printf("invalid media type found in mapping csv: %s", tmdbType)
			continue
		}
		media.Count += 1
	}

	return results, err
}

func main() {
	titleCompareCSVPath := os.Args[1]
	tmdbMediaCSVPath := os.Args[2]
	wikidataMediaCSVPath := os.Args[3]
	outputMatchCSVPath := os.Args[4]

	compareSet, err := loadCompareCSV(titleCompareCSVPath)
	if err != nil {
		log.Fatalf("error while loading compare csv: %s", err)
	}

	tmdbMediaSet, err := loadMediaMappingCSV(tmdbMediaCSVPath)
	if err != nil {
		log.Fatalf("error while loading tmdb media set csv: %s", err)
	}

	wikidataMediaSet, err := loadMediaMappingCSV(wikidataMediaCSVPath)
	if err != nil {
		log.Fatalf("error while loading wikidata media set csv: %s", err)
	}

	var matches []*Match

	var matchedIDs []string

	for _, item := range compareSet {
		var bestResult *Match
		tmdbMapping, exists := tmdbMediaSet[item.TmdbID]
		if !exists {
			continue
		}
		for _, possibility := range item.Possibilites {
			wikidataMapping, exists := wikidataMediaSet[possibility.WikidataID]
			if !exists {
				continue
			}

			mappingScore, mapMatchCount := calculateMappingScoreOverlapCoeff(tmdbMapping, wikidataMapping)
			totalScore := mappingScore * possibility.Score

			saveTheResult := bestResult == nil
			saveTheResult = saveTheResult || totalScore > bestResult.TotalScore
			saveTheResult = saveTheResult || totalScore == bestResult.TotalScore && possibility.Score > bestResult.NameScore

			if saveTheResult {
				bestResult = &Match{
					TmdbID:              item.TmdbID,
					TmdbCompanyName:     item.CompanyName,
					WikidataID:          possibility.WikidataID,
					WikidataCompanyName: possibility.CompanyName,
					NameScore:           possibility.Score,
					MappingScore:        mappingScore,
					TmdbMapCount:        tmdbMapping.Count,
					WikidataMapCount:    wikidataMapping.Count,
					MapMatchCount:       mapMatchCount,
					TotalScore:          totalScore,
				}
			}
		}
		// i am interested in 3 quadrants
		// POSITIVE POSITIVES
		// POSITIVE NEGATIVES (name match = low, high mapping match)
		// NEGATIVE POSITIVES (name match = high, zero mapping match)
		if bestResult != nil {
			matches = append(matches, bestResult)
			matchedIDs = append(matchedIDs, bestResult.WikidataID)
		}
	}

	sort.Slice(matches, func(i int, j int) bool {
		m1 := matches[i]
		m2 := matches[j]
		if m1.TotalScore != m2.TotalScore {
			return m1.TotalScore > m2.TotalScore
		}
		return m1.NameScore > m2.NameScore
	})

	saveMatches(matches, outputMatchCSVPath)
}

func saveMatches(matches []*Match, path string) error {
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()

	csvWriter := csv.NewWriter(f)
	// write headers
	err = csvWriter.Write([]string{
		"match",
		"tmdb_id",
		"tmdb_company_name",
		"wikidata_id",
		"wikidata_company_name",
		"total_score",
		"name_match_subscore",
		"common_media_subscore",
		"tmdb_media_count",
		"wikidata_media_count",
		"common_media_count",
	})
	if err != nil {
		return err
	}

	counts := make(map[string]int)

	for _, match := range matches {
		label := match.Label()
		counts[label] = counts[label] + 1
		if label == "NOPE" {
			continue
		}
		csvWriter.Write([]string{
			label,
			match.TmdbID,
			match.TmdbCompanyName,
			match.WikidataID,
			match.WikidataCompanyName,
			strconv.FormatFloat(match.TotalScore, 'f', 4, 64),
			strconv.FormatFloat(match.NameScore, 'f', 4, 64),
			strconv.FormatFloat(match.MappingScore, 'f', 4, 64),
			strconv.FormatInt(int64(match.TmdbMapCount), 10),
			strconv.FormatInt(int64(match.WikidataMapCount), 10),
			strconv.FormatInt(int64(match.MapMatchCount), 10),
		})
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	err = os.Rename(path+".tmp", path)
	if err != nil {
		return err
	}

	fmt.Printf("COUNTS: %v\n", counts)

	return nil
}

func (m Match) Label() string {
	nameMatchGood := m.NameScore > 0.72
	mappingMatchGood := m.MappingScore > 0.0

	if nameMatchGood && mappingMatchGood {
		return "PROBABLY"
	} else if !nameMatchGood && mappingMatchGood {
		return "MAYBE"
	} else if nameMatchGood && !mappingMatchGood {
		if m.NameScore > 0.9 {
			return "MAYBE"
		}
		return "UNLIKELY"
	} else {
		return "NOPE"
	}
}

func calculateMappingScoreOverlapCoeff(tmdbMapping *Media, wikidataMapping *Media) (float64, int) {
	var union int
	if tmdbMapping.Count > wikidataMapping.Count {
		union = wikidataMapping.Count
	} else {
		union = tmdbMapping.Count
	}
	if union == 0 {
		return 0, 0 // avoid a divide by zero error
	}

	intersection := 0
	for _, tmdbID := range tmdbMapping.Movies {
		for _, wdTmdbID := range wikidataMapping.Movies {
			if tmdbID == wdTmdbID {
				intersection += 1
			}
		}
	}
	for _, tmdbID := range tmdbMapping.TV {
		for _, wdTmdbID := range wikidataMapping.TV {
			if tmdbID == wdTmdbID {
				intersection += 1
			}
		}
	}

	// union = wikidataMapping.Count + tmdbMapping.Count - intersection
	// if union == 0 {
	// 	return 0, 0 // avoid a divide by zero error
	// }
	// return float64(intersection) / float64(union), intersection
	return float64(intersection) / float64(union), intersection
}
