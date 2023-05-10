package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rohfle/quickiedata"
	"github.com/schollz/progressbar/v3"
)

const RETRIEVE_BATCH_SIZE = 10
const SAVE_BATCH_SIZE = 100

type Media struct {
	ID        string
	MediaType string
	TmdbID    string
	Title     string
	Year      string
	Sitelinks string
	Poster    string
}

type Company struct {
	ID    string
	Name  string
	Media []*Media
}

func FindInSlice(haystack []string, needle string) int {
	for idx, hay := range haystack {
		if hay == needle {
			return idx
		}
	}
	return -1
}

func estimateRowCount(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	buf := make([]byte, 32*1024)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := f.Read(buf)
		count += bytes.Count(buf[:c], lineSep)

		switch {
		case err == io.EOF:
			return count, nil

		case err != nil:
			return count, err
		}
	}
}

func loadLUT(path string) (map[string]*Company, error) {
	var companiesLUT = make(map[string]*Company)
	f, err := os.Open(path)
	if err != nil {
		if errCast, ok := err.(*fs.PathError); ok && errCast.Err == syscall.ENOENT {
			return companiesLUT, nil // no such file - this is ok, just return an empty lut
		}
		return nil, err
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	// read headers
	_, err = csvReader.Read()
	if err != nil {
		return nil, err
	}

	for {
		record, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		companyID := record[0]
		companyName := record[1]
		media := &Media{
			ID:        record[2],
			MediaType: record[3],
			Title:     record[4],
			Year:      record[5],
			Sitelinks: record[6],
			Poster:    record[7],
		}

		if companyID == "" || companyID[0] != 'Q' {
			continue
		}

		company, exists := companiesLUT[companyID]
		if !exists {
			company = &Company{
				ID:   companyID,
				Name: companyName,
			}
			companiesLUT[companyID] = company
		}

		if media.ID != "" {
			company.Media = append(company.Media, media)
		}

	}
	return companiesLUT, nil
}

func saveLUT(companiesLUT map[string]*Company, path string) error {
	f, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	defer f.Close()

	csvWriter := csv.NewWriter(f)
	// write headers
	err = csvWriter.Write([]string{"company_id", "company_name", "id", "type", "title", "year", "sitelinks", "poster"})
	if err != nil {
		return err
	}

	for _, company := range companiesLUT {
		if len(company.Media) == 0 {
			err = csvWriter.Write([]string{
				company.ID,
				company.Name,
				"",
				"",
				"",
				"",
				"",
				"",
			})
			if err != nil {
				return err
			}
		} else {
			for _, media := range company.Media {
				err = csvWriter.Write([]string{
					company.ID,
					company.Name,
					media.TmdbID,
					media.MediaType,
					media.Title,
					media.Year,
					media.Sitelinks,
					media.Poster,
				})
				if err != nil {
					return err
				}
			}
		}
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

	return nil
}

func main() {
	wd := quickiedata.NewWikidataClient(&quickiedata.HTTPClientSettings{
		UserAgent:       "quickiedata (rohfle@gmail.com) Wikidata:Property_proposal/TMDB_company_ID",
		RequestInterval: 1 * time.Second,
		Backoff:         1 * time.Second,
		MaxBackoff:      30 * time.Second,
		MaxRetries:      5,
		MaxConnsPerHost: 1,
	})

	compareCSVPath := os.Args[1]
	mediaMappingCSVPath := os.Args[2]
	forceRefresh := false
	if len(os.Args) > 3 && os.Args[3] == "--force" {
		forceRefresh = true
	}

	rowCount, err := estimateRowCount(compareCSVPath)
	if err != nil {
		log.Fatal(err)
	}

	if rowCount <= 1 {
		fmt.Println("Nothing to do")
		return // nothing to do
	}

	f, err := os.Open(compareCSVPath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	headers, err := csvReader.Read()
	if err != nil {
		log.Fatal(err)
	}

	result1IDIdx := FindInSlice(headers, "result1ID")
	result1NameIdx := FindInSlice(headers, "result1Name")
	result2IDIdx := FindInSlice(headers, "result2ID")
	result2NameIdx := FindInSlice(headers, "result2Name")

	if result1IDIdx == -1 ||
		result1NameIdx == -1 ||
		result2IDIdx == -1 ||
		result2NameIdx == -1 {
		log.Fatal("Invalid CSV given: must have fields result1ID, result1Name, result2ID, result2Name")
	}

	companiesLUT, err := loadLUT(mediaMappingCSVPath)
	if err != nil {
		log.Fatal(err)
	}

	var companyIDsToGet = make([]string, 0, 21)

	bar := progressbar.NewOptions(rowCount,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(15),
		progressbar.OptionShowCount(),
		progressbar.OptionShowDescriptionAtLineEnd(),
		progressbar.OptionSetDescription("[cyan][1/3][reset] Writing moshable file..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))

	recordsUnsaved := 0
	for {
		bar.Describe("Reading rows...")
		record, err := csvReader.Read()
		bar.Add(1)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		cID1 := record[result1IDIdx]
		cName1 := record[result1NameIdx]
		cID2 := record[result2IDIdx]
		cName2 := record[result2NameIdx]

		if cID1 == "" || cID1[0] != 'Q' {
			continue
		}

		if _, exists := companiesLUT[cID1]; forceRefresh || !exists {
			companiesLUT[cID1] = &Company{
				ID:   cID1,
				Name: cName1,
			}
			companyIDsToGet = append(companyIDsToGet, cID1)
		}

		if cID2 == "" || cID1[0] != 'Q' {
			continue
		}

		if _, exists := companiesLUT[cID2]; forceRefresh || !exists {
			companiesLUT[cID2] = &Company{
				ID:   cID2,
				Name: cName2,
			}
			companyIDsToGet = append(companyIDsToGet, cID2)
		}

		if len(companyIDsToGet) >= RETRIEVE_BATCH_SIZE {
			bar.Describe("Getting " + strings.Join(companyIDsToGet, ", "))
			medias, err := getWDCompanyMedia(wd, companyIDsToGet)
			if err != nil {
				log.Fatal(err)
			}
			// empty the slice
			companyIDsToGet = companyIDsToGet[:0]

			for companyID, media := range medias {
				companiesLUT[companyID].Media = media
				recordsUnsaved += 1
			}

			if recordsUnsaved >= SAVE_BATCH_SIZE {
				bar.Describe("Saving to disk...")
				err := saveLUT(companiesLUT, mediaMappingCSVPath)
				if err != nil {
					log.Fatal(err)
				}
				recordsUnsaved = 0
			}
		}
	}

	// Handle unprocessed entities
	if len(companyIDsToGet) > 0 {
		bar.Describe("Getting " + strings.Join(companyIDsToGet, ", "))
		medias, err := getWDCompanyMedia(wd, companyIDsToGet)
		if err != nil {
			log.Fatal(err)
		}

		for companyID, media := range medias {
			companiesLUT[companyID].Media = media
			recordsUnsaved += 1
		}
	}

	if recordsUnsaved > 0 {
		bar.Describe("Saving to disk...")
		err := saveLUT(companiesLUT, mediaMappingCSVPath)
		if err != nil {
			log.Fatal(err)
		}
		recordsUnsaved = 0
	}
	bar.Finish()
}

func getWDCompanyMedia(wd *quickiedata.WikidataClient, companyIDs []string) (map[string][]*Media, error) {

	query := quickiedata.NewSPARQLQuery()
	query.Template = `
		SELECT ?item ?productionCompany ?itemLabel ?linkCount ?poster ?tmdbMovieID ?tmdbTVID (MIN(?year2) AS ?year)
		WHERE
		{
		?item wdt:P272 ?productionCompany.
		?item wikibase:sitelinks ?linkCount .
		OPTIONAL { ?item wdt:P3383 ?poster }
		OPTIONAL {
			?item wdt:P577 ?pubDate.
			BIND(YEAR(?pubDate) AS ?year2)
		}
		OPTIONAL { ?item wdt:P4947 ?tmdbMovieID }
		OPTIONAL { ?item wdt:P4983 ?tmdbTVID }
		FILTER(?tmdbMovieID != "" || ?tmdbTVID != "")
		OPTIONAL {
			?item rdfs:label ?itemLabel
			FILTER langMatches(lang(?itemLabel), "en")
		}
		OPTIONAL {
			?item rdfs:label ?itemLabel
		}
		} GROUP BY ?item ?productionCompany ?itemLabel ?linkCount ?poster ?tmdbMovieID ?tmdbTVID ORDER BY DESC(?linkCount)
	`

	var cids []quickiedata.WikidataID
	for _, cid := range companyIDs {
		cids = append(cids, quickiedata.WikidataID("wd:"+cid))
	}

	query.Variables["productionCompany"] = cids

	options := quickiedata.NewSPARQLQueryOptions()
	sdata, err := wd.SPARQLQuerySimple(context.Background(), query, options)
	if err != nil {
		return nil, fmt.Errorf("error in SPARQLQuerySimple: %w", err)
	}

	var mediaList = make(map[string]map[string]*Media)
	for _, result := range sdata.Results {
		companyID := result["productionCompany"].ValueAsString()
		companyMedia := mediaList[companyID]
		if companyMedia == nil {
			companyMedia = make(map[string]*Media) // TODO: change to a map to prevent duplicates
		}
		media := &Media{
			ID:     result["item"].ValueAsString(),
			Title:  result["itemLabel"].ValueAsString(),
			Poster: result["poster"].ValueAsString(),
		}

		if year := result["year"].ValueAsInteger(); year != nil {
			media.Year = strconv.FormatInt(*year, 10)
		}

		if sitelinks := result["linkCount"].ValueAsInteger(); sitelinks != nil {
			media.Sitelinks = strconv.FormatInt(*sitelinks, 10)
		}

		if v := result["tmdbMovieID"]; v != nil {
			media.MediaType = "movie"
			media.TmdbID = v.ValueAsString()
		} else if v := result["tmdbTVID"]; v != nil {
			media.MediaType = "tv"
			media.TmdbID = strings.SplitN(v.ValueAsString(), "/", 2)[0] // removes 111/season/1 etc
		}
		companyMedia[media.ID] = media
		mediaList[companyID] = companyMedia
	}

	var mediaListOut = make(map[string][]*Media)
	for companyID, mediaLUT := range mediaList {
		medias := make([]*Media, 0, len(mediaLUT))
		for _, media := range mediaLUT {
			medias = append(medias, media)
		}
		mediaListOut[companyID] = medias
	}

	return mediaListOut, nil
}
