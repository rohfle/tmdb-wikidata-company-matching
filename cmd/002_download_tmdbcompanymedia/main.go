package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/rohfle/quickiedata"
	"github.com/schollz/progressbar/v3"
)

const SAVE_BATCH_SIZE = 20

type Media struct {
	MediaType  string
	TmdbID     string
	Title      string
	Year       string
	Popularity string
	Poster     string
}

type Company struct {
	TmdbID string
	Name   string
	Media  []*Media
}

type TMDBDiscoverMovieResponse struct {
	Results []struct {
		ID          int64   `json:"id"`
		PosterPath  string  `json:"poster_path"`
		Popularity  float64 `json:"popularity"`
		Title       string  `json:"title"`
		ReleaseDate string  `json:"release_date"`
	} `json:"results"`
	TotalPages int64 `json:"total_pages"`
}

type TMDBDiscoverTVResponse struct {
	Results []struct {
		ID           int64   `json:"id"`
		PosterPath   string  `json:"poster_path"`
		Popularity   float64 `json:"popularity"`
		Name         string  `json:"name"`
		FirstAirDate string  `json:"first_air_date"`
	} `json:"results"`
	TotalPages int64 `json:"total_pages"`
}

// TODO: multiple pages - get all results
func tmdbRequestCompanyMediaActual(client *http.Client, tmdbAPIKey string, tmdbCompanyID string, mediaType string, page int64) ([]*Media, int64, error) {
	if mediaType != "movie" && mediaType != "tv" {
		return nil, 0, fmt.Errorf("tmdbRequestCompanyMedia: invalid mediaType %s", mediaType)
	}

	baseURL := "https://api.themoviedb.org/3/discover/" + mediaType
	var values = url.Values{}
	values.Set("api_key", tmdbAPIKey)
	values.Set("with_companies", tmdbCompanyID)
	values.Set("page", strconv.FormatInt(page, 10))
	fullURL := baseURL + "?" + values.Encode()

	resp, err := client.Get(fullURL)
	if err != nil {
		return nil, 0, fmt.Errorf("error while retrieving %s: %w", fullURL, err)
	}
	defer resp.Body.Close()

	var medias []*Media
	var pages int64

	if mediaType == "movie" {
		var response TMDBDiscoverMovieResponse
		err := json.NewDecoder(resp.Body).Decode(&response)
		if err != nil {
			return nil, 0, fmt.Errorf("error while unmarshalling discover movie response: %w", err)
		}
		for _, result := range response.Results {
			media := &Media{
				MediaType:  "movie",
				TmdbID:     strconv.FormatInt(result.ID, 10),
				Title:      result.Title,
				Popularity: strconv.FormatFloat(result.Popularity, 'f', 4, 64),
				Poster:     result.PosterPath,
				Year:       strings.SplitN(result.ReleaseDate, "-", 2)[0],
			}
			medias = append(medias, media)
		}
		pages = response.TotalPages
	} else if mediaType == "tv" {
		var response TMDBDiscoverTVResponse
		err := json.NewDecoder(resp.Body).Decode(&response)
		if err != nil {
			return nil, 0, fmt.Errorf("error while unmarshalling discover tv response: %w", err)
		}
		for _, result := range response.Results {
			media := &Media{
				MediaType:  "tv",
				TmdbID:     strconv.FormatInt(result.ID, 10),
				Title:      result.Name,
				Popularity: strconv.FormatFloat(result.Popularity, 'f', 4, 64),
				Poster:     result.PosterPath,
				Year:       strings.SplitN(result.FirstAirDate, "-", 2)[0],
			}
			medias = append(medias, media)
		}
		pages = response.TotalPages
	}
	return medias, pages, nil
}

func tmdbRequestCompanyMedia(client *http.Client, tmdbAPIKey string, tmdbCompanyID string, mediaType string) ([]*Media, error) {
	var page int64
	var totalPages int64 = 1
	const MAX_PAGES = 1000

	var allmedias []*Media

	for page = 1; page <= totalPages; page++ {
		medias, pageCount, err := tmdbRequestCompanyMediaActual(client, tmdbAPIKey, tmdbCompanyID, mediaType, page)
		if err != nil {
			return nil, err
		}
		if page == 1 {
			if pageCount > MAX_PAGES {
				pageCount = MAX_PAGES
			}
			totalPages = pageCount
		}
		allmedias = append(allmedias, medias...)
	}

	return allmedias, nil
}

func tmdbGetCompanyMedia(client *http.Client, tmdbAPIKey string, tmdbCompanyID string) ([]*Media, error) {
	medias, err := tmdbRequestCompanyMedia(client, tmdbAPIKey, tmdbCompanyID, "movie")
	if err != nil {
		return nil, err
	}
	medias2, err := tmdbRequestCompanyMedia(client, tmdbAPIKey, tmdbCompanyID, "tv")
	if err != nil {
		return nil, err
	}
	return append(medias, medias2...), nil
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
			TmdbID:     record[2],
			MediaType:  record[3],
			Title:      record[4],
			Year:       record[5],
			Popularity: record[6],
			Poster:     record[7],
		}

		if companyID == "" {
			continue
		}

		company, exists := companiesLUT[companyID]
		if !exists {
			company = &Company{
				TmdbID: companyID,
				Name:   companyName,
			}
			companiesLUT[companyID] = company
		}

		if media.TmdbID != "" {
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
	err = csvWriter.Write([]string{"company_id", "company_name", "id", "type", "title", "year", "popularity", "poster"})
	if err != nil {
		return err
	}

	for _, company := range companiesLUT {
		if len(company.Media) == 0 {
			err = csvWriter.Write([]string{
				company.TmdbID,
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
					company.TmdbID,
					company.Name,
					media.TmdbID,
					media.MediaType,
					media.Title,
					media.Year,
					media.Popularity,
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
	client := quickiedata.QuickieHTTPClient(&quickiedata.HTTPClientSettings{
		UserAgent:       "quickiedata (rohfle@gmail.com) Wikidata:Property_proposal/TMDB_company_ID",
		RequestInterval: 300 * time.Millisecond,
		Backoff:         1 * time.Second,
		MaxBackoff:      30 * time.Second,
		MaxRetries:      5,
		MaxConnsPerHost: 1,
	})

	compareCSVPath := os.Args[1]
	mediaMappingCSVPath := os.Args[2]
	TMDB_API_KEY := os.Getenv("TMDB_API_KEY")
	if TMDB_API_KEY == "" {
		log.Fatal("TMDB_API_KEY environment variable not set")
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

	tmdbIDIdx := FindInSlice(headers, "tmdbID")
	tmdbNameIdx := FindInSlice(headers, "tmdbName")

	if tmdbIDIdx == -1 ||
		tmdbNameIdx == -1 {
		log.Fatal("Invalid CSV given: must have fields tmdbID, tmdbName")
	}

	companiesLUT, err := loadLUT(mediaMappingCSVPath)
	if err != nil {
		log.Fatal(err)
	}

	bar := progressbar.NewOptions(rowCount,
		progressbar.OptionSetWidth(15),
		progressbar.OptionEnableColorCodes(true),
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

		tmdbID := record[tmdbIDIdx]
		tmdbName := record[tmdbNameIdx]

		if tmdbID == "" {
			continue
		}

		if company, exists := companiesLUT[tmdbID]; !exists || len(company.Media) >= 20 {
			companiesLUT[tmdbID] = &Company{
				TmdbID: tmdbID,
				Name:   tmdbName,
			}
			bar.Describe("Getting media for company " + tmdbID + " " + tmdbName)
			medias, err := tmdbGetCompanyMedia(client, TMDB_API_KEY, tmdbID)
			if err != nil {
				log.Fatal(err)
			}
			companiesLUT[tmdbID].Media = medias
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
