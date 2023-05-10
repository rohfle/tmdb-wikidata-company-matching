# Steps

## load company data

https://files.tmdb.org/p/exports/production_company_ids_MM_DD_YYYY.json.gz

https://query.wikidata.org/#%23%20List%20companies%20that%20are%20a%20production%20company%20for%20an%20audiovisual%20work%0ASELECT%20DISTINCT%20%3Fpcomp%20%3FpcompLabel%20%3Flogo%0AWHERE%20%0A%7B%0A%20%20%3Fitem%20wdt%3AP31%2Fwdt%3AP279%2B%20wd%3AQ2431196.%0A%20%20%3Fitem%20wdt%3AP272%20%3Fpcomp.%0A%20%20OPTIONAL%20%7B%3Fpcomp%20wdt%3AP154%20%3Flogo%20%7D%0A%20%20SERVICE%20wikibase%3Alabel%20%7B%20bd%3AserviceParam%20wikibase%3Alanguage%20%22%5BAUTO_LANGUAGE%5D%2Cen%22.%20%7D%0A%7D

## title compare

Compares the titles using levenshtein

```sh
go run cmd/001_titlecompare/main.go production_company_ids_MM_DD_YYYY.json.gz wikidata-companies.csv title_compare.csv
```

## download media ids

Downloads media ids for company id

```sh
export TMDB_API_KEY="<your key>"
go run cmd/002_download_tmdbcompanymedia/main.go title_compare.csv tmdb_media_mapping.csv
go run cmd/003_download_wikidatacompanymedia/main.go title_compare.csv wikidata_media_mapping.csv
```

## compare media ids

Compare media id sets for company in tmdb and wikidata and find best match

```sh
	titleCompareCSVPath := os.Args[1]
	tmdbMediaCSVPath := os.Args[2]
	wikidataMediaCSVPath := os.Args[3]
	outputMatchCSVPath := os.Args[4]

go run cmd/004_mediaidscompare/main.go title_compare.csv tmdb_media_mapping.csv wikidata_media_mapping.csv result.csv
```

## example output

[here](./result_2023-05-10.csv)

## labels

- **PROBABLY** - name similar, at least one common media
- **MAYBE** - name not similar, at least one common media OR name very similar, no common media
- **UNLIKELY** - name similar, no common media
- **NOPE** - name not similar, no common media

## links

https://www.wikidata.org/wiki/Wikidata:Property_proposal/TMDB_company_ID
