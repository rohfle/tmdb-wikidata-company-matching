import requests
import os
import sys
import json
import time
import csv
import shutil
from tqdm import tqdm
from copy import copy
from collections import defaultdict

TMDB_API_KEY = os.environ["TMDB_API_KEY"]

compare_csv_path = sys.argv[1]
media_mapping_csv_path = sys.argv[2]


def load_data(path):
    media_lut = defaultdict(lambda: defaultdict(dict))
    company_lut = {}

    try:
        with open(path) as f:
            reader = csv.DictReader(f)
            for row in reader:
                cid = row['company_id']
                cname = row['company_name']
                if cname != '' and cid not in company_lut:
                    company_lut[cid] = cname
                if row['id'] == '':
                    continue  # maybe a blank result
                item = { k: row[k] for k in ['id', 'type', 'title', 'year', 'poster', 'popularity'] }
                media_lut[cid][item['type']][item['id']] = item
    except Exception as err:
        print("Error while loading existing data:", str(err))

    return media_lut, company_lut


def save_data(media_lut, company_lut, path):
    with open(path + '.tmp', 'w') as f:
        writer = csv.DictWriter(f, fieldnames=[
            'company_id', 'company_name', 'id', 'type', 'title', 'year', 'popularity', 'poster'
        ])

        writer.writeheader()

        for cid, bytype in media_lut.items():
            items = [item for byid in bytype.values() for item in byid.values()]
            if len(items) == 0:
                # write something anyway to show no results
                row = defaultdict(str, company_id=cid, company_name=company_lut[cid])
                writer.writerow(row)
            for item in items:
                row = copy(item)
                row['company_id'] = cid
                row['company_name'] = company_lut.get(cid)
                writer.writerow(row)

    shutil.move(path + '.tmp', path)


def make_request(company_id, mediatype):
    if mediatype not in ['movie', 'tv']:
        raise Exception(f'invalid mediatype "{mediatype}"')

    base_url = f'https://api.themoviedb.org/3/discover/{mediatype}'
    params = {
        "api_key": TMDB_API_KEY,
        "with_companies": company_id,
    }
    rendered_params = "&".join(["%s=%s" % item for item in params.items()])
    url = f'{base_url}/?' + rendered_params
    response = requests.get(url)

    items = []
    data = response.json()
    for result in data['results']:
        item = {
            'id': str(result['id']),
            'type': mediatype,
            'poster': result.get('poster_path'),
            'popularity': str(result['popularity']),
        }
        if mediatype == 'movie':
            item['title'] = result['title']
            item['year'] = result.get('release_date', '')[:4]
        elif mediatype == 'tv':
            item['title'] = result['name']
            item['year'] = result.get('first_air_date', '')[:4]
        items += [item]
    return items



media_lut, company_lut = load_data(media_mapping_csv_path)

with open(compare_csv_path) as f:
    row_count = sum(1 for _ in f) - 1

if row_count <= 0:
    exit()

with open(compare_csv_path) as f:
    reader = csv.DictReader(f)
    rows_unsaved = 0

    pbar = tqdm(reader, dynamic_ncols=True, total=row_count)
    for rowidx, row in enumerate(pbar):
        cid = row['tmdbID']
        cname = row['tmdbName']

        if float(row['result1Score']) < 0.75:
            continue  # skip low scoring results for now

        if cid in company_lut:
            pbar.set_description(f"Skipping already retrieved [{cid}]: {cname}".ljust(80))
            continue

        company_lut[cid] = cname
        pbar.set_description(f"Retrieving [{cid}]: {cname}".ljust(80))
        media_lut[cid] = defaultdict(dict)

        for mediatype in ['movie', 'tv']:
            for item in make_request(cid, mediatype):
                media_lut[cid][mediatype][item['id']] = item

        rows_unsaved += 1
        if rows_unsaved >= 100:
            pbar.set_description("Saving progress...".ljust(80))
            save_data(media_lut, company_lut, media_mapping_csv_path)
            rows_unsaved = 0

    if rows_unsaved > 0:
        print("Saving unsaved entries...")
        save_data(media_lut, company_lut, media_mapping_csv_path)


