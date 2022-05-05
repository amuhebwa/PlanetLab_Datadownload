import asyncio
import planet
from planet import api
import requests
from requests.auth import HTTPBasicAuth
from multiprocessing.dummy import Pool as ThreadPool
import os
import json
import rasterio
from matplotlib import pyplot as plt
import numpy as np

item_type = "PSOrthoTile" #PSScene,
scene_type = u"analytic"
base_url = "https://api.planet.com/data/v1/quick-search"


PLANET_API_KEY = os.getenv('PL_API_KEY') #$PL_API_KEY

def activate_item(item_id):
    image_url = 'https://api.planet.com/data/v1/item-types/{}/items/{}/assets'.format(item_type, item_id)
    result = requests.get(image_url, auth=HTTPBasicAuth(PLANET_API_KEY, ''))
    if result.json()[scene_type]['status'] != "active":
        print('Activating Item Id: {}'.format(item_id))
        links = result.json()[scene_type]["_links"]
        self_link = links["_self"]
        activation_link = links["activate"]
        activate_result = requests.get(activation_link, auth=HTTPBasicAuth(PLANET_API_KEY, ''))
    else:
        print('{} is ACTIVE'.format(item_id))


def check_activation_stations(item_id_arr):
    for _, item_id in enumerate(item_id_arr):
        print('Checking Activation Status for Item Id: {}'.format(item_id))
        image_url = 'https://api.planet.com/data/v1/item-types/{}/items/{}/assets'.format(item_type, item_id)
        result = requests.get(image_url, auth=HTTPBasicAuth(PLANET_API_KEY, ''))
        print('Status: {}'.format(result.json()[scene_type]['status']))

def get_download_link(item_id):
    image_url = 'https://api.planet.com/data/v1/item-types/{}/items/{}/assets'.format(item_type, item_id)
    result = requests.get(image_url, auth=HTTPBasicAuth(PLANET_API_KEY, ''))
    links = result.json()[scene_type]["_links"]
    self_link = links["_self"]
    activation_status_result = requests.get(self_link, auth=HTTPBasicAuth(PLANET_API_KEY, ''))
    status = activation_status_result.json()["status"]

    if status == 'active':
        download_link = activation_status_result.json()["location"]
    else:
        download_link = None
        print('Not yet active')
    return download_link


with open('nairobi_bounds_planet_3m_sample.geojson') as f:
	aoi = json.loads(f.read())
geojson_geometry = aoi.get('features')[0].get('geometry')

# create a geometry filter
geometry_filter = {
    "type": "GeometryFilter",
    "field_name": "geometry",
    "config": geojson_geometry
    }
# create a date filter
date_range_filter = {
    "type": "DateRangeFilter",
    "field_name": "acquired",
    "config": {
        "gte": "2018-01-01T00:00:00.000Z",
        "lte": "2018-01-06T00:00:00.000Z"
        }
    }
# Get images which have < 50% of the clouds.
# specifically, Nairobi area seems to have lots of clouds so we set a low filter
cloud_cover_filter = {
    "type": "RangeFilter",
    "field_name": "cloud_cover",
    "config": {
        "lte": 0.50
        }
    }
# combined all filters into a single object
combined_filter = {
    "type": "AndFilter",
    "config": [geometry_filter, date_range_filter, cloud_cover_filter]
    }

search_request = {
    "item_types":[item_type],
    "filter": combined_filter
    }

 # get image Ids
search_result = requests.post(base_url, auth=HTTPBasicAuth(PLANET_API_KEY, ''), json=search_request)
#print(json.dumps(search_result.json(), indent=1))

# Extract image IDs
image_ids = [feature['id'] for feature in search_result.json()['features']]
# select 10 images for activation
image_ids = image_ids[0:10]
arallelism=5

def parallel_activation(image_ids, parallelism):
    thread_pool = ThreadPool(parallelism)
    thread_pool.map(activate_item, image_ids)

#parallel_activation(image_ids, parallelism)
check_activation_stations(image_ids)
download_link = get_download_link(image_ids[0])
print(download_link)

f = '/Users/aggreymuhebwa/Downloads/1040538_3738308_2018-01-05_102c_BGRN_Analytic.tif'

