import os
import ee
import json
import numpy as np
import requests
import geopandas as gpd

################################################################################################

credentials = ee.ServiceAccountCredentials(
    "test1-landsat8@geelandsat8.iam.gserviceaccount.com",
    "geelandsat8-6af86334d7ec.json",
)
ee.Initialize(credentials)

SATELLITE = "ECMWF/ERA5_LAND/MONTHLY_AGGR"
BANDS = ["total_precipitation_sum"]

##Rectangualr box ref:minx, miny, maxx, maxy
#REGION = ee.Geometry.BBox(8, 45, 9, 46)

###Region by polygon:
REGION = gpd.read_file(f'reference_datasets/gadm_401_GID_1/IND.gpkg')
REGION = REGION.dissolve("GID_0").reset_index()
REGION.geometry = REGION.simplify(0.1)
REGION.drop(columns=['GID_1', 'NAME_1'], axis=1, inplace=True )
REGION = ee.FeatureCollection(json.loads(REGION.to_json()))

START_DATE = "2000-01-01"
END_DATE = "2000-06-01"

image = ee.ImageCollection(SATELLITE).filterDate(START_DATE, END_DATE).filterBounds(REGION)
image = image.sum()


##If region by Polygon (not rect) remember to clip
image = image.clip(REGION)
image = image.mask(image.mask())

SCALE = ee.ImageCollection(SATELLITE).first().projection().nominalScale().getInfo()

url = image.getDownloadUrl({
    'bands': BANDS,
    'scale': SCALE,
    'crs': "EPSG:4326",
    'region': REGION.geometry(),
    'format': 'GEO_TIFF'
})

response = requests.get(url)

with open("myImage_INDIA_clipped.tif", 'wb') as fd:
    fd.write(response.content)

print("done")


