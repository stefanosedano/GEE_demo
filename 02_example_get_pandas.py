import os
import ee
import json
import numpy as np
import geopandas as gpd
import pandas as pd

################################################################################################

def fc_to_dict(fc):
    prop_names = fc.first().propertyNames()
    prop_lists = fc.reduceColumns(
        reducer=ee.Reducer.toList().repeat(prop_names.size()), selectors=prop_names
    ).get("list")

    return ee.Dictionary.fromLists(prop_names, prop_lists)

def pad_dict_list(dict_list, padel):
    lmax = 0
    for lname in dict_list.keys():
        lmax = max(lmax, len(dict_list[lname]))
    for lname in dict_list.keys():
        ll = len(dict_list[lname])
        if ll < lmax:
            dict_list[lname] += [padel] * (lmax - ll)
    return dict_list



credentials = ee.ServiceAccountCredentials(
    "test1-landsat8@geelandsat8.iam.gserviceaccount.com",
    "geelandsat8-6af86334d7ec.json",
)
ee.Initialize(credentials)

area = gpd.read_file(f'reference_datasets/gadm_401_GID_1/IND.gpkg')
#area = area.dissolve("GID_1").reset_index()
area.geometry = area.simplify(0.1)
area = ee.FeatureCollection(json.loads(area.to_json()))

SATELLITE = "ECMWF/ERA5_LAND/MONTHLY_AGGR"

BANDS = ["total_precipitation_sum"]

SCALE = ee.ImageCollection(SATELLITE).first().projection().nominalScale().getInfo()

dataset = ee.ImageCollection(SATELLITE).select(BANDS)

startDate="1950-03-01"
intervalUnit="month"
timeWindowLength = 1
intervalCount = 12*5

temporalReducer = ee.Reducer.sum()
spatialReducers = ee.Reducer.sum()

intervals = ee.List.sequence(0, intervalCount - 1, timeWindowLength)

# Map reductions over index sequence to calculate statistics for each interval.
def a(i):
    # Calculate temporal composite.
    startRangeL = ee.Date(startDate).advance(i, intervalUnit)
    endRangeL = startRangeL.advance(timeWindowLength, intervalUnit)
    temporalStat = dataset.filterDate(startRangeL, endRangeL).reduce(temporalReducer)

    # Calculate zonal statistics.
    statsL = temporalStat.reduceRegions(
        collection=area,
        reducer=spatialReducers,
        scale=dataset.first().projection().nominalScale().getInfo(),
        crs=dataset.first().projection()
    )

    # Set start date as a feature property.

    def b(feature):
        #  or 'YYYY-MM-dd'
        return feature.set({'composite_start': startRangeL.format('YYYYMM')})

    return statsL.map(b)


zonalStatsL = intervals.map(a)

zonalStatsL = ee.FeatureCollection(zonalStatsL).flatten()

output = fc_to_dict(zonalStatsL).getInfo()
output = pad_dict_list(output, np.nan)
out_pd = pd.DataFrame(output)

print("done")
