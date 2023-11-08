import ee
import numpy as np
import pandas as pd
import geopandas as gpd
import json

def getArea(GID_0):
    area = gpd.read_file(f'../../reference_datasets/gadm_401_GID_1/{GID_0[0:3]}.gpkg')

    area = area.loc[area.GID_0 == GID_0]
    #area = area.dissolve("GID_0").reset_index()
    #area.geometry = area.simplify(0.1)
    area = ee.FeatureCollection(json.loads(area.to_json()))
    return area

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

class ZsGEE:

    def __init__(self,):

        # some init here
        self.some_var_here = ""

    def get_dataframe(self):

        startDate =  self.datestart  # '2000-03-01' # time period of interest beginning date
        interval = self.timeWindowLength  # time window length
        intervalUnit = self.intervalUnit #'month'  # unit of time e.g. 'year', 'month', 'day'
        intervalCount = self.intervalCount  # 275 # number of time windows in the series

        area = getArea(self.GadmGID)
        dataset = ee.ImageCollection(self.satellite).select(self.bands)
        temporalReducer = ee.Reducer.sum()  # how to reduce images in time window

        # Defines mean, standard deviation, and variance as the zonal statistics.
        spatialReducers = ee.Reducer.sum()
        # Get time window index sequence.
        intervals = ee.List.sequence(0, intervalCount - 1, interval)

        # Map reductions over index sequence to calculate statistics for each interval.
        def a(i):
            # Calculate temporal composite.
            startRangeL = ee.Date(startDate).advance(i, intervalUnit)
            endRangeL = startRangeL.advance(interval, intervalUnit)
            temporalStat = dataset.filterDate(startRangeL, endRangeL).reduce(temporalReducer).multiply(lc)


            # Calculate zonal statistics.
            statsL = temporalStat.reduceRegions(
                collection=area,
                reducer=spatialReducers,
                scale= dataset.first().projection().nominalScale().getInfo(),
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
        return out_pd




if __name__ == '__main__':

    credentials = ee.ServiceAccountCredentials(
        "test1-landsat8@geelandsat8.iam.gserviceaccount.com",
        "C:\\Users\\email\\Documents\\conflictproxyindicators\\Z_experiments\\2023_MU_Sierra_Leone_civil_conflcit\\geelandsat8-6af86334d7ec.json",
    )
    ee.Initialize(credentials)


    dfs=[]
    for year in range(2013,2023):
        print(year)
        myPanelData = ZsGEE()
        myPanelData.intervalCount = 12
        myPanelData.timeWindowLength = 1
        myPanelData.intervalUnit = "month"
        myPanelData.datestart = f"{year}-01-01"
        myPanelData.satellite = "ECMWF/ERA5_LAND/MONTHLY_AGGR"
        myPanelData.bands = ["total_precipitation_sum"]
        myPanelData.temporal_reducer = "sum"
        myPanelData.GadmGID = 'IND'

        df = myPanelData.get_dataframe()
        dfs.append(df)


    ####

    df = myPanelData.get_dataframe()
    dfs.append(df)

    myPanelData = ZsGEE()
    myPanelData.intervalCount = 9
    myPanelData.timeWindowLength = 1
    myPanelData.intervalUnit = "month"
    myPanelData.datestart = "2023-01-01"
    myPanelData.satellite = "ECMWF/ERA5_LAND/MONTHLY_AGGR"
    myPanelData.bands = ["total_precipitation_sum"]

    myPanelData.GadmGID = 'YEM'

    df = myPanelData.get_dataframe()
    dfs.append(df)
    dfs = pd.concat(dfs)
    dfs.to_csv("total_precipitation_yemen_2013-2023.csv")




