##https://developers.google.com/earth-engine/datasets/catalog/NOAA_VIIRS_DNB_MONTHLY_V1_VCMCFG#description
##credit https://data.ngdc.noaa.gov/instruments/remote-sensing/passive/spectrometers-radiometers/imaging/viirs/dnb_composites/v10/README_dnb_composites_v1.txt
#https://eogdata.mines.edu/products/vnl/
##Any VNL
##C. D. Elvidge, K. E. Baugh, M. Zhizhin, and F.-C. Hsu, “Why VIIRS data are superior to DMSP for mapping nighttime lights,” Asia-Pacific Advanced Network 35, vol. 35, p. 62, 2013.

import io
import numpy as np
import os
from osgeo import gdal
import ee
import pandas as pd
import geopandas as gpd
import json
from multiprocessing import Pool
from sklearn.preprocessing  import minmax_scale
from datetime import timedelta
import datetime



def writeGeoTiff_v3(npArray,geoTrans,outpath,dataType,wkt,MEM):

    if (MEM == 1):
        driver = gdal.GetDriverByName('MEM')
    else:
        driver = gdal.GetDriverByName('GTiff')
    dataset = driver.Create(
         outpath,
         npArray.shape[1],
         npArray.shape[0],
         1,
         dataType,
         ['COMPRESS=LZW'])



    if (geoTrans != ''):
        dataset.SetGeoTransform(geoTrans)
    dataset.GetRasterBand(1).WriteArray(npArray)
    if (wkt != ''):
        dataset.SetProjection(wkt)

    dataset.FlushCache()

    return dataset


def warp_and_match_resolution(in_path,out_path,match_file):
    # Resample with GDAL warp
    source_image_metadata = gdal.Open(in_path,gdal.GA_ReadOnly)
    source_match=gdal.Open(match_file,gdal.GA_ReadOnly)

    geo_t = source_match.GetGeoTransform()
    x_size=source_match.RasterXSize
    y_size=source_match.RasterYSize
    xmin = min(geo_t[0], geo_t[0] + x_size * geo_t[1])
    xmax = max(geo_t[0], geo_t[0] + x_size * geo_t[1])
    ymin = min(geo_t[3], geo_t[3] + y_size * geo_t[5])
    ymax = max(geo_t[3], geo_t[3] + y_size * geo_t[5])

    try:
        gdal.Warp(out_path,
                          in_path,
                          dstSRS='EPSG:4326',
                          outputType=gdal.GDT_UInt16,
                          xRes=source_match.GetGeoTransform()[1], yRes=source_match.GetGeoTransform()[1],
                          resampleAlg="average",
                          options=['-te', str(xmin), str(ymin), str(xmax), str(ymax)]
                          )

        return True
    except Exception as e:
        print(e)

def warp_to_resolution(in_path,out_path,resolution):
    # Resample with GDAL warp
    source_image_metadata = gdal.Open(in_path,gdal.GA_ReadOnly)
    geo_t = source_image_metadata.GetGeoTransform()
    x_size=source_image_metadata.RasterXSize
    y_size=source_image_metadata.RasterYSize
    xmin = min(geo_t[0], geo_t[0] + x_size * geo_t[1])
    xmax = max(geo_t[0], geo_t[0] + x_size * geo_t[1])
    ymin = min(geo_t[3], geo_t[3] + y_size * geo_t[5])
    ymax = max(geo_t[3], geo_t[3] + y_size * geo_t[5])

    xmin =int(xmin / resolution) * resolution - resolution
    xmax = int(xmax / resolution) * resolution + resolution
    ymin= int(ymin / resolution) * resolution - resolution
    ymax = int(ymax / resolution) * resolution + resolution


    try:
        gdal.Warp(out_path,
                          in_path,
                          dstSRS='EPSG:4326',
                          outputType=gdal.GDT_UInt16,
                          xRes=resolution, yRes=resolution,
                          resampleAlg="average",
                          options=['-te', str(xmin), str(ymin), str(xmax), str(ymax)]
                          )


        return True
    except Exception as e:
        print(e)


def getArea(GID_0,LEVEL_AGG):
    area = gpd.read_file(f'reference_datasets/gadm_401_GID_1/{GID_0[0:3]}.gpkg')
    if LEVEL_AGG == "GID_0":
        area = area.dissolve("GID_0").reset_index()
        area.drop(columns=['GID_1', 'NAME_1'], axis=1, inplace=True )

    if LEVEL_AGG == "GID_1":
        area = area.loc[area.GID_1 == GID_0]
        area = area.dissolve("GID_1").reset_index()
        area.drop(columns=['GID_0', 'NAME_0'], axis=1, inplace=True )


    area = ee.FeatureCollection(json.loads(area.to_json()))
    return area



def get_image_url(image,bands,scale,region):
    import requests


    path = image.getDownloadUrl({
        'bands': bands,
        'scale': scale,
        'crs': "EPSG:4326",
        'region': region,
        'format': 'GEO_TIFF'
    })

    response = requests.get(path)
    return response


def process(el):
    lon, lat, lon_lon_steps, lat_lat_steps, startDate, endDate, year, quarter,basepath,email,keypath = el

    basepath = f"{basepath}/{year}/{quarter}/"
    if not os.path.exists(basepath):
        os.makedirs(basepath)

    credentials = ee.ServiceAccountCredentials(
        email,
        keypath,
    )
    ee.Initialize(credentials)


    numtile = "_".join(map(str,el))
    filename = f"{basepath}/tile_{numtile}.tif"

    if not os.path.exists(filename):
        region = ee.Geometry.BBox(el[0], el[1], el[2], el[3])

        bands = ["avg_rad"]

        mycollection = ee.ImageCollection("NOAA/VIIRS/DNB/MONTHLY_V1/VCMCFG")
        SCALE = mycollection.first().projection().nominalScale().getInfo()

        image = mycollection.filterDate(startDate, endDate)

        # in case we want to filter by images with confidence
        def filterConfidence(image):
            mask = image.select('cf_cvg').gt(1)
            return image.updateMask(mask)
        image = image.map(filterConfidence)


        image = image.mean().multiply(10000).uint16()


        response = get_image_url(image, bands, SCALE, region)

        with open(filename, 'wb') as fd:
            fd.write(response.content)

def downlaodtiles(basepath,email,keypath):

    list_of_bbox=[]
    lon_steps = 5
    lat_steps = 5
    start_date = "2012-01-01"
    end_date = "2024-01-01"
    dates = pd.date_range(start_date, end_date, freq='Q')
    dates_plus=[]
    for date in dates:
        dates_plus.append(date+ timedelta(days=1))

    for i in range(0,len(dates_plus)):
        startDate = dates_plus[i].strftime('%Y-%m-%d')
        endDate = dates_plus[i+1].strftime('%Y-%m-%d')
        quarter = f"Q{dates_plus[i].quarter}"
        year = dates_plus[i].year
        for lon in range(-185,180,lon_steps):
            for lat in range(-75,85,lat_steps):
                list_of_bbox.append([lon, lat, lon+lon_steps, lat+lat_steps, startDate, endDate,year, quarter,basepath,email,keypath])

    with Pool(20) as p:
        p.map(process, list_of_bbox)

if __name__ == "__main__":
    basepath = "DATA/NIGHTLIGHT/NOAA_VIIRS_DNB_MONTHLY_V1_VCMCFG/"
    email = "test1-landsat8@geelandsat8.iam.gserviceaccount.com",
    keypath = "C:\\Users\\email\\Documents\\conflictproxyindicators\\Z_experiments\\2023_MU_Sierra_Leone_civil_conflcit\\geelandsat8-6af86334d7ec.json",

    #Here I am downloading the data by tile to overpass the GEE limits. tiles of 5x5 degreee
    downlaodtiles(basepath,email,keypath)

    # once the tiles are downloaded I build a virutal ratster:
    #import subprocess
    #result = subprocess.run(['gdalbuildvrt','-vrtnodata 0', 'D:/DATA/NIGHTLIGHT/NOAA_VIIRS_DNB_MONTHLY_V1_VCMCFG/tiles/*.tif', 'D:/DATA/NIGHTLIGHT/NOAA_VIIRS_DNB_MONTHLY_V1_VCMCFG/tiles/index.vrt'])

    # Because the input has 500m resoultion and we want to match the worldpop, it is required to warp
    #in_path = "D:/DATA/NIGHTLIGHT/NOAA_VIIRS_DNB_MONTHLY_V1_VCMCFG/tiles/index.vrt"
    #out_path = "D:/DATA/NIGHTLIGHT/NOAA_VIIRS_DNB_MONTHLY_V1_VCMCFG/tiles/2012-04-01_2023-09-01_NOAA-VIIRS-DNB-MONTHLY_V1-VCMCFG.tif"
    #match_file = "D:/DATA/NIGHTLIGHT/NOAA_VIIRS_DNB_MONTHLY_V1_VCMCFG/ppp_2020_1km_Aggregated.tif"
    #warp_and_match_resolution(in_path, out_path, match_file)

    # to minmax
    #out_path_minmax = "D:/DATA/NIGHTLIGHT/NOAA_VIIRS_DNB_MONTHLY_V1_VCMCFG/tiles/2012-04-01_2023-09-01_NOAA-VIIRS-DNB-MONTHLY_V1-VCMCFG_minmax.tif"
    #source_image_metadata = gdal.Open(out_path, gdal.GA_ReadOnly)
    #geo_t = source_image_metadata.GetGeoTransform()
    #wkt = source_image_metadata.GetProjection()
    #raster_array_source = source_image_metadata.ReadAsArray()
    #raster_array_source = minmax_scale(raster_array_source.flatten()).reshape(raster_array_source.shape)
    #writeGeoTiff_v3(raster_array_source,geo_t,out_path_minmax,gdal.GDT_Float32,wkt,0)


