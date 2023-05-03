FROM tensorflow/tensorflow:2.12.0-gpu-jupyter
#FROM python:3.8.10-slim-buster
RUN apt update
RUN apt-get install nano -y

RUN apt-get install software-properties-common -y
RUN apt-get update -y
RUN apt-get install gdal-bin -y
RUN apt-get install libgdal-dev -y
RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal
RUN export C_INCLUDE_PATH=/usr/include/gdal
RUN apt install python3-pip -y
RUN pip install setuptools==57.5.0
RUN pip install pandas==1.3.4
RUN pip install geopandas
RUN pip install earthengine-api
RUN pip install pyarrow
RUN pip install matplotlib
RUN pip install geojson
RUN python -m pip install --upgrade pip
RUN pip install gdal
RUN pip install rasterio

ENV PYTHONPATH "${PYTHONPATH}:/demogee"

WORKDIR "/demogee"

COPY run_jupiter.sh run_jupiter.sh





