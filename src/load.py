import os
import geopandas as gpd
from elasticsearch import Elasticsearch, helpers
import json


def createIndex(elasticHost, indexName, replicas):
    es = Elasticsearch([elasticHost])
    index = None

    try:
        index = es.indices.get(index=indexName)
    except Exception:
        index = es.indices.create(index=indexName, mappings={
            "properties": {
                "GRD_FIXID": {
                    "type": "keyword"
                },
                "GRD_FLOAID": {
                    "type": "keyword"
                },
                "GRD_INSPIR": {
                    "type": "keyword"
                },
                "MUNICIPIO": {
                    "type": "text"
                },
                "CMUN": {
                    "type": "keyword"
                },
                "POB_TOT": {
                    "type": "integer"
                },
                "POB_M": {
                    "type": "integer"
                },
                "POB_H": {
                    "type": "integer"
                },
                "EDAD0015": {
                    "type": "integer"
                },
                "EDAD1664": {
                    "type": "integer"
                },
                "EDAD65_": {
                    "type": "integer"
                },
                "ESP": {
                    "type": "integer"
                },
                "UE15": {
                    "type": "integer"
                },
                "MAG": {
                    "type": "integer"
                },
                "AMS": {
                    "type": "integer"
                },
                "OTR": {
                    "type": "integer"
                },
                "MUNI": {
                    "type": "integer"
                },
                "MUND": {
                    "type": "integer"
                },
                "PROVD": {
                    "type": "integer"
                },
                "CCAAD": {
                    "type": "integer"
                },
                "PAISD": {
                    "type": "integer"
                },
                "TR_05": {
                    "type": "integer"
                },
                "TR0610": {
                    "type": "integer"
                },
                "TR1115": {
                    "type": "integer"
                },
                "TR16_": {
                    "type": "integer"
                },
                "AFIL_SS": {
                    "type": "integer"
                },
                "AFIL_SS_M": {
                    "type": "integer"
                },
                "AFIL_SS_H": {
                    "type": "integer"
                },
                "AFIL_SS_A": {
                    "type": "integer"
                },
                "AFIL_SS_P": {
                    "type": "integer"
                },
                "PENC": {
                    "type": "integer"
                },
                "PENC_M": {
                    "type": "integer"
                },
                "PENC_H": {
                    "type": "integer"
                },
                "PENCJUB": {
                    "type": "integer"
                },
                "PENCVIU": {
                    "type": "integer"
                },
                "PENCJYV": {
                    "type": "integer"
                },
                "PENCINC": {
                    "type": "integer"
                },
                "PENCOTR": {
                    "type": "integer"
                },
                "PENCJUB_MI": {
                    "type": "float"
                },
                "PENCVIU_MI": {
                    "type": "float"
                },
                "PENCJYV_MI": {
                    "type": "float"
                },
                "PENCINC_MI": {
                    "type": "float"
                },
                "PENCOTR_MI": {
                    "type": "float"
                },
                "DEMP": {
                    "type": "integer"
                },
                "DEMP_M": {
                    "type": "integer"
                },
                "DEMP_H": {
                    "type": "integer"
                },
                "DEMP_PR": {
                    "type": "integer"
                },
                "DEMP_PR_M": {
                    "type": "integer"
                },
                "DEMP_PR_H": {
                    "type": "integer"
                },
                "polygon": {
                    "type": "geo_shape"
                }
            }
        })

        es.indices.put_settings(index=indexName, body={
                                "number_of_replicas": replicas})


def shp2Geojson(df):
    return df.to_json()


def createDoc(indexName, geojson):
    _GRD_FIXID = None
    _GRD_FLOAID = None
    _GRD_INSPIR = None
    _MUNICIPIO = None
    _CMUN = None
    _POB_TOT = None
    _POB_M = None
    _POB_H = None
    _EDAD0015 = None
    _EDAD1664 = None
    _EDAD65_ = None
    _ESP = None
    _UE15 = None
    _MAG = None
    _AMS = None
    _OTR = None
    _MUNI = None
    _MUND = None
    _PROVD = None
    _CCAAD = None
    _PAISD = None
    _TR_05 = None
    _TR0610 = None
    _TR1115 = None
    _TR16_ = None
    _AFIL_SS = None
    _AFIL_SS_M = None
    _AFIL_SS_H = None
    _AFIL_SS_A = None
    _AFIL_SS_P = None
    _PENC = None
    _PENC_M = None
    _PENC_H = None
    _PENCJUB = None
    _PENCVIU = None
    _PENCJYV = None
    _PENCINC = None
    _PENCOTR = None
    _PENCJUB_MI = None
    _PENCVIU_MI = None
    _PENCJYV_MI = None
    _PENCINC_MI = None
    _PENCOTR_MI = None
    _DEMP = None
    _DEMP_M = None
    _DEMP_H = None
    _DEMP_PR = None
    _DEMP_PR_M = None
    _DEMP_PR_H = None
    for feature in json.loads(geojson)["features"]:
        if "GRD_FIXID" in feature["properties"]:
            _GRD_FIXID = feature["properties"]["GRD_FIXID"]
        if "GRD_FLOAID" in feature["properties"]:
            _GRD_FLOAID = feature["properties"]["GRD_FLOAID"]
        if "GRD_INSPIR" in feature["properties"]:
            _GRD_INSPIR = feature["properties"]["GRD_INSPIR"]
        if "MUNICIPIO" in feature["properties"]:
            _MUNICIPIO = feature["properties"]["MUNICIPIO"].strip().split(" / ")
            print(_MUNICIPIO)
        if "CMUN" in feature["properties"]:
            _CMUN = feature["properties"]["CMUN"].strip().split(" / ")
            print(_CMUN)
        if "POB_TOT" in feature["properties"]:
            _POB_TOT = feature["properties"]["POB_TOT"]
        if "POB_M" in feature["properties"]:
            _POB_M = feature["properties"]["POB_M"]
        if "POB_H" in feature["properties"]:
            _POB_H = feature["properties"]["POB_H"]
        if "EDAD0015" in feature["properties"]:
            _EDAD0015 = feature["properties"]["EDAD0015"]
        if "EDAD1664" in feature["properties"]:
            _EDAD1664 = feature["properties"]["EDAD1664"]
        if "EDAD65_" in feature["properties"]:
            _EDAD65_ = feature["properties"]["EDAD65_"]
        if "ESP" in feature["properties"]:
            _ESP = feature["properties"]["ESP"]
        if "UE15" in feature["properties"]:
            _UE15 = feature["properties"]["UE15"]
        if "MAG" in feature["properties"]:
            _MAG = feature["properties"]["MAG"]
        if "AMS" in feature["properties"]:
            _AMS = feature["properties"]["AMS"]
        if "OTR" in feature["properties"]:
            _OTR = feature["properties"]["OTR"]
        if "MUNI" in feature["properties"]:
            _MUNI = feature["properties"]["MUNI"]
        if "MUND" in feature["properties"]:
            _MUND = feature["properties"]["MUND"]
        if "PROVD" in feature["properties"]:
            _PROVD = feature["properties"]["PROVD"]
        if "CCAAD" in feature["properties"]:
            _CCAAD = feature["properties"]["CCAAD"]
        if "PAISD" in feature["properties"]:
            _PAISD = feature["properties"]["PAISD"]
        if "TR_05" in feature["properties"]:
            _TR_05 = feature["properties"]["TR_05"]
        if "TR0610" in feature["properties"]:
            _TR0610 = feature["properties"]["TR0610"]
        if "TR1115" in feature["properties"]:
            _TR1115 = feature["properties"]["TR1115"]
        if "TR16_" in feature["properties"]:
            _TR16_ = feature["properties"]["TR16_"]
        if "AFIL_SS" in feature["properties"]:
            _AFIL_SS = feature["properties"]["AFIL_SS"]
        if "AFIL_SS_M" in feature["properties"]:
            _AFIL_SS_M = feature["properties"]["AFIL_SS_M"]
        if "AFIL_SS_H" in feature["properties"]:
            _AFIL_SS_H = feature["properties"]["AFIL_SS_H"]
        if "AFIL_SS_A" in feature["properties"]:
            _AFIL_SS_A = feature["properties"]["AFIL_SS_A"]
        if "AFIL_SS_P" in feature["properties"]:
            _AFIL_SS_P = feature["properties"]["AFIL_SS_P"]
        if "PENC" in feature["properties"]:
            _PENC = feature["properties"]["PENC"]
        if "PENC_M" in feature["properties"]:
            _PENC_M = feature["properties"]["PENC_M"]
        if "PENC_H" in feature["properties"]:
            _PENC_H = feature["properties"]["PENC_H"]
        if "PENCJUB" in feature["properties"]:
            _PENCJUB = feature["properties"]["PENCJUB"]
        if "PENCVIU" in feature["properties"]:
            _PENCVIU = feature["properties"]["PENCVIU"]
        if "PENCJYV" in feature["properties"]:
            _PENCJYV = feature["properties"]["PENCJYV"]
        if "PENCINC" in feature["properties"]:
            _PENCINC = feature["properties"]["PENCINC"]
        if "PENCOTR" in feature["properties"]:
            _PENCOTR = feature["properties"]["PENCOTR"]
        if "PENCJUB_MI" in feature["properties"]:
            _PENCJUB_MI = feature["properties"]["PENCJUB_MI"]
        if "PENCVIU_MI" in feature["properties"]:
            _PENCVIU_MI = feature["properties"]["PENCVIU_MI"]
        if "PENCJYV_MI" in feature["properties"]:
            _PENCJYV_MI = feature["properties"]["PENCJYV_MI"]
        if "PENCINC_MI" in feature["properties"]:
            _PENCINC_MI = feature["properties"]["PENCINC_MI"]
        if "PENCOTR_MI" in feature["properties"]:
            _PENCOTR_MI = feature["properties"]["PENCOTR_MI"]
        if "DEMP" in feature["properties"]:
            _DEMP = feature["properties"]["DEMP"]
        if "DEMP_M" in feature["properties"]:
            _DEMP_M = feature["properties"]["DEMP_M"]
        if "DEMP_H" in feature["properties"]:
            _DEMP_H = feature["properties"]["DEMP_H"]
        if "DEMP_PR" in feature["properties"]:
            _DEMP_PR = feature["properties"]["DEMP_PR"]
        if "DEMP_PR_M" in feature["properties"]:
            _DEMP_PR_M = feature["properties"]["DEMP_PR_M"]
        if "DEMP_PR_H" in feature["properties"]:
            _DEMP_PR_H = feature["properties"]["DEMP_PR_H"]
        objeto = {
            "GRD_FIXID": _GRD_FIXID,
            "GRD_FLOAID": _GRD_FLOAID,
            "GRD_INSPIR": _GRD_INSPIR,
            "MUNICIPIO": _MUNICIPIO,
            "CMUN": _CMUN,
            "POB_TOT": _POB_TOT,
            "POB_M": _POB_M,
            "POB_H": _POB_H,
            "EDAD0015": _EDAD0015,
            "EDAD1664": _EDAD1664,
            "EDAD65_": _EDAD65_,
            "ESP": _ESP,
            "UE15": _UE15,
            "MAG": _MAG,
            "AMS": _AMS,
            "OTR": _OTR,
            "MUNI": _MUNI,
            "MUND": _MUND,
            "PROVD": _PROVD,
            "CCAAD": _CCAAD,
            "PAISD": _PAISD,
            "TR_05": _TR_05,
            "TR0610": _TR0610,
            "TR1115": _TR1115,
            "TR16_": _TR16_,
            "AFIL_SS": _AFIL_SS,
            "AFIL_SS_M": _AFIL_SS_M,
            "AFIL_SS_H": _AFIL_SS_H,
            "AFIL_SS_A": _AFIL_SS_A,
            "AFIL_SS_P": _AFIL_SS_P,
            "PENC": _PENC,
            "PENC_M": _PENC_M,
            "PENC_H": _PENC_H,
            "PENCJUB": _PENCJUB,
            "PENCVIU": _PENCVIU,
            "PENCJYV": _PENCJYV,
            "PENCINC": _PENCINC,
            "PENCOTR": _PENCOTR,
            "PENCJUB_MI": _PENCJUB_MI,
            "PENCVIU_MI": _PENCVIU_MI,
            "PENCJYV_MI": _PENCJYV_MI,
            "PENCINC_MI": _PENCINC_MI,
            "PENCOTR_MI": _PENCOTR_MI,
            "DEMP": _DEMP,
            "DEMP_M": _DEMP_M,
            "DEMP_H": _DEMP_H,
            "DEMP_PR": _DEMP_PR_M,
            "DEMP_PR_M": _DEMP_PR_M,
            "DEMP_PR_H": _DEMP_PR_H,
            "polygon": {
                "type": "polygon",
                "coordinates": feature["geometry"]["coordinates"]
            }
        }

        yield {"_index": indexName, "_id": _GRD_FIXID, "_source": objeto}


def loadData(elasticHost, indexName, geojson):
    es = Elasticsearch(elasticHost, http_compress=True, headers={
                       'content-type': 'application/json'})
    for action in helpers.streaming_bulk(es, createDoc(indexName, geojson)):
        print(action)


def getShapefiles():
    for file in os.listdir("./data"):
        if file.endswith(".shp"):
            print("procesando "+os.path.splitext(file)[0])
            yield {"file": os.path.join("./data", file), "indexName": os.path.splitext(file)[0]}


if __name__ == "__main__":
    elasticHost = "http://localhost:9200"
    # número de replicas de los sharps del indice por defecto 1 se deja a 0 en desarrollo un solo nodo elasticsearch
    replicas = 0
    for shapefile in getShapefiles():
        indexName = shapefile['indexName']
        file = (shapefile['file'])
        df = gpd.read_file(file)
        createIndex(elasticHost, indexName, replicas)
        geojson = shp2Geojson(df.to_crs("EPSG:4326"))
        loadData(elasticHost, indexName, geojson)
        # createDoc(indexName, geojson)
