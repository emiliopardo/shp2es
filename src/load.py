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
                    "type": "keyword"
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

def getFields(geojson):
    attributes = []
    myModel = json.loads(geojson)["features"][0]["properties"]
    for key in myModel.keys():
        attributes.append(key)
    return attributes


def createDoc(indexName, model ,geojson):
    for feature in json.loads(geojson)["features"]:
        myJson = "{"
        for attr in model: 
            # logica para gestionar array de  CMUN y MUNICIPIO  
            if attr=="MUNICIPIO" or attr=="CMUN":
                str1 = '","'.join(feature["properties"][attr].strip().split(" / "))
                str2 ="[\""+str1+"\"]"
                newAtrr = "\""+attr+"\":"+str2+","
                myJson = myJson+newAtrr
            # lógica para controlar string
            elif attr=="GRD_FIXID" or attr=="GRD_FLOAID" or attr=="GRD_INSPIR" or attr=="NOTA":
                newAtrr = "\""+attr+"\":\""+str(feature["properties"][attr])+"\","
                myJson = myJson+newAtrr
            # todo lo demas es numérico
            else:    
                newAtrr = "\""+attr+"\":"+str(feature["properties"][attr])+","
                myJson = myJson+newAtrr
        myJson = myJson + '"polygon": {"type": "polygon","coordinates":'
        myJson = myJson +json.dumps(feature["geometry"]["coordinates"])+"}"
        myJson = myJson +"}"
        yield {"_index": indexName, "_id": feature["properties"]["GRD_FIXID"], "_source": myJson}

    
def loadData(elasticHost, indexName, model,geojson):
    es = Elasticsearch(elasticHost, http_compress=True, headers={
                       'content-type': 'application/json'})
    for action in helpers.streaming_bulk(es, createDoc(indexName, model,geojson)):
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
        model = getFields(geojson)
        loadData(elasticHost, indexName, model,geojson)
       

    # indexName = "mep02_250"
    # df = gpd.read_file("./data/mep02_250.shp")
    # createIndex(elasticHost, indexName, replicas)
    # geojson = shp2Geojson(df.to_crs("EPSG:4326"))
    # model = getFields(geojson)
    # loadData(elasticHost, indexName, model ,geojson)
