# import shapely.geometry
import geopandas as gpd
from elasticsearch import Elasticsearch
import json



df = gpd.read_file('./data/mep20_250_4326.shp')
# print(df)

geojson_str = df.to_json()

# print (geojson_str)

es = Elasticsearch(['http://localhost:9200'])
geoindex = None


try:
    geoindex = es.indices.get(index='geoindex')
except Exception:
    geoindex = es.indices.create('geoindex', {
        "mappings": {
            "properties": {
                "polygon": {
                    "type": "geo_shape",
                    "strategy": "recursive"
                }
            }
        }
    })


# shapely_polygon = shapely.geometry.Polygon([(0, 0), (0, 1), (1, 0)])
# geojson_str = geopandas.GeoSeries([shapely_polygon]).to_json()


# for feature in json.loads(geojson_str)['features']:
#     print(feature)


# for feature in json.loads(geojson_str)['features']:
#     es.index('geoindex', { "polygon": {
#         "type": "polygon",
#         "coordinates": feature['geometry']['coordinates']
#     }}, id=feature['id'])

# count = es.count({}, 'geoindex')
# print(count)