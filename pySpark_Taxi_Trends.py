from pyspark import SparkContext
import fiona
import fiona.crs
import shapely
import rtree

import pandas as pd
import geopandas as gp
import heapq
import sys

def createIndex(shapefile):
    import rtree
    import fiona.crs
    import pyproj
    import geopandas as gpd
    zones = gpd.read_file(shapefile).to_crs(fiona.crs.from_epsg(2263))
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    index = rtree.Rtree()
    for idx,geometry in enumerate(zones.geometry):
        index.insert(idx, geometry.bounds)
    return (index, zones)
def findZone(p, index, zones):
    match = index.intersection((p.x, p.y, p.x, p.y))
    for idx in match:
        if zones.geometry[idx].contains(p):
            return idx
    return None

def processTrips(pid, records):
    import csv
    import pyproj
    import shapely.geometry as geom
    proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    
    HOODS = 'neighborhoods.geojson'
    indexN, zonesN = createIndex(HOODS)  
    
    if pid==0:
        next(records)
    reader = csv.reader(records)
    counts = {}
    
    for row in reader:
      
        try:  ## pick-ups
            p = geom.Point(proj(float(row[5]), float(row[6])))
        except:
            continue
        try:  ## drop-offs
            p_a = geom.Point(proj(float(row[9]), float(row[10])))
        except:
            continue
        zoneN = findZone(p, indexN, zonesN)
        
        if zoneN: # and zone_a
            zoneB = findZone(p_a, indexN, zonesN)
            if zoneB is not None:
                zone = (zoneB, zoneN)
                if zone:
                    counts[zone] = counts.get(zone, 0) + 1
    return counts.items()

def main(sc):
    TAXI = sys.argv[1]
    HOODS = 'neighborhoods.geojson'
    neighborhoods = gp.read_file(HOODS)

    rdd = sc.textFile(TAXI)
    counts = rdd.mapPartitionsWithIndex(processTrips)


    countsPerBORO = counts.map(lambda x: ((neighborhoods['borough'][x[0][0]], 
                                                    neighborhoods['neighborhood'][x[0][1]]), 
                                                    x[1]))
    countsPerNeighborhood = countsPerBORO.reduceByKey(lambda x,y: x+y).sortBy(lambda x: -x[1])
    boros = ['Brooklyn', 'Queens', 'Bronx', 'Manhattan', 'Staten Island']

    top3 = {}
    for b in boros:
        top3[b]=[]
        for c in countsPerNeighborhood.filter(lambda x: x[0][0]==b).take(3):
            top3[b].append(c)

    for a in top3:
        print(top3[a],'\n')
            
if __name__ == "__main__":
    sc = SparkContext()
    main(sc)