#!/usr/bin/env python

import shapefile
import sys

sf = shapefile.Reader(sys.argv[1])
shapes = sf.shapes()

def dump_region(shpId, partId, points):
    sys.stdout.write("%d,%s" % (shpId * 100 + partId, len(points)))
    for point in points:
        sys.stdout.write(",%f,%f" % (point[0], point[1]))
    sys.stdout.write("\n")

def dump_shape(shpId, shape):
    partsBegin = shape.parts
    partsEnd = list(partsBegin)
    partsEnd.append(len(shape.points))
    partsEnd = partsEnd[1:]
    for i in range(len(partsBegin)):
        regionPoints = shape.points[partsBegin[i]:partsEnd[i]]
        dump_region(shpId, i, regionPoints)

shpId = 10000000000

for shape in shapes:
    dump_shape(shpId, shape)
    shpId += 1
