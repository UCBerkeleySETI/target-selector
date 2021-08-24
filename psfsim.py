#!/usr/bin/env python
import numpy as np
import csv
import re
import scipy.constants as con
import datetime
from skimage import measure
from mk_target_selector.redis_tools import (get_redis_key,
                                            connect_to_redis)

# subarray ID
product_id = "array_1"
# metadata for current observation
current_freq = float(get_redis_key(connect_to_redis(), "{}:current_obs:frequency".format(product_id)))
current_coords = get_redis_key(connect_to_redis(), "{}:current_obs:coords".format(product_id))
current_pool = get_redis_key(connect_to_redis(), "{}:current_obs:pool_resources".format(product_id))
current_antennas = ','.join(re.findall(r'm\d{3}', current_pool)).replace("m", "")
current_time = datetime.datetime.now()

# metadata for testing purposes
# product_id = "array_1"
# current_freq = "1500000000"
# current_coords = "15.66200000000003, -28.836777777777776"
# current_pool = "bluse_1,cbf_1,fbfuse_1,m000,m001,m002,m003,m004,m005," \
#                "m006,m007,m008,m009,m010,m011,m015,m017,m018,m019,m020," \
#                "m021,m023,m024,m025,m026,m027,m028,m029,m030,m031,m032," \
#                "m033,m034,m035,m036,m037,m038,m039,m040,m041,m042,m043," \
#                "m044,m045,m046,m048,m049,m050,m051,m052,m053,m056,m057," \
#                "m058,m059,m060,m061,m063,ptuse_4,sdp_1,tuse_"
# current_antennas = ','.join(re.findall(r'm\d{3}', current_pool)).replace("m", "")
# current_time = datetime.datetime.now()

# reference coordinates for MeerKAT (latitude, longitude, altitude?)
refAnt = (-30.71106, 21.44389, 1035)
wavelength = con.c / current_freq
J2000RefTime = datetime.datetime(2000, 1, 1, 11, 58, 56)  # UTC

# ASDF
gridNum = 100000*2
imsize = 200

# list of numbers of antennas currently in use (i.e. 001, 002, 003,...)
antlist = [int(a) for a in current_antennas.split(',')]
# get antenna metadata from antenna.csv table
ants = np.genfromtxt('antenna.csv',
                     delimiter=',',
                     dtype=None,
                     names=["name", "", "", "", "", "ENU", "", "", ""],
                     encoding="ascii")

# ENU = East North Up? ASDF
ENUoffsets = []
# create table with ENU offsets for each antenna
for a in antlist:
    ENUoffsets.append(np.array([float(ants["ENU"][a].split(' ')[2]),
                                float(ants["ENU"][a].split(' ')[3]),
                                float(ants["ENU"][a].split(' ')[4])]))

'''
Get gains for baseline weights
'''
weights = np.zeros(64,)
weights[:] = 1.  # equal weight for all antennas

'''
Create baselines
'''
# initialise array of arrays for baselines for each antenna
Baselines = []
for i in range(0, len(antlist)):
    row = []
    for j in range(0, len(antlist)):
        row.append([])
    Baselines.append(row)

BaselineList = []
index = 1
for i in range(0, len(antlist)):
    for j in range(index, len(antlist)):  # for each antenna,
        Baselines[i][j] = ENUoffsets[i]-ENUoffsets[j]  # get baselines from ENU offset,
        BaselineList.append(Baselines[i][j])  # add to array
    index += 1

'''
Rotate and project baselines
'''

# reference coordinates for MeerKAT
refLat = np.deg2rad(refAnt[0])
refLon = refAnt[1]

# observation time metadata
TimeOffset = current_time - J2000RefTime
TimeOffset = TimeOffset.days + TimeOffset.seconds/(60.*60.*24.) + TimeOffset.microseconds/(1000000.*60.*60.*24.)
ObsTime = current_time.hour + current_time.minute/60. + current_time.second/(60.*60.) + current_time.microsecond/(1000000.*60.*60.)

# Local Sidereal Time
LST = 100.46+0.985647*TimeOffset + refLon + 15*ObsTime
LST = LST % 360.

# current observation primary beam pointing coordinates
ra_deg = float(current_coords.split(", ")[0])
dec_deg = float(current_coords.split(", ")[1])
DEC = np.deg2rad(dec_deg)
# hour angle
HA = np.deg2rad(LST) - np.deg2rad(ra_deg)

RotatedProjectedBaselines = []

for b in BaselineList:
    epsilon = 0.000000000001
    length = np.sqrt(b[0]**2 + b[1]**2 + b[2]**2)

    # azimuth and elevation
    azim = np.arctan2(b[0], (b[1]+epsilon))
    el = np.arcsin(b[2] / (length+epsilon))
    
    # rotation matrix
    Rot = np.array([np.cos(refLat)*np.sin(el) - np.sin(refLat) * np.cos(el) * np.cos(azim),
                    np.cos(el)*np.sin(azim),
                    np.sin(refLat) * np.sin(el) + np.cos(refLat)*np.cos(el)*np.cos(azim)])

    # projection matrix
    Proj = np.array([
        [np.sin(HA), np.cos(HA), 0],
        [-np.sin(DEC)*np.cos(HA), np.sin(DEC)*np.sin(HA), np.cos(DEC)],
        [np.cos(DEC)*np.cos(HA), -np.cos(DEC)*np.sin(HA), np.sin(DEC)]])

    # dot product of rotation and projection matrices
    RotatedProjectedBaselines.append(np.dot(length*Rot.T, Proj.T))

'''
UV samples
'''
imLength = gridNum/3600  # deg
step = np.deg2rad(imLength)
uvSamples = []
for b in RotatedProjectedBaselines:
    u = int(round(b[0]/wavelength/step + (gridNum/2 - 1)))
    v = int(round(b[1]/wavelength/step + (gridNum/2 - 1)))
    uvSamples.append((u, v))

'''
DFT grid
'''

halfLength = imsize/2
interval = 1

ul = np.mgrid[0:halfLength:interval, 0:halfLength:interval]
ur = np.mgrid[0:halfLength:interval, gridNum-halfLength:gridNum:interval]
bl = np.mgrid[gridNum-halfLength:gridNum:interval, 0:halfLength:interval]
br = np.mgrid[gridNum-halfLength:gridNum:interval, gridNum-halfLength:gridNum:interval]
        
imagesCoord = np.array([np.concatenate((np.concatenate((ul[0].T, ur[0].T)).T,
                                        np.concatenate((bl[0].T, br[0].T)).T)).flatten(),
                        np.concatenate((np.concatenate((ul[1].T, ur[1].T)).T,
                                        np.concatenate((bl[1].T, br[1].T)).T)).flatten()])

'''
DFT
'''

index = 1
WeightingList = []
for i in range(0, 64):
    for j in range(index, 64):
        WeightingList.append(weights[i]*weights[j])
    index += 1
WeightingList /= np.amax(WeightingList)

fringeSum = 0
    
for p in range(0, len(uvSamples)):
    U = imagesCoord[1] * uvSamples[p][0]
    V = imagesCoord[0] * uvSamples[p][1]
    weight = WeightingList[p]
    fringeSum += weight*np.exp(1j*2*con.pi*(U + V)/gridNum)

fringeSum = fringeSum.reshape(imsize, imsize)/len(uvSamples)
fringeSum = np.abs(fringeSum)

image = np.fft.fftshift(fringeSum)
image = np.fliplr(image)
image /= np.amax(image)

contours = measure.find_contours(image, 0.5)
contour_curve = []
curve_ra = []
curve_dec = []
with open("contour_vertices.csv", "w", newline="") as f:
    for contour in contours:
        for n in range(0, len(contour)):
            # convert from pixels to coordinates; 1 pixel = 1 arc second
            contour_ra_deg = ((contour[n][1] - 250) / 3600) + ra_deg
            contour_dec_deg = ((contour[n][0] - 250) / 3600) + dec_deg
            contour_deg = [(contour_ra_deg, contour_dec_deg)]
            contour_curve.append(contour_deg)
            curve_ra.append(contour_ra_deg)
            curve_dec.append(contour_dec_deg)
            writer = csv.writer(f)
            writer.writerows(contour_deg)
