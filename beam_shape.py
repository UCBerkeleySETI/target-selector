#!/usr/bin/env python
"""
Code for figuring out the precise shape of the region the telescope can observe.
"""

import csv
import datetime
from geometry import distance, Ellipse, LinearTransform
import io
import math
import numpy as np
import os
import re
import scipy.constants as con

from skimage import measure
from mk_target_selector.redis_tools import get_redis_key, connect_to_redis


class BeamShape(object):
    """
    BeamShape is used to determine the shape of a Meerkat beam.
    """

    def __init__(self, freq, coords, pool, time=None):
        """
        freq can be provided as string or number
        coords is a comma separate string of ra,dec in degrees
        pool is a comma-separated list of a bunch of stuff that looks like "m038" or "cbf_1"
        """
        self.freq = float(freq)
        self.coords = coords
        self.antennas = ",".join(re.findall(r"m\d{3}", pool)).replace("m", "")

        self.ra_deg = float(coords.split(", ")[0])
        self.dec_deg = float(coords.split(", ")[1])

        if time is None:
            self.time = datetime.datetime.now()
        else:
            self.time = time

    def create_image(self):
        # reference coordinates for MeerKAT (latitude, longitude, altitude?)
        refAnt = (-30.71106, 21.44389, 1035)
        wavelength = con.c / self.freq
        J2000RefTime = datetime.datetime(
            2000, 1, 1, 11, 58, 56, tzinfo=datetime.timezone.utc
        )

        # ASDF
        gridNum = 100000 * 2
        imsize = 200

        # list of numbers of antennas currently in use (i.e. 001, 002, 003,...)
        antlist = [int(a) for a in self.antennas.split(",")]
        # get antenna metadata from antenna.csv table
        ants = np.genfromtxt(
            "antenna.csv",
            delimiter=",",
            dtype=None,
            names=["name", "", "", "", "", "ENU", "", "", ""],
            encoding="ascii",
        )

        # ENU = East North Up? ASDF
        ENUoffsets = []
        # create table with ENU offsets for each antenna
        for a in antlist:
            ENUoffsets.append(
                np.array(
                    [
                        float(ants["ENU"][a].split(" ")[2]),
                        float(ants["ENU"][a].split(" ")[3]),
                        float(ants["ENU"][a].split(" ")[4]),
                    ]
                )
            )

        """
        Get gains for baseline weights
        """
        weights = np.zeros(64)
        weights[:] = 1.0  # equal weight for all antennas

        """
        Create baselines
        """
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
                Baselines[i][j] = (
                    ENUoffsets[i] - ENUoffsets[j]
                )  # get baselines from ENU offset,
                BaselineList.append(Baselines[i][j])  # add to array
            index += 1

        """
        Rotate and project baselines
        """

        # reference coordinates for MeerKAT
        refLat = np.deg2rad(refAnt[0])
        refLon = refAnt[1]

        # observation time metadata
        TimeOffset = self.time - J2000RefTime
        TimeOffset = (
            TimeOffset.days
            + TimeOffset.seconds / (60.0 * 60.0 * 24.0)
            + TimeOffset.microseconds / (1000000.0 * 60.0 * 60.0 * 24.0)
        )
        ObsTime = (
            self.time.hour
            + self.time.minute / 60.0
            + self.time.second / (60.0 * 60.0)
            + self.time.microsecond / (1000000.0 * 60.0 * 60.0)
        )

        # Local Sidereal Time
        LST = 100.46 + 0.985647 * TimeOffset + refLon + 15 * ObsTime
        LST = LST % 360.0

        # current observation primary beam pointing coordinates
        DEC = np.deg2rad(self.dec_deg)
        # hour angle
        HA = np.deg2rad(LST) - np.deg2rad(self.ra_deg)

        RotatedProjectedBaselines = []

        for b in BaselineList:
            epsilon = 0.000000000001
            length = np.sqrt(b[0] ** 2 + b[1] ** 2 + b[2] ** 2)

            # azimuth and elevation
            azim = np.arctan2(b[0], (b[1] + epsilon))
            el = np.arcsin(b[2] / (length + epsilon))

            # rotation matrix
            Rot = np.array(
                [
                    np.cos(refLat) * np.sin(el)
                    - np.sin(refLat) * np.cos(el) * np.cos(azim),
                    np.cos(el) * np.sin(azim),
                    np.sin(refLat) * np.sin(el)
                    + np.cos(refLat) * np.cos(el) * np.cos(azim),
                ]
            )

            # projection matrix
            Proj = np.array(
                [
                    [np.sin(HA), np.cos(HA), 0],
                    [-np.sin(DEC) * np.cos(HA), np.sin(DEC) * np.sin(HA), np.cos(DEC)],
                    [np.cos(DEC) * np.cos(HA), -np.cos(DEC) * np.sin(HA), np.sin(DEC)],
                ]
            )

            # dot product of rotation and projection matrices
            RotatedProjectedBaselines.append(np.dot(length * Rot.T, Proj.T))

        """
        UV samples
        """
        imLength = gridNum / 3600  # deg
        step = np.deg2rad(imLength)
        uvSamples = []
        for b in RotatedProjectedBaselines:
            u = int(round(b[0] / wavelength / step + (gridNum / 2 - 1)))
            v = int(round(b[1] / wavelength / step + (gridNum / 2 - 1)))
            uvSamples.append((u, v))

        """
        DFT grid
        """

        halfLength = imsize / 2
        interval = 1

        ul = np.mgrid[0:halfLength:interval, 0:halfLength:interval]
        ur = np.mgrid[0:halfLength:interval, gridNum - halfLength : gridNum : interval]
        bl = np.mgrid[gridNum - halfLength : gridNum : interval, 0:halfLength:interval]
        br = np.mgrid[
            gridNum - halfLength : gridNum : interval,
            gridNum - halfLength : gridNum : interval,
        ]

        imagesCoord = np.array(
            [
                np.concatenate(
                    (
                        np.concatenate((ul[0].T, ur[0].T)).T,
                        np.concatenate((bl[0].T, br[0].T)).T,
                    )
                ).flatten(),
                np.concatenate(
                    (
                        np.concatenate((ul[1].T, ur[1].T)).T,
                        np.concatenate((bl[1].T, br[1].T)).T,
                    )
                ).flatten(),
            ]
        )

        """
        DFT
        """

        index = 1
        WeightingList = []
        for i in range(0, 64):
            for j in range(index, 64):
                WeightingList.append(weights[i] * weights[j])
            index += 1
        WeightingList /= np.amax(WeightingList)

        fringeSum = 0

        # print(f"len(RotatedProjectedBaselines) = {len(RotatedProjectedBaselines)}")
        # print(f"len(uvSamples) = {len(uvSamples)}")

        for p in range(0, len(uvSamples)):
            U = imagesCoord[1] * uvSamples[p][0]
            V = imagesCoord[0] * uvSamples[p][1]
            weight = WeightingList[p]
            fringeSum += weight * np.exp(1j * 2 * con.pi * (U + V) / gridNum)

        fringeSum = fringeSum.reshape(imsize, imsize) / len(uvSamples)
        fringeSum = np.abs(fringeSum)

        image = np.fft.fftshift(fringeSum)
        image = np.fliplr(image)
        image /= np.amax(image)

        return image

    def find_contours(self, image):
        """
        Each contour is a list of (ra_deg, dec_deg) tuples.
        We return a list of contours.
        """
        pixel_contours = measure.find_contours(image, 0.5)
        self.contours = []
        for pixel_contour in pixel_contours:
            output_contour = []
            for x, y in pixel_contour:
                # pixel_delta = imsize / 2
                pixel_delta = 50
                ra_deg = (y - pixel_delta) / 3600 + self.ra_deg
                dec_deg = (x - pixel_delta) / 3600 + self.dec_deg
                output_contour.append((ra_deg, dec_deg))
            self.contours.append(output_contour)
        return self.contours

    def fit_ellipse(self):
        """
        Returns an Ellipse fit as closely as possible to the 0.5 contour.
        """
        image = self.create_image()
        contours = self.find_contours(image)

        # We assume that the longest contour is the best one to fit an ellipse to.
        longest = max(contours, key=len)

        ellipse = Ellipse.fit_with_center(self.ra_deg, self.dec_deg, longest)

        return ellipse


def get_test_beam_shape():
    return BeamShape(
        1500000000,
        "15.66200000000003, -28.836777777777776",
        (
            "bluse_1,cbf_1,fbfuse_1,m000,m001,m002,m003,m004,m005,"
            "m006,m007,m008,m009,m010,m011,m015,m017,m018,m019,m020,"
            "m021,m023,m024,m025,m026,m027,m028,m029,m030,m031,m032,"
            "m033,m034,m035,m036,m037,m038,m039,m040,m041,m042,m043,"
            "m044,m045,m046,m048,m049,m050,m051,m052,m053,m056,m057,"
            "m058,m059,m060,m061,m063,ptuse_4,sdp_1,tuse_"
        ),
        time=datetime.datetime(2021, 1, 1, 12, 00, 00, 0, tzinfo=datetime.timezone.utc),
    )


def get_redis_beam_shape():
    product_id = "array_1"
    freq = float(
        get_redis_key(connect_to_redis(), "{}:current_obs:frequency".format(product_id))
    )
    coords = get_redis_key(
        connect_to_redis(), "{}:current_obs:coords".format(product_id)
    )
    pool = get_redis_key(
        connect_to_redis(), "{}:current_obs:pool_resources".format(product_id)
    )
    return BeamShape(freq, coords, pool)


def write_contours(contours, f):
    for contour in contours:
        for point in contour:
            writer = csv.writer(f)
            writer.writerows([point])


def assert_near(x, y):
    assert abs(x - y) < 0.001


def test_against_golden_output():
    shape = get_test_beam_shape()
    ellipse = shape.fit_ellipse()

    # Test some ellipse utils
    ra, dec = ellipse.max_ra_point()
    assert_near(ellipse.evaluate(ra, dec), 1)
    ra, dec = ellipse.max_dec_point()
    assert_near(ellipse.evaluate(ra, dec), 1)
    ra = ellipse.horizontal_ray_intersection()
    assert_near(ellipse.evaluate(ra, ellipse.dec), 1)

    # Test the to_unit_circle transformation
    longest = max(shape.contours, key=len)
    t = LinearTransform.to_unit_circle(ellipse)
    center = t.transform_point(ellipse.ra, ellipse.dec)

    buf = io.StringIO()
    write_contours(shape.contours, buf)

    golden = (
        open(os.path.join(os.path.dirname(__file__), "test", "contour_vertices.csv"))
        .read()
        .split()
    )

    output = buf.getvalue().strip().split()

    for golden_line, output_line in zip(golden, output):
        if golden_line != output_line:
            print("golden:", golden_line)
            print("output:", output_line)


if __name__ == "__main__":
    test_against_golden_output()