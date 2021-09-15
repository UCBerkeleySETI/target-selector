#!/usr/bin/env python

from beam_shape import BeamShape, write_contours
import optimizer
import csv
import numpy as np
import scipy.constants as con
from geometry import Target, distance, attenuation
from test_plot import test_plot
from optimizer_test_data import time, pool_resources, coordinates, frequency, targets

assert __name__ == "__main__"

shape = BeamShape(frequency, coordinates, pool_resources, time=time)
ellipse = shape.inscribe_ellipse()

proportional_offset_list = []
power_multiplier_list = []
for index in enumerate(targets["source_id"]):
    pointing_coord = (map(float, coordinates.split(", ")))
    target_ra = targets["ra"][index[0]]
    target_dec = targets["decl"][index[0]]
    target_coord = (target_ra, target_dec)
    radial_offset = distance(target_coord, pointing_coord)
    beam_width = np.rad2deg((con.c / float(frequency)) / 13.5)
    proportional_offset = radial_offset / beam_width
    power_multiplier = attenuation(proportional_offset)
    proportional_offset_list.append(proportional_offset)
    power_multiplier_list.append(power_multiplier)
targets["proportional_offset"] = proportional_offset_list
targets["power_multiplier"] = power_multiplier_list

possible_targets = Target.parse_targets(targets)
# Write target list to csv for checking
with open("sanity_check/fov_total_targets.csv", "w") as f:
    cols = ("ra", "decl")
    writer = csv.writer(f)
    writer.writerow(cols)
    for item in possible_targets:
        coords = (item.ra, item.dec)
        writer.writerow(coords)

beams, targets = optimizer.optimize_ellipses(
    possible_targets=possible_targets, ellipse=ellipse
)

# Validate that the beam ellipses contain their targets
for beam in beams:
    e = ellipse.centered_at(beam.ra, beam.dec)
    for target in beam.targets:
        assert e.evaluate(target.ra, target.dec) <= 1

optimizer.write_csvs(beams, targets)

# plot the outputted CSVs for sanity checking
test_plot()
