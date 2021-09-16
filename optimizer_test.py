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

# print(
#     "Beam shape: A = {:.3f}, B = {:.3f}, C = {:.3f}".format(
#         ellipse.a, ellipse.b, ellipse.c
#     )
# )

pointing_ra, pointing_dec = map(float, coordinates.split(", "))
possible_targets = Target.parse_targets(targets, pointing_ra, pointing_dec, frequency)

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

# Calculate the average attenuation of targets locally within the coherent beams
attenuations_local = []
for beam in beams:
    attenuations_local.extend(
        ellipse.centered_at(beam.ra, beam.dec).attenuations(beam.targets)
    )
print("Average local attenuation:", sum(attenuations_local) / len(attenuations_local))
print("Minimum local attenuation:", min(attenuations_local))

# Calculate the average attenuation of coherent beams within the primary beam
attenuations_primary = []
for target in targets:
    attenuations_primary.append(
        target.power_multiplier
    )
print("Average primary attenuation:", sum(attenuations_primary) / len(attenuations_primary))
print("Minimum primary attenuation:", min(attenuations_primary))


# Validate that the beam ellipses contain their targets
for beam in beams:
    e = ellipse.centered_at(beam.ra, beam.dec)
    for target in beam.targets:
        assert e.contains(target.ra, target.dec)

optimizer.write_csvs(beams, targets)

# plot the outputted CSVs for sanity checking
test_plot()
