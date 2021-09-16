#!/usr/bin/env python

from beam_shape import BeamShape
import optimizer
import csv
import numpy as np
import scipy.constants as con
from geometry import Target
from test_plot import test_plot
from optimizer_test_data import time, pool_resources, coordinates, frequency, targets

assert __name__ == "__main__"

shape = BeamShape(frequency, coordinates, pool_resources, time=time)
ellipse = shape.inscribe_ellipse()
atten = shape.fit_attenuation_function()

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
    possible_targets, ellipse, attenuation=atten
)

# Calculate the average attenuation of local beams
distances = []
for beam in beams:
    distances.extend(
        ellipse.centered_at(beam.ra, beam.dec).fractional_distances(beam.targets)
    )
local_attenuations = [atten(d) for d in distances]

print("Average local attenuation:", sum(local_attenuations) / len(local_attenuations))
print("Minimum local attenuation:", min(local_attenuations))

# Calculate the average attenuation of coherent beams within the primary beam
primary_attenuations = [t.power_multiplier for t in targets]
print(
    "Average primary attenuation:",
    sum(primary_attenuations) / len(primary_attenuations),
)
print("Minimum primary attenuation:", min(primary_attenuations))

# Validate that the beam ellipses contain their targets
for beam in beams:
    e = ellipse.centered_at(beam.ra, beam.dec)
    for target in beam.targets:
        assert e.contains(target.ra, target.dec)

optimizer.write_csvs(beams, targets)

# plot the outputted CSVs for sanity checking
test_plot()
