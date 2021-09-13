#!/usr/bin/env python

from beam_shape import BeamShape, write_contours
import optimizer
from geometry import Target
from optimizer_test_data import time, pool_resources, coordinates, frequency, targets

assert __name__ == "__main__"

shape = BeamShape(frequency, coordinates, pool_resources, time=time)
ellipse = shape.fit_ellipse()
possible_targets = Target.parse_targets(targets)
beams, targets = optimizer.optimize_ellipses(possible_targets=possible_targets, ellipse=ellipse)
optimizer.write_csvs(beams, targets)