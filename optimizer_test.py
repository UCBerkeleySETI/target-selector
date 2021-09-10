#!/usr/bin/env python

from beam_shape import BeamShape
import optimizer
from optimizer_test_data import time, pool_resources, coordinates, frequency, targets

assert __name__ == "__main__"

shape = BeamShape(frequency, coordinates, pool_resources, time=time)
ellipse = shape.fit_ellipse()
beams, targets = optimizer.optimize_ellipses(targets, ellipse)
optimizer.write_csvs(beams, targets)
