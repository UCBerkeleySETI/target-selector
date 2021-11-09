#!/usr/bin/env python

import csv
import numpy as np
import scipy.constants as con

from beam_shape import BeamShape
from geometry import Target
from optimizer import Optimizer
from optimizer_test_data import time, pool_resources, coordinates, frequency, targets
from test_plot import test_plot
from mk_target_selector.logger import log as logger

assert __name__ == "__main__"

pointing_ra, pointing_dec = map(float, coordinates.split(", "))
possible_targets = Target.parse_targets(targets, pointing_ra, pointing_dec, frequency)

opt = Optimizer(frequency, coordinates, pool_resources, possible_targets, time=time)
opt.optimize()
opt.show_attenuation_stats()
opt.write_csvs()


# plot the outputted CSVs for sanity checking
test_plot()
