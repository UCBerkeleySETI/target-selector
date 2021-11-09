#!/usr/bin/env python
"""
Tools to optimize beam placement.
"""

from beam_shape import BeamShape
import csv
import math
import mip
import numpy as np
import os
import pandas as pd
from scipy.spatial import KDTree
from geometry import Circle, LinearTransform
from mk_target_selector.logger import log as logger

# One target of priority n is worth priority_decay targets of priority n+1.
priority_decay = 10


class Optimizer(object):
    def __init__(
        self,
        frequency,
        coordinates,
        pool,
        possible_targets,
        time=None,
        num_beams=None,
        min_local_attenuation=None,
        min_include_attenuation=None,
    ):
        self.frequency = frequency
        self.coordinates = coordinates
        self.pool = pool
        self.possible_targets = possible_targets
        self.time = time

        # For each of the following config options, we prioritize:
        # 1. A setting the caller explicitly set
        # 2. The value from an environment variable
        # 3. The default
        self.num_beams = num_beams or int(os.getenv("NUM_BEAMS") or 64)
        self.min_local_attenuation = min_local_attenuation or float(
            os.getenv("MIN_LOCAL_ATTENUATION") or 0.5
        )
        self.min_include_attenuation = min_include_attenuation
        if not self.min_include_attenuation:
            env_min_include_attenuation = os.getenv("MIN_INCLUDE_ATTENUATION")
            if env_min_include_attenuation:
                self.min_include_attenuation = float(env_min_include_attenuation)

    def optimize(self):
        """
        Creates an optimized self.beams and self.targets
        """
        self.shape = BeamShape(self.frequency, self.coordinates, self.pool, self.time)
        self.ellipse, self.attenuation = self.shape.fit_attenuation_function(
            self.min_local_attenuation
        )
        # self.main_targets is just the targets that pass min_local_attenuation
        self.beams, self.targets = optimize_ellipses(
            self.possible_targets,
            self.ellipse,
            self.num_beams,
            attenuation=self.attenuation,
        )

        # Validate that the beam ellipses contain their targets
        for beam in self.beams:
            e = self.ellipse.centered_at(beam.ra, beam.dec)
            for target in beam.targets:
                assert e.contains(target.ra, target.dec)

        # Add extra_targets if min_include_attenuation is set
        if self.min_include_attenuation:
            big_ellipse = self.shape.inscribe_ellipse(self.min_include_attenuation)
            t = LinearTransform.to_unit_circle(big_ellipse)
            transformed_targets = np.array(
                [
                    t.transform_point(target.ra, target.dec)
                    for target in self.possible_targets
                ]
            )
            transformed_centers = [
                t.transform_point(beam.ra, beam.dec) for beam in self.beams
            ]

            tree = KDTree(transformed_centers)

            target_index_matrix = tree.query_ball_point(transformed_targets, 1)
            for beam, target_indexes in zip(self.beams, target_index_matrix):
                e = self.ellipse.centered_at(beam.ra, beam.dec)
                beam.extra_targets = []
                for i in target_indexes:
                    target = self.possible_targets[i]
                    if target not in beam.targets:
                        assert not e.contains(target.ra, target.dec)
                        beam.extra_targets.append(target)

    def show_attenuation_stats(self):
        assert self.beams, "optimize first"
        distances = []
        for beam in self.beams:
            distances.extend(
                self.ellipse.centered_at(beam.ra, beam.dec).fractional_distances(
                    beam.targets
                )
            )

        logger.info("Centre attenuation: {}".format(self.attenuation(0)))

        local_attenuations = [self.attenuation(d) for d in distances]

        logger.info("Average local attenuation: {}".format(sum(local_attenuations) / len(local_attenuations)))
        logger.info("Minimum local attenuation: {}".format(min(local_attenuations)))

        # Calculate the average attenuation of coherent beams within the primary beam
        primary_attenuations = [t.power_multiplier for t in self.targets]
        logger.info("Average primary attenuation: {}".format(sum(primary_attenuations) / len(primary_attenuations)))
        logger.info("Minimum primary attenuation: {}".format(min(primary_attenuations)))

        if self.min_include_attenuation:
            logger.info("Including {} extra targets".format(sum(len(beam.extra_targets) for beam in self.beams)))

    def write_csvs(self):
        assert self.beams, "optimize first"
        # beams = write_csvs_helper(self.beams, self.targets)[0]
        # targets = write_csvs_helper(self.beams, self.targets)[1]
        p = write_csvs_helper(self.beams, self.targets)
        return p[0], p[1]


def intersect_two_circles(x0, y0, r0, x1, y1, r1):
    """
    Finding the intersections of two circles.

    Thanks to:
    https://stackoverflow.com/questions/55816902/finding-the-intersection-of-two-circles
    """
    # circle 1: (x0, y0), radius r0
    # circle 2: (x1, y1), radius r1

    d = math.sqrt((x1 - x0) ** 2 + (y1 - y0) ** 2)

    if d > r0 + r1:
        raise ValueError("The circles are non-intersecting")

    if d < abs(r0 - r1):
        raise ValueError("One circle is within the other")

    if d == 0 and r0 == r1:
        raise ValueError("The circles are coincident")

    a = (r0 ** 2 - r1 ** 2 + d ** 2) / (2 * d)
    h = math.sqrt(r0 ** 2 - a ** 2)
    x2 = x0 + a * (x1 - x0) / d
    y2 = y0 + a * (y1 - y0) / d
    x3 = x2 + h * (y1 - y0) / d
    y3 = y2 - h * (x1 - x0) / d

    x4 = x2 - h * (y1 - y0) / d
    y4 = y2 + h * (x1 - x0) / d

    return ((x3, y3), (x4, y4))


def optimize_circles(possible_targets, radius, num_beams, attenuation=None):
    """
    Calculate the best-scoring num_beams beams assuming each beam is a circle with the given radius.
    possible_targets is a list of Target objects.
    attenuation is the attenuation function to use for optimizing the beam centers.

    This returns (circle_list, target_list) containing the Circle and Target objects
    for the optimal beam placement.

    Raises a RuntimeError if it cannot perform the optimization, usually if the data
    is degenerate in some way.
    """
    arr = np.array([[p.ra, p.dec] for p in possible_targets])
    tree = KDTree(arr)

    # Find all pairs of points that could be captured by a single observation
    pairs = tree.query_pairs(2 * radius)
    logger.info(
        "Of {} total remaining targets in the field of view,"
        " {} target pairs can be observed with a single formed beam".format(
            len(possible_targets), len(pairs)
        )
    )

    # A list of (ra, dec) coordinates for the center of possible circles
    candidate_centers = []

    # Add one center for each of the targets that aren't part of any pairs
    in_a_pair = set()
    for i, j in pairs:
        in_a_pair.add(i)
        in_a_pair.add(j)
    for i in range(len(possible_targets)):
        if i not in in_a_pair:
            t = possible_targets[i]
            candidate_centers.append((t.ra, t.dec))

    # Add two centers for each pair of targets that are close to each other
    for i0, i1 in pairs:
        p0 = possible_targets[i0]
        p1 = possible_targets[i1]
        # For each pair, find two points that are a bit less than radius away from each point.
        # These are the possible centers of the circle.
        # TODO: make the mathematical argument of this algorithm's sufficiency clearer
        r = 0.9999 * radius
        try:
            c0, c1 = intersect_two_circles(
                x0=p0.ra, y0=p0.dec, r0=r, x1=p1.ra, y1=p1.dec, r1=r
            )
            candidate_centers.append(c0)
            candidate_centers.append(c1)
        except ValueError:
            continue

    logger.info(
        "Including targets insufficiently close to any others leaves"
        " {} candidates for beamforming coordinates".format(len(candidate_centers))
    )
    candidate_target_indexes = tree.query_ball_point(candidate_centers, radius)

    # Construct Circle objects.
    # Filter out any circles whose included targets are the same as a previous circle
    circles = []
    seen = set()
    for (ra, dec), target_indexes in zip(candidate_centers, candidate_target_indexes):
        targets = [possible_targets[i] for i in target_indexes]
        circle = Circle(ra, dec, radius, targets)
        key = circle.key()
        if key in seen:
            continue
        seen.add(key)
        circles.append(circle)

    logger.info(
        "Removing functional duplicates leaves {} remaining candidates".format(
            len(circles)
        )
    )

    # We want to pick the set of circles that covers the most targets.
    # This is the "maximum coverage problem".
    # https://en.wikipedia.org/wiki/Maximum_coverage_problem
    # We encode this as an integer linear program.
    model = mip.Model(sense=mip.MAXIMIZE)
    model.verbose = 0

    # Variable t{n} is whether the nth target is covered
    target_vars = [
        model.add_var(name="t{n}", var_type=mip.BINARY)
        for n in range(len(possible_targets))
    ]

    # Variable c{n} is whether the nth circle is selected
    circle_vars = [
        model.add_var(name="c{n}", var_type=mip.BINARY) for n in range(len(circles))
    ]

    # Add a constraint that we must select at most num_beams circles
    model += mip.xsum(circle_vars) <= num_beams

    # For each target, if its variable is 1 then at least one of its circles must also be 1
    circles_for_target = {}
    for (circle_index, circle) in enumerate(circles):
        for target in circle.targets:
            if target.index not in circles_for_target:
                circles_for_target[target.index] = []
            circles_for_target[target.index].append(circle_index)
    for target_index, circle_indexes in circles_for_target.items():
        cvars = [circle_vars[i] for i in circle_indexes]
        model += mip.xsum(cvars) >= target_vars[target_index]

    # Maximize the total score for targets we observe
    model.objective = mip.xsum(
        t.score * tvar for (t, tvar) in zip(possible_targets, target_vars)
    )

    # Optimize
    status = model.optimize(max_seconds=30)
    if status == mip.OptimizationStatus.OPTIMAL:
        logger.info("Optimal solution found.")
    elif status == mip.OptimizationStatus.FEASIBLE:
        logger.info("Feasible solution found.")
    else:
        raise RuntimeError("No solution found during integer programming optimization.")

    selected_circles = []
    for circle, circle_var in zip(circles, circle_vars):
        if circle_var.x > 1e-6:
            selected_circles.append(circle)

    selected_targets = []
    for target, target_var in zip(possible_targets, target_vars):
        if target_var.x > 1e-6:
            selected_targets.append(target)

    for c in selected_circles:
        c.recenter(attenuation=attenuation)

    return (selected_circles, selected_targets)


def optimize_ellipses(possible_targets, ellipse, num_beams, attenuation=None):
    """
    possible_targets is a list of Target objects.
    ellipse defines the shape of our beam.
    attenuation is the attenuation function to use for optimizing the beam centers.

    Returns a (beam_list, target_list) tuple containing the Beam and Target objects that
    can be observed with the optimal beam placement.
    """
    # Find a transform that makes the beam look like a circle and transform our data.
    t = LinearTransform.to_unit_circle(ellipse)
    transformed_targets = [t.transform_target(target) for target in possible_targets]

    # Solve the problem in the transformed space
    circles, selected_targets = optimize_circles(
        transformed_targets, 1, num_beams, attenuation=attenuation
    )

    # Un-transform the answer
    inverse = t.invert()
    beams = [inverse.transform_beam(circle) for circle in circles]
    beam_targets = [inverse.transform_target(target) for target in selected_targets]
    return (beams, beam_targets)


def write_csvs_helper(selected_beams, selected_targets):
    """
    Write out some csvs to inspect the solution to an optimizer run.
    """
    logger.info("The solution observes {} unique targets.".format(len(selected_targets)))
    pcount = {}
    for t in selected_targets:
        pcount[t.priority] = pcount.get(t.priority, 0) + 1
    for p, count in sorted(pcount.items()):
        logger.info("{} of the targets have priority {}".format(count, p))
    targets_to_observe = []
    beams_to_observe = []
    for beam in selected_beams:
        target_str = ", ".join(t.source_id for t in beam.targets)
        dist_str = ", ".join(str(t.dist_c) for t in beam.targets)
        priority_str = ", ".join(str(t.priority) for t in beam.targets)
        table_str = ", ".join(t.table_name for t in beam.targets)
        beams_to_observe.append(
            [beam.ra, beam.dec, target_str, priority_str, dist_str, table_str]
        )
        # logger.info("Beam ({}, {}) contains targets {}".format(beam.ra, beam.dec, target_str))
        for t in beam.targets:
            targets_to_observe.append(
                [
                    t.ra,
                    t.dec,
                    beam.ra,
                    beam.dec,
                    t.source_id,
                    t.priority,
                    t.dist_c,
                    t.table_name,
                ]
            )

    print(len(targets_to_observe))

    beam_columns = [
        "ra",
        "decl",
        "source_id",
        "contained_priority",
        "contained_dist_c",
        "contained_table",
    ]
    beams_dict = {
        k: [x[i] for x in beams_to_observe] for i, k in enumerate(beam_columns)
    }

    pd.DataFrame.to_csv(
        pd.DataFrame.from_dict(beams_dict), "sanity_check/beamform_beams.csv"
    )

    # writing beam extent coordinates for checking purposes
    beamform_beams = pd.read_csv("sanity_check/beamform_beams.csv")
    contour_vertices = pd.read_csv("sanity_check/contour_vertices.csv")
    ellipse_vertices = pd.read_csv("sanity_check/ellipse_vertices.csv")
    beam_ra = beamform_beams["ra"]
    beam_dec = beamform_beams["decl"]
    contour_ra = contour_vertices["ra"]
    contour_dec = contour_vertices["decl"]
    ellipse_ra = ellipse_vertices["ra"]
    ellipse_dec = ellipse_vertices["decl"]
    shifted_contours = []
    shifted_ellipses = []
    for n in range(0, len(beam_ra)):
        for m in range(0, len(contour_ra)):
            shifted_ra = beam_ra[n] + contour_ra[m]
            shifted_dec = beam_dec[n] + contour_dec[m]
            shifted_contours.append((shifted_ra, shifted_dec))
        for p in range(0, len(ellipse_ra)):
            shifted_ra = beam_ra[n] + ellipse_ra[p]
            shifted_dec = beam_dec[n] + ellipse_dec[p]
            shifted_ellipses.append((shifted_ra, shifted_dec))
    with open("sanity_check/shifted_contours.csv", "w") as f:
        cols = ("ra", "decl")
        writer = csv.writer(f)
        writer.writerow(cols)
        for item in shifted_contours:
            writer.writerow(item)
    with open("sanity_check/shifted_ellipses.csv", "w") as f:
        cols = ("ra", "decl")
        writer = csv.writer(f)
        writer.writerow(cols)
        for item in shifted_ellipses:
            writer.writerow(item)

    target_columns = [
        "ra",
        "decl",
        "beam_ra",
        "beam_decl",
        "source_id",
        "priority",
        "dist_c",
        "table_name",
    ]
    targets_dict = {
        k: [x[i] for x in targets_to_observe] for i, k in enumerate(target_columns)
    }

    pd.DataFrame.to_csv(
        pd.DataFrame.from_dict(targets_dict), "sanity_check/beamform_targets.csv"
    )

    return beams_dict, targets_dict
