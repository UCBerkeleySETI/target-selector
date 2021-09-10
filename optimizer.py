#!/usr/bin/env python
"""
Tools to optimize beam placement.
"""

from geometry import Circle, LinearTransform, Target
import math
import mip
import scipy.constants as con
import pandas as pd
import numpy as np
from scipy.spatial import KDTree

# One target of priority n is worth priority_decay targets of priority n+1.
priority_decay = 10


def optimize_circles(self, possible_targets, radius):
    """
    Calculate the best-scoring 64 beams assuming each beam is a circle with the given radius.
    possible_targets is a list of Target objects.

    This returns (circle_list, target_list) containing the Circle and Target objects
    for the optimal beam placement.

    Raises a RuntimeError if it cannot perform the optimization, usually if the data
    is degenerate in some way.
    """
    arr = np.array([[p.ra, p.dec] for p in possible_targets])
    tree = KDTree(arr)

    # Find all pairs of points that could be captured by a single observation
    pairs = tree.query_pairs(2 * radius)
    print(
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
            c0, c1 = self.intersect_two_circles(p0.ra, p0.dec, r, p1.ra, p1.dec, r)
            candidate_centers.append(c0)
            candidate_centers.append(c1)
        except ValueError:
            continue

    print(
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

    print(
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

    # Add a constraint that we must select at most 64 circles
    model += mip.xsum(circle_vars) <= 64

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
        print("Optimal solution found.")
    elif status == mip.OptimizationStatus.FEASIBLE:
        print("Feasible solution found.")
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

    return (selected_circles, selected_targets)


def optimize_ellipses(self, possible_targets, ellipse):
    """
    possible_targets is a list of Target objects.
    ellipse defines the shape of our beam.

    Returns a (beam_list, target_list) tuple containing the Beam and Target objects that
    can be observed with the optimal beam placement.
    """
    # Find a transform that makes the beam look like a circle and transform our data.
    t = LinearTransform.to_unit_circle(ellipse)
    transformed_targets = [t.transform_target(target) for target in possible_targets]

    # Solve the problem in the transformed space
    circles, selected_targets = optimize_circles(transformed_targets, 1)

    # Un-transform the answer
    inverse = t.invert()
    beams = [inverse.transform_beam(circle) for circle in circles]
    beam_targets = [inverse.transform_target(target) for target in selected_targets]
    return (beams, beam_targets)


def write_csvs(selected_beams, selected_targets):
    """
    Write out some csvs to inspect the solution to an optimizer run.
    """
    print("The solution observes {} unique targets.".format(len(selected_targets)))
    pcount = {}
    for t in selected_targets:
        pcount[t.priority] = pcount.get(t.priority, 0) + 1
    for p, count in sorted(pcount.items()):
        print("{} of the targets have priority {}".format(count, p))
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
        # print("Beam ({}, {}) contains targets {}".format(beam.ra, beam.dec, target_str))
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

    pd.DataFrame.to_csv(pd.DataFrame.from_dict(beams_dict), "beamform_beams.csv")
    print(beams_dict)
    # write_pair_redis(self.redis_server, "{}:current_obs:beamform_beams"
    #                  .format(product_id), json.dumps(beams_dict))

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

    pd.DataFrame.to_csv(pd.DataFrame.from_dict(targets_dict), "beamform_targets.csv")
    print(targets_dict)
    # write_pair_redis(self.redis_server, "{}:current_obs:beamform_targets"
    #                  .format(product_id), json.dumps(targets_dict))
