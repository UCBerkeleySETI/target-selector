import math
import mip
import smallestenclosingcircle
import scipy.constants as con
import pandas as pd
import numpy as np
from scipy.spatial import KDTree

# One target of priority n is worth priority_decay targets of priority n+1.
priority_decay = 10


class Target(object):
    """
    We give each point an index based on its ordinal position in our input.
    Otherwise the data is precisely the data provided in redis.
    """

    def __init__(self, index, source_id, ra, decl, priority, dist_c, table_name):
        self.index = index
        self.source_id = source_id
        self.ra = ra
        self.decl = decl
        self.priority = priority
        self.dist_c = dist_c
        self.table_name = table_name

        # Targets with a lower priority have a higher score.
        # We are maximizing score of all targets.
        # The maximum priority is 7.
        self.score = int(priority_decay ** (7 - self.priority))


def beam_number(self, product_id, targets):
    """Function to calculate the maximum number of targets observable with 64 beams

    Thanks to:
    https://github.com/lacker/targeter

    """

    class Circle(object):
        """
        A circle along with the set of Targets that is within it.
        """

        def __init__(self, ra, decl, targets):
            self.ra = ra
            self.decl = decl
            self.targets = targets
            self.recenter()

        def key(self):
            """
            A tuple key encoding the targets list.
            """
            return tuple(t.index for t in self.targets)

        def recenter(self):
            """
            Alter ra and decl to minimize the maximum distance to any point.
            """
            points = [(t.ra, t.decl) for t in self.targets]
            x, y, r = smallestenclosingcircle.make_circle(points)
            assert r < beamform_rad
            self.ra, self.decl = x, y

    # Parse the json into Point objects for convenience
    points_db = [
        Target(index, *args)
        for (index, args) in enumerate(
            zip(
                targets["source_id"],
                targets["ra"],
                targets["decl"],
                targets["priority"],
                targets['dist_c'],
                targets['table_name']
            )
        )
    ]

    arr = np.array([[p.ra, p.decl] for p in points_db])
    tree = KDTree(arr)

    # Find all pairs of points that could be captured by a single observation
    pairs = tree.query_pairs(2 * beamform_rad)
    print("Of {} total remaining targets in the field of view,"
                " {} target pairs can be observed with a single formed beam".format(len(points_db), len(pairs)))

    # A list of (ra, decl) coordinates for the center of possible circles
    candidate_centers = []

    # Add one center for each of the targets that aren't part of any pairs
    in_a_pair = set()
    for i, j in pairs:
        in_a_pair.add(i)
        in_a_pair.add(j)
    for i in range(len(points_db)):
        if i not in in_a_pair:
            t = points_db[i]
            candidate_centers.append((t.ra, t.decl))

    # Add two centers for each pair of targets that are close to each other
    for i0, i1 in pairs:
        p0 = points_db[i0]
        p1 = points_db[i1]
        # For each pair, find two points that are a bit less than beamform_rad away from each point.
        # These are the possible centers of the circle.
        # TODO: make the mathematical argument of this algorithm's sufficiency clearer
        r = 0.9999 * beamform_rad
        try:
            c0, c1 = self.intersect_two_circles(p0.ra, p0.decl, r, p1.ra, p1.decl, r)
            candidate_centers.append(c0)
            candidate_centers.append(c1)
        except ValueError:
            continue

    print("Including targets insufficiently close to any others leaves"
                " {} candidates for beamforming coordinates".format(len(candidate_centers)))
    candidate_target_indexes = tree.query_ball_point(candidate_centers, beamform_rad)

    # Construct Circle objects.
    # Filter out any circles whose included targets are the same as a previous circle
    circles = []
    seen = set()
    for (ra, decl), target_indexes in zip(candidate_centers, candidate_target_indexes):
        targets = [points_db[i] for i in target_indexes]
        circle = Circle(ra, decl, targets)
        key = circle.key()
        if key in seen:
            continue
        seen.add(key)
        circles.append(circle)

    print("Removing functional duplicates leaves {} remaining candidates".format(len(circles)))

    # We want to pick the set of circles that covers the most targets.
    # This is the "maximum coverage problem".
    # https://en.wikipedia.org/wiki/Maximum_coverage_problem
    # We encode this as an integer linear program.
    model = mip.Model(sense=mip.MAXIMIZE)
    model.verbose = 0

    # Variable t{n} is whether the nth target is covered
    target_vars = [
        model.add_var(name="t{n}", var_type=mip.BINARY) for n in range(len(points_db))
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
        t.score * tvar for (t, tvar) in zip(points_db, target_vars)
    )

    # Optimize
    status = model.optimize(max_seconds=30)
    if status == mip.OptimizationStatus.OPTIMAL:
        print("Optimal solution found")
    elif status == mip.OptimizationStatus.FEASIBLE:
        print("Feasible solution found")
    else:
        print("No solution found. This is probably a bug")
        return

    selected_circles = []
    for circle, circle_var in zip(circles, circle_vars):
        if circle_var.x > 1e-6:
            selected_circles.append(circle)

    selected_targets = []
    for target, target_var in zip(points_db, target_vars):
        if target_var.x > 1e-6:
            selected_targets.append(target)

    print("The solution observes {} unique targets".format(len(selected_targets)))
    pcount = {}
    for t in selected_targets:
        pcount[t.priority] = pcount.get(t.priority, 0) + 1
    for p, count in sorted(pcount.items()):
        print("{} of the targets have priority {}".format(count, p))
    targets_to_observe = []
    circles_to_observe = []
    for circle in selected_circles:
        target_str = ", ".join(t.source_id for t in circle.targets)
        dist_str = ", ".join(str(t.dist_c) for t in circle.targets)
        priority_str = ", ".join(str(t.priority) for t in circle.targets)
        table_str = ", ".join(t.table_name for t in circle.targets)
        circles_to_observe.append([circle.ra, circle.decl, target_str, priority_str, dist_str, table_str])
        # print("Circle ({}, {}) contains targets {}".format(circle.ra, circle.decl, target_str))
        for t in circle.targets:
            targets_to_observe.append([t.ra, t.decl,
                                       circle.ra, circle.decl,
                                       t.source_id, t.priority,
                                       t.dist_c, t.table_name])

    circle_columns = ['ra', 'decl', 'source_id', 'contained_priority', 'contained_dist_c', 'contained_table']
    circles_dict = {k: [x[i] for x in circles_to_observe] for i, k in enumerate(circle_columns)}
    pd.DataFrame.to_csv(pd.DataFrame.from_dict(circles_dict), "beamform_beams.csv")
    print(circles_dict)
    # write_pair_redis(self.redis_server, "{}:current_obs:beamform_beams"
    #                  .format(product_id), json.dumps(circles_dict))

    target_columns = ['ra', 'decl', 'circle_ra', 'circle_decl', 'source_id', 'priority', 'dist_c', 'table_name']
    targets_dict = {k: [x[i] for x in targets_to_observe] for i, k in enumerate(target_columns)}
    pd.DataFrame.to_csv(pd.DataFrame.from_dict(targets_dict), "beamform_targets.csv")
    print(targets_dict)
    # write_pair_redis(self.redis_server, "{}:current_obs:beamform_targets"
    #                  .format(product_id), json.dumps(targets_dict))
