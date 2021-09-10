#!/usr/bin/env python
"""
Utilities to do geometry in ra,dec space.
"""
import numpy as np
import smallestenclosingcircle


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

    @staticmethod
    def parse_targets(targets_dict):
        """
        Create a list of Target objects from a dictionary containing keys with a bunch
        of parallel lists:
        source_id
        ra
        decl
        priority
        dist_c
        table_name
        """
        return [
            Target(index, *args)
            for (index, args) in enumerate(
                zip(
                    targets["source_id"],
                    targets["ra"],
                    targets["decl"],
                    targets["priority"],
                    targets["dist_c"],
                    targets["table_name"],
                )
            )
        ]


class Circle(object):
    """
    A circle along with the set of Targets that is within it.
    """

    def __init__(self, ra, decl, radius, targets):
        self.ra = ra
        self.decl = decl
        self.radius = radius
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
        assert r < self.radius
        self.ra, self.decl = x, y


class Ellipse(object):
    def __init__(self, ra, decl, a, b, c):
        """
        An ellipse centered at the origin can be defined with the equation:
        ax^2 + bxy + cy^2 = 1
        
        This represents an ellipse of this shape, but centered at (ra, decl).
        You can think of it as defining x and y as
        x = (ra - ellipse.ra)
        y = (decl - ellipse.decl)

        This way of defining an ellipse makes it easy to translate.
        """
        self.ra = ra
        self.decl = decl
        self.a = a
        self.b = b
        self.c = c

    def evaluate(ra, decl):
        """
        The evaluation is 0 at the ellipse center, in [0, 1) inside the ellipse,
        1 at the boundary, and greater than 1 outside the ellipse.
        """
        x = ra - self.ra
        y = decl - self.decl
        return self.a * x * x + self.b * x * y + self.c * y * y
