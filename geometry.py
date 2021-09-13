#!/usr/bin/env python
"""
Utilities to do geometry in ra,dec space.
"""
import math
import numpy as np
import smallestenclosingcircle


def distance(point1, point2):
    x1, y1 = point1
    x2, y2 = point2
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)


class Target(object):
    """
    We give each point an index based on its ordinal position in our input.
    Otherwise the data is precisely the data provided in redis.
    """

    def __init__(self, index, source_id, ra, dec, priority, dist_c, table_name):
        self.index = index
        self.source_id = source_id
        self.ra = ra
        self.dec = dec
        self.priority = priority
        self.dist_c = dist_c
        self.table_name = table_name

        # Targets with a lower priority have a higher score.
        # We are maximizing score of all targets.
        # The maximum priority is 7.
        priority_decay = 10
        self.score = int(priority_decay ** (7 - self.priority))

    def __str__(self):
        return "target {}, priority {}, at ({:.3f}, {:.3f})".format(
            self.index, self.priority, self.ra, self.dec
        )

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
                    targets_dict["source_id"],
                    targets_dict["ra"],
                    targets_dict["decl"],
                    targets_dict["priority"],
                    targets_dict["dist_c"],
                    targets_dict["table_name"],
                )
            )
        ]


class Beam(object):
    """
    An object representing a beam aimed at a particular location, along with the targets
    visible in that beam.
    """

    def __init__(self, ra, dec, targets):
        self.ra = ra
        self.dec = dec
        self.targets = targets

    def key(self):
        """
        A tuple key encoding the targets list.
        """
        return tuple(t.index for t in self.targets)

    def __str__(self):
        return "beam at ({:.3f}, {:.3f})".format(self.ra, self.dec)


class Circle(Beam):
    """
    A beam shaped like a circle.
    """

    def __init__(self, ra, dec, radius, targets):
        super().__init__(ra, dec, targets)
        self.radius = radius
        self.recenter()

    def recenter(self):
        """
        Alter ra and dec to minimize the maximum distance to any point.
        """
        points = [(t.ra, t.dec) for t in self.targets]
        x, y, r = smallestenclosingcircle.make_circle(points)
        assert r < self.radius
        self.ra, self.dec = x, y


class Ellipse(object):
    def __init__(self, ra, dec, a, b, c):
        """
        An ellipse centered at the origin can be defined with the equation:
        ax^2 + bxy + cy^2 = 1
        
        This represents an ellipse of this shape, but centered at (ra, dec).
        You can think of it as defining x and y as
        x = (ra - ellipse.ra)
        y = (dec - ellipse.dec)

        This way of defining an ellipse makes it easy to translate.
        """
        self.ra = ra
        self.dec = dec
        self.a = a
        self.b = b
        self.c = c

    def centered_at(self, ra, dec):
        """
        A version of this ellipse that's translated to have the given center.
        """
        return Ellipse(ra, dec, self.a, self.b, self.c)

    def evaluate(self, ra, dec):
        """
        The evaluation is 0 at the ellipse center, in [0, 1) inside the ellipse,
        1 at the boundary, and greater than 1 outside the ellipse.
        """
        x = ra - self.ra
        y = dec - self.dec
        return self.a * x * x + self.b * x * y + self.c * y * y

    def max_dec_point(self):
        """
        The point with largest dec, on the boundary of the ellipse.

        From formula at:
        https://math.stackexchange.com/questions/616645/determining-the-major-minor-axes-of-an-ellipse-from-general-form
        """
        z = 4 * self.a * self.c - self.b * self.b
        assert z > 0

        y_t = 2 * math.sqrt(self.a / z)
        x_t = -0.5 * self.b * y_t / self.a
        return (self.ra + x_t, self.dec + y_t)

    def max_ra_point(self):
        """
        The point with largest ra, on the boundary of the ellipse.

        From formula at:
        https://math.stackexchange.com/questions/616645/determining-the-major-minor-axes-of-an-ellipse-from-general-form
        """
        z = 4 * self.a * self.c - self.b * self.b
        assert z > 0

        x_t = 2 * math.sqrt(self.c / z)
        y_t = -0.5 * self.b * x_t / self.c
        return (self.ra + x_t, self.dec + y_t)

    def horizontal_ray_intersection(self):
        """
        The ra value for a point that intersects a ray moving in the positive-ra
        direction from the center.
        """
        return self.ra + 1 / math.sqrt(self.a)

    def ray_intersection(self, slope, right_side):
        """
        (ra, dec) for the point leaving the center of the ellipse with the given slope.
        There are two solutions so if right_side we find the one with ra > self.ra,
        if not right_side we return the one with ra < self.ra.
        """
        # For x = ra - self.ra, y = dec - self.dec
        # y = mx
        # Ax^2 + Bxy + Cy^2 = 1
        # x = 1 / sqrt(A + Bm + Cm^2)
        x = 1 / math.sqrt(self.a + self.b * slope + self.c * slope * slope)
        if not right_side:
            x = -x
        y = slope * x
        return (self.ra + x, self.dec + y)

    def contour(self, num_points):
        """
        A list of (ra, dec) points that approximates the ellipse.
        """
        # Start with the rightmost half. theta is the angle from the origin,
        # it goes from -pi/2 to +pi/2 but not quite to the edge.
        epsilon = math.pi / 100
        start = -math.pi / 2
        end = math.pi / 2
        first_half = []
        second_half = []
        half_size = num_points // 2
        for i in range(half_size):
            theta = start + (i + 0.5) * (end - start) / half_size
            slope = math.tan(theta)
            first_half.append(self.ray_intersection(slope, True))
            second_half.append(self.ray_intersection(slope, False))
        return first_half + second_half

    @staticmethod
    def fit_with_center(center_ra, center_dec, points):
        """
        Create an ellipse with the center at (ra, dec) and using the provided points
        to fit an ellipse as closely as possible.
        Points are (ra, dec) tuples.
        """
        ras = np.array([ra - center_ra for (ra, _) in points])
        decs = np.array([dec - center_dec for (_, dec) in points])

        # Code based on http://juddzone.com/ALGORITHMS/least_squares_ellipse.html
        # for fitting an ellipse, but we adjust because when the ellipse is centered
        # at the origin, D and E must be zero.

        x = ras[:, np.newaxis]
        y = decs[:, np.newaxis]
        J = np.hstack((x * x, x * y, y * y))
        K = np.ones_like(x)
        JT = J.transpose()
        JTJ = np.dot(JT, J)
        InvJTJ = np.linalg.inv(JTJ)
        ABC = np.dot(InvJTJ, np.dot(JT, K))

        a, b, c = ABC.flatten()
        if a <= 0 or c <= 0:
            raise ValueError(
                "An ellipse could not be fitted. abc = ({}, {}, {})".format(a, b, c)
            )

        return Ellipse(center_ra, center_dec, a, b, c)


class LinearTransform(object):
    """
    A linear transformation on the (ra, dec) space.
    Can be defined as a 2x2 matrix.
    """

    def __init__(self, matrix):
        """
        Construct a transformation given the four components of its transform matrix.
        """
        self.matrix = matrix

    @staticmethod
    def from_elements(ra_ra, dec_ra, ra_dec, dec_dec):
        """
        Construct a transformation given the four components of its transform matrix.
        Intuitively you can think of the meaning of parameter names like "dec_ra" as
        "the influence of pre-transform-dec on post-transform ra", but you are also
        free to not think about this too deeply.
        """
        return LinearTransform(np.array([[ra_ra, dec_ra], [ra_dec, dec_dec]]))

    def transform_point(self, ra, dec):
        return tuple(np.matmul(self.matrix, [ra, dec]))

    def transform_target(self, target):
        new_ra, new_dec = self.transform_point(target.ra, target.dec)
        return Target(
            target.index,
            target.source_id,
            new_ra,
            new_dec,
            target.priority,
            target.dist_c,
            target.table_name,
        )

    def transform_beam(self, beam):
        ra, dec = self.transform_point(beam.ra, beam.dec)
        targets = [self.transform_target(t) for t in beam.targets]
        return Beam(ra, dec, targets)

    def invert(self):
        return LinearTransform(np.linalg.inv(self.matrix))

    @staticmethod
    def to_unit_circle(ellipse):
        """
        Find a linear transform that would transform the provided ellipse into a unit circle.
        This doesn't constrain to a single transformation, so for convenience we look for a
        "shear" transformation that keeps the dec=0 axis fixed.
        """
        e = ellipse.centered_at(0, 0)

        # This will go to (0, 1)
        top_ra, top_dec = e.max_dec_point()

        # (right_ra, 0) will go to (1, 0)
        right_ra = e.horizontal_ray_intersection()

        ra_ra = 1 / right_ra
        ra_dec = 0
        dec_dec = 1 / top_dec

        # top_ra * ra_ra + top_dec * dec_ra = 0, therefore:
        dec_ra = -1 * top_ra * ra_ra / top_dec

        return LinearTransform.from_elements(ra_ra, dec_ra, ra_dec, dec_dec)


if __name__ == "__main__":
    e = Ellipse(3, 7, 1, 0, 1)
    for point in e.contour(18):
        assert abs(distance(point, (3, 7)) - 1) < 0.01
