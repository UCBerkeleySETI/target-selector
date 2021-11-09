#!/usr/bin/env python
"""
Utilities to do geometry in ra,dec space.
"""
import math
import numpy as np
import os
import scipy.constants as con
from scipy.optimize import minimize
import smallestenclosingcircle


def cosine_attenuation(x):
    """
    The cosine-tapered field function, where x = rho / theta_b.
    See section 2.2.2 in https://iopscience.iop.org/article/10.3847/1538-4357/ab5d2d

    Key values:
    cosine_attenuation(0) = 1
    cosine_attenuation(0.5) = 0.5
    """
    k = 1.1889647809329454
    numer = math.cos(k * math.pi * x)
    denom = 1 - 4 * k * k * x * x
    return (numer / denom) ** 2


def distance(point1, point2):
    # Pythagorean distance formula
    x1, y1 = point1
    x2, y2 = point2
    return math.sqrt((x1 - x2) ** 2 + (y1 - y2) ** 2)


def great_circle_distance(point1, point2):
    # Great Circle Distance formula
    # Input in degrees
    x1, y1 = point1
    x2, y2 = point2
    # Output in degrees
    return np.rad2deg(math.acos(math.sin(math.radians(y1)) * math.sin(math.radians(y2))
                                + math.cos(math.radians(y1)) * math.cos(math.radians(y2))
                                * math.cos(math.fabs(math.radians(x2) - math.radians(x1)))))


def normalize_ra(pointing_ra, ra):
    """
    ra has meaning modulo 360. For example, -180 and 180 are the same ra.
    To normalize, we want to choose the one that is closest to pointing_ra.
    """
    while abs((ra + 360) - pointing_ra) < abs(ra - pointing_ra):
        ra = ra + 360
    while abs((ra - 360) - pointing_ra) < abs(ra - pointing_ra):
        ra = ra - 360
    return ra


class Target(object):
    def __init__(
        self,
        index,
        source_id,
        ra,
        dec,
        dist_c,
        table_name,
        priority,
        power_multiplier,
        score,
    ):
        """
        We give each point an index based on its ordinal position in our input.
        score defines our optimization; we look for targets that maximize the
        sum of their scores.
        Otherwise the data is precisely the data provided in redis.
        """
        self.index = index
        self.source_id = source_id
        self.ra = ra
        self.dec = dec
        self.dist_c = dist_c
        self.table_name = table_name
        self.priority = priority
        self.power_multiplier = power_multiplier
        self.score = score

    def __str__(self):
        return "Index {}: score {}, at ({:.3f}, {:.3f})".format(
            self.index, self.score, self.ra, self.dec
        )

    @staticmethod
    def parse_targets(targets_dict, pointing_ra, pointing_dec, frequency):
        """
        Create a list of Target objects from a serialized form.
        targets_dict is a dictionary containing keys with a bunch
        of parallel lists:
        source_id
        ra
        decl
        priority
        dist_c
        table_name

        Pointing ra/dec and frequency are used for prioritizing each target.
        Change the numerical logic in this function if you want to change how the targets
        are prioritized.
        The pointing coordinates are also used to normalize the target ra.
        """

        targets = []
        for index, (source_id, ra, dec, priority, dist_c, table_name) in enumerate(
            zip(
                targets_dict["source_id"],
                targets_dict["ra"],
                targets_dict["decl"],
                targets_dict["priority"],
                targets_dict["dist_c"],
                targets_dict["table_name"],
            )
        ):
            ra = normalize_ra(pointing_ra, ra)

            # Attenuation is a linear multiplier on score.
            # ASDF ASDF
            radial_offset = great_circle_distance((ra, dec), (pointing_ra, pointing_dec))
            beam_width = np.rad2deg((con.c / float(frequency)) / 13.5)
            proportional_offset = radial_offset / beam_width
            power_multiplier = cosine_attenuation(proportional_offset)

            # Targets with a lower priority have a higher score.
            # The maximum priority is 7.
            priority_decay = 10
            score = int((power_multiplier ** 1) * priority_decay ** (7 - priority))
            targets.append(
                Target(
                    index,
                    source_id,
                    ra,
                    dec,
                    dist_c,
                    table_name,
                    priority,
                    power_multiplier,
                    score,
                )
            )
        return targets


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


def find_zero(f, x, y, epsilon=3e-16):
    """
    Find the number where f(z) = 0, given x and y where f(x) < 0 and f(y) > 0.
    Get within epsilon.
    """
    assert f(y) > 0
    assert f(x) < 0

    z = (x + y) / 2
    if abs(x - y) < epsilon:
        return z

    result = f(z)
    if result == 0:
        return z
    if result < 0:
        return find_zero(f, z, y, epsilon=epsilon)
    return find_zero(f, x, z, epsilon=epsilon)


class Circle(Beam):
    """
    A beam shaped like a circle.
    """

    def __init__(self, ra, dec, radius, targets):
        super().__init__(ra, dec, targets)
        self.radius = radius

    def recenter_minimizing_max_distance(self):
        """
        Alter ra and dec to minimize the maximum distance to any point.
        """
        points = [(t.ra, t.dec) for t in self.targets]
        x, y, r = smallestenclosingcircle.make_circle(points)
        assert r < self.radius
        self.ra, self.dec = x, y

    def recenter_optimizing_attenuation(self, attenuation):
        """
        Find the optimal center based on attenuated score.
        (ra, dec) is an estimate of the optimal center, which we use to speed up our
        optimization.
        """
        # For fast optimization, we want our scoring function to be concave.
        # We also want to forbid any center further than radius for one of the targets.
        # So we add a penalty for excessive distance that is enough to make the score
        # of any center that's on the boundary to be zero.
        # We find a bound for the score so that we can be sure this penalty is large enough.
        total_score = sum(t.score for t in self.targets)

        # We are minimizing a loss function
        def loss_function(point):
            answer = 0
            for target in self.targets:
                dist = distance(point, (target.ra, target.dec))
                normalized_dist = dist / self.radius
                answer += target.score * attenuation(normalized_dist / 2)
                if normalized_dist > 0.999:
                    # Add a large penalty
                    answer -= 1000 * (normalized_dist - 0.999) * total_score
            # Make it negative so it's a "loss function"
            return -answer

        x0 = np.array([self.ra, self.dec])
        optimal = minimize(loss_function, x0, method="BFGS").x

        new_ra, new_dec = tuple(optimal)
        for target in self.targets:
            if distance((new_ra, new_dec), (target.ra, target.dec)) >= self.radius:
                # The "optimization" would move this target out of the beam.
                # Just do nothing and exit.
                return

        self.ra, self.dec = new_ra, new_dec

    def recenter(self, attenuation=None):
        """
        Recenter the circle, picking an appropriate method.
        """
        self.recenter_minimizing_max_distance()
        if len(self.targets) > 1 and attenuation is not None:
            self.recenter_optimizing_attenuation(attenuation)


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

    def contains(self, ra, dec):
        return self.evaluate(ra, dec) <= 1

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

    @staticmethod
    def inscribe_with_center(center_ra, center_dec, points):
        """
        Create an ellipse with the center at (ra, dec) that approximates the shape of the
        provided points, with the constraint that the ellipse does not contain any of
        the points.
        Points are (ra, dec) tuples.
        """
        fit = Ellipse.fit_with_center(center_ra, center_dec, points)
        min_eval = min(fit.evaluate(ra, dec) for (ra, dec) in points)
        scaling = 1 / min_eval
        return Ellipse(
            center_ra, center_dec, fit.a * scaling, fit.b * scaling, fit.c * scaling
        )

    def fractional_distances(self, targets):
        """
        Calculates the fractional distances for each target.
        Fractional distance is a fraction of the directional width.
        So it's 0 for the center point, 0.5 for anything on the boundary of the ellipse.
        """
        transform = LinearTransform.to_unit_circle(self)
        center_ra, center_dec = transform.transform_point(self.ra, self.dec)
        circle = Circle(
            center_ra, center_dec, 1, [transform.transform_target(t) for t in targets]
        )
        return [
            0.5 * distance((center_ra, center_dec), (t.ra, t.dec))
            for t in circle.targets
        ]


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
            target.dist_c,
            target.table_name,
            target.priority,
            target.power_multiplier,
            target.score,
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
