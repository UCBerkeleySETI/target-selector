import numpy as np
from astropy import units as u
from astropy.time import Time
from astropy.coordinates import EarthLocation, AltAz

def calc_delay(ant_pos, p_az, p_alt, az, alt):
    """Calculates the antenna delay

    # TODO: Figure of the geometry of the MeerKAT telescope position coordinates

    Args:
        ant_pos: (np.ndarray)
            Positions should be defined from some reference antenna/point
        p_az: (float)
            Azimuth of the array pointing in radians
        p_polar: (float)
            Altitude of the array pointing in radians
        az: (float, np.ndarray)
            Target azimuth in radians
        alt: (float, np.ndarray)
            Target altitude in radians

    Returns:
        tau: (np.ndarray)
            Array of delay values

    """
    ant_pos = np.array(ant_pos)

    c = 3e8
    p_polar = np.pi / 2.0 - p_alt
    polar = np.pi / 2.0 - alt


    if len(ant_pos.shape) == 2:
        X, Y, Z = ant_pos[:, 0], ant_pos[:, 1], ant_pos[:, 2]
        if isinstance(polar, np.ndarray):
            X, Y, Z = X[:, np.newaxis], Y[:, np.newaxis], Z[:, np.newaxis]


    elif ant_pos.shape[0] == 3:
        X, Y, Z = ant_pos[:]

    # TODO: Add logger here

    p_delay = (X * np.cos(p_az) * np.sin(p_polar) + \
               Y * np.sin(p_az) * np.sin(p_polar) + \
               Z * np.cos(p_polar))

    s_delay = (X * np.cos(az) * np.sin(polar) + \
               Y * np.sin(az) * np.sin(polar) + \
               Z * np.cos(polar))

    tau = (s_delay - p_delay) / c

    return tau

def calc_weights(freqs, delay):
    """Calculates the weight of a given observation
    """
    if np.array(freqs).shape[0] == 1:
        phase = 2.0 * np.pi * freqs * delay

    else:
        phase = 2.0 * np.pi * freqs * delay[:, np.newaxis]

    return np.exp(-1j * phase)

def transform_to_az_alt(source, times):
    """Transform from ra/dec coordinates to alt/az coordinates from MeerKATs perspective

    Args:
        source: (astropy.coordinates.SkyCoord)
            SkyCoord object with coordinates for an object or objects
        times: (astropy.time.Time)
            Observation time(s)

    Returns:
        az: (astropy.coordinates.angles.Longitude)
            Azimuth coordinate list with shape (N_times x N_sources)
        alt: (astropy.coordinates.angles.Latitude)
            Altitude coordinate list with shape (N_times x N_sources)
    """
    mk_loc = _meerkat_location()
    meerkat_frame = AltAz(obstime = times, location=mk_loc)

    if len(source.shape) == 1 and len(times.shape) == 1:
        source = source[:, np.newaxis]

    frame = source.transform_to(meerkat_frame)
    return frame

def _meerkat_location():
    meerkat_pos = (-30.721111, 21.411111)
    loc_meerkat = EarthLocation(lat = meerkat_pos[0]*u.deg,
                                lon = meerkat_pos[1]*u.deg)
    return loc_meerkat
