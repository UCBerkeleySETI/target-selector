def priority_decay():
    # One target of priority n is worth decay_parameter targets of priority n+1.
    decay_parameter = 10
    return decay_parameter


def min_local_attenuation():
    # The minimum coherent beam attenuation required for a target to be considered in targeting decisions
    min_attenuation = 0.5
    return min_attenuation


def min_include_attenuation():
    # The minimum coherent beam attenuation required for a target to be marked as observed
    # min_attenuation = 0.5
    # return min_attenuation
    pass


def primary_sensitivity_exponent():
    # The relative weighting of primary beam sensitivity compared with priority in target scoring
    exponent = 1
    return exponent


def number_beams():
    # The maximum number of coherent beams formable
    num_beams = 64
    return num_beams