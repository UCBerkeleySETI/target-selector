import matplotlib
import math
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import astropy.coordinates as coord
import astropy.units as u
from astropy.io import ascii
from mpl_toolkits.axes_grid1.inset_locator import zoomed_inset_axes
from mpl_toolkits.axes_grid1.inset_locator import mark_inset

targets_all = pd.read_csv("sanity_check/fov_total_targets.csv")
beamform_coords = pd.read_csv("sanity_check/beamform_beams.csv")
beamform_targets = pd.read_csv("sanity_check/beamform_targets.csv")
shifted_contours = pd.read_csv("sanity_check/shifted_contours.csv")
shifted_ellipses = pd.read_csv("sanity_check/shifted_ellipses.csv")

targets_all_ra = targets_all['ra']
targets_all_dec = targets_all['decl']
beamform_coords_ra = beamform_coords['ra']
beamform_coords_decl = beamform_coords['decl']
beamform_targets_ra = beamform_targets['ra']
beamform_targets_decl = beamform_targets['decl']
shifted_contours_ra = shifted_contours['ra']
shifted_contours_decl = shifted_contours['decl']
shifted_ellipses_ra = shifted_ellipses['ra']
shifted_ellipses_decl = shifted_ellipses['decl']

# fig = plt.figure()
# ax = fig.add_subplot()
# axins = fig.add_subplot(122)

plt.xlabel("Right Ascension (deg)")
plt.ylabel("Declination (deg)")

plt.scatter(shifted_contours_ra, shifted_contours_decl, s=0.001, marker=",", color='pink')
plt.scatter(shifted_ellipses_ra, shifted_ellipses_decl, s=0.001, marker=",", color='cyan')
plt.scatter(targets_all_ra, targets_all_dec, s=0.01, marker="x", color='black')
# plt.scatter(beamform_coords_ra, beamform_coords_decl, s=0.001, marker=",", color='green')
plt.scatter(beamform_targets_ra, beamform_targets_decl, s=0.01, marker="x", color='blue')
# plt.set_aspect('equal')

# # sub region of the original image
# x1, x2, y1, y2 = 45.25, 45.75, -31.4, -31.0
# axins.set_xlim(x1, x2)
# axins.set_ylim(y1, y2)
#
# axins.scatter(points_all_ra, points_all_dec, s=0.5, marker="+", color='red')
# axins.scatter(points_64_ra, points_64_dec, s=0.5, marker="+", color='black')
# for m in circles_64.index:
#     beamform_full = plt.Circle((circles_64_ra[m], circles_64_dec[m]), radius=beamform_rad, color='black', fill=False, lw=0.2)
#     beamform_zoom = plt.Circle((circles_64_ra[m], circles_64_dec[m]), radius=beamform_rad, color='black', fill=False, lw=0.2)
#     plt.add_patch(beamform_full)
#     axins.add_patch(beamform_zoom)
# axins.set_aspect('equal')

plt.xticks(visible=False)
plt.yticks(visible=False)

# draw a bbox of the region of the inset axes in the parent axes and
# connecting lines between the bbox and the inset axes area
# mark_inset(ax, axins, loc1=2, loc2=3, fc="none", ec="0.5")

plt.savefig('test_plot.pdf')
plt.show()