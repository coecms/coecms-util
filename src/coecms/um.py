#!/usr/bin/env python
# Copyright 2018 ARC Centre of Excellence for Climate Extremes
# author: Scott Wales <scott.wales@unimelb.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function

import mule
import numpy
import xarray
import dask.array


def create_um_surface_ancillary(input_ds, output_filename, stash_map):
    """Create a surface-level UM ancillary file

    Args:
        input_ds: Source dataset/dataarray
        output_filename: UM ancillary file to create
        stash_map: Mapping of `input_ds` variable name to STASH code

    Returns:
        :obj:`mule.AncilFile`: Ancillary file data

    Example:
        ::

            input_ds = xarray.open_mfdataset(files, engine='pynio')
            stash_map = {'CI_GDS0_SFC': 31, 'SSTK_GDS0_SFC': 507,}

            ancil = create_um_surface_ancillary(input_ds, stash_map)
            ancil.to_file('sstice.ancil')

    Todo:
        * Assumes Gregorian calendar
        * Assumes sub-daily frequency
        * Does not compress output
    """

    time = input_ds.initial_time0_hours
    lat = input_ds.g0_lat_1
    lon = input_ds.g0_lon_2

    tstep = (time[1] - time[0]) / numpy.timedelta64(1,'s')

    template = {
        'fixed_length_header': {
            'sub_model': 1,         # Atmosphere
            'dataset_type': 4,      # Ancillary
            'horiz_grid_type': 0,   # Global
            'calendar': 1,          # Gregorian
            'grid_staggering': 6,   # EndGame
            'time_type': 1,         # Time series
            'model_version': 1006,  # UM 10.6
            # Start time
            't1_year': time.dt.year.values[0],
            't1_month': time.dt.month.values[0],
            't1_day': time.dt.day.values[0],
            't1_hour': time.dt.hour.values[0],
            't1_minute': time.dt.minute.values[0],
            't1_second': time.dt.second.values[0],
            # End time
            't2_year': time.dt.year.values[-1],
            't2_month': time.dt.month.values[-1],
            't2_day': time.dt.day.values[-1],
            't2_hour': time.dt.hour.values[-1],
            't2_minute': time.dt.minute.values[-1],
            't2_second': time.dt.second.values[-1],
            # Frequency (must be sub-daily)
            't3_year': 0,
            't3_month': 0,
            't3_day': 0,
            't3_hour': tstep / 3600,
            't3_minute': tstep % 3600 / 60,
            't3_second': tstep % 60,
        },
        'integer_constants': {
            'num_times': time.size,
            'num_cols': lon.size,
            'num_rows': lat.size,
            'num_levels': 1,
            'num_field_types': len(stash_map),
        },
        'real_constants': {
            'start_lat': lat.values[0] + (lat.values[1] - lat.values[0])/2.0,
            'row_spacing': lat.values[1] - lat.values[0],
            'start_lon': lon.values[0] + (lon.values[1] - lon.values[0])/2.0,
            'col_spacing': lon.values[1] - lon.values[0],
            'north_pole_lat': 90,
            'north_pole_lon': 0,
        },
        'level_dependent_constants': {
            'dims': (1,None)
        },
    }

    ancil = mule.AncilFile.from_template(template)

    # UM Missing data magic value
    MDI = -1073741824.0

    for var, stash in stash_map.items():
        # Mask out NANs with MDI
        var_data = xarray.where(dask.array.isnan(input_ds[var]), MDI, input_ds[var])

        for t in var_data.initial_time0_hours:
            field = mule.Field3.empty()

            field.lbyr = t.dt.year.values
            field.lbmon = t.dt.month.values
            field.lbdat = t.dt.day.values
            field.lbhr = t.dt.hour.values
            field.lbmin = t.dt.minute.values
            field.lbsec = t.dt.second.values

            field.lbtime = 1        # Instantaneous Gregorian calendar
            field.lbcode = 1        # Regular Lat-Lon grid
            field.lbhem = 0         # Global

            field.lbrow = ancil.integer_constants.num_rows
            field.lbnpt = ancil.integer_constants.num_cols

            field.lbpack = 0        # No packing
            field.lbrel = 3         # UM 8.1 or later
            field.lbvc = 129        # Surface field

            field.lbuser1 = 1       # Real data
            field.lbuser4 = stash   # STASH code
            field.lbuser7 = 1       # Atmosphere model

            field.bplat = ancil.real_constants.north_pole_lat
            field.bplon = ancil.real_constants.north_pole_lon

            field.bdx = ancil.real_constants.col_spacing
            field.bdy = ancil.real_constants.row_spacing
            field.bzx = ancil.real_constants.start_lon - field.bdx / 2.0
            field.bzy = ancil.real_constants.start_lat - field.bdy / 2.0

            field.bmdi = MDI
            field.bmks = 1.0

            field.set_data_provider(mule.ArrayDataProvider(var_data.sel(initial_time0_hours = t)))

            ancil.fields.append(field)

    return ancil
