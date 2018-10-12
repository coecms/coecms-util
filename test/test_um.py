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

from coecms.um import *

import xarray
import numpy


def test_create_surface_ancillary():
    da = xarray.Dataset(
        {'sst': (['time', 'lat', 'lon'], numpy.zeros((3, 3, 3)))},
        coords={
            'time': ('time', [1, 2, 3], {'units': 'days since 1900-01-01'}),
            'lat': ('lat', [1, 2, 3], {'axis': 'Y'}),
            'lon': ('lon', [1, 2, 3], {'axis': 'X'}),
        })
    da = xarray.decode_cf(da)

    ancil = create_surface_ancillary(da, {'sst': 507})

    # The file should pass Mule's internal checks
    ancil.validate()

    # The field grid should match the input grid
    assert ancil.fields[0].lbnpt == 3
    assert ancil.fields[0].lbrow == 3
    assert ancil.fields[0].bdx == 1
    assert ancil.fields[0].bdy == 1
    assert ancil.fields[0].bzx == da.lon[0] - 1
    assert ancil.fields[0].bzy == da.lat[0] - 1


def test_global_grid():
    n96e = global_grid('n96e')
    assert n96e.lons.size == 192
    assert n96e.lats.size == 144
    assert n96e.lons[0] == 0.937500
    assert n96e.lons[-1] == 359.062500
    assert n96e.lats[0] == -89.375000
    assert n96e.lats[-1] == 89.37500

    n96eu = global_grid('n96e','U')
    assert n96eu.lons.size == 192
    assert n96eu.lats.size == 144
    assert n96eu.lons[0] == 0.0
    assert n96eu.lons[-1] == 358.125000
    assert n96eu.lats[0] == -89.375000
    assert n96eu.lats[-1] == 89.37500

    n96ev = global_grid('n96e','V')
    assert n96ev.lons.size == 192
    assert n96ev.lats.size == 145
    assert n96ev.lons[0] == 0.937500
    assert n96ev.lons[-1] == 359.062500
    assert n96ev.lats[0] == -90.0
    assert n96ev.lats[-1] == 90.0

    n216e = global_grid('n216e')
    assert n216e.lons.size == 432
    assert n216e.lats.size == 324
    numpy.testing.assert_almost_equal(n216e.lons[0], 0.416667, decimal=6)
    numpy.testing.assert_almost_equal(n216e.lons[-1], 359.583333, decimal=6)
    numpy.testing.assert_almost_equal(n216e.lats[0], -89.722222, decimal=6)
    numpy.testing.assert_almost_equal(n216e.lats[-1], 89.722222, decimal=6)


def test_sstice_erai():
    ancil = sstice_erai('20010101','20010102','6H', global_grid('n96e'))

    # Check file resolution
    numpy.testing.assert_almost_equal(ancil.real_constants.start_lat, -89.375, decimal=6)
    numpy.testing.assert_almost_equal(ancil.real_constants.start_lon, 0.9375, decimal=6)
    numpy.testing.assert_almost_equal(ancil.real_constants.col_spacing, 1.875, decimal=6)
    numpy.testing.assert_almost_equal(ancil.real_constants.row_spacing, 1.25, decimal=6)

    # Check field resolution
    assert ancil.fields[0].lbnpt == 192
    assert ancil.fields[0].lbrow == 144
    numpy.testing.assert_almost_equal(ancil.fields[0].bzx, -0.9375, decimal=6)
    numpy.testing.assert_almost_equal(ancil.fields[0].bzy, -90.625, decimal=6)
