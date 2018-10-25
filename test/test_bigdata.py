#!/usr/bin/env python
"""
file:   test/test_bigdata.py
author: Scott Wales <scott.wales@unimelb.edu.au>

Copyright 2018 ARC Centre of Excellence for Climate Systems Science

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from coecms.bigdata import *

import dask
import netCDF4
import numpy
import pandas
import xarray


def test_unsafe_mfdataset(tmpdir):
    path0 = tmpdir.join('0.nc')
    ds0 = xarray.Dataset()
    ds0['a'] = xarray.DataArray(numpy.random.rand(3, 6), dims=['x', 'time'])
    ds0.to_netcdf(path0)

    path1 = tmpdir.join('1.nc')
    ds1 = xarray.Dataset()
    ds1['a'] = xarray.DataArray(numpy.random.rand(3, 6), dims=['x', 'time'])
    ds1.to_netcdf(path1)

    ds = unsafe_mfdataset([path0, path1])
    assert ds['a'].shape == (3, 12)
    assert ds['time'].shape == (12,)
    numpy.testing.assert_array_equal(ds['time'], range(12))
