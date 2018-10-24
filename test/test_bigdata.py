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


def test_virtual_dataset(tmpdir):
    # I can create an empty dataset
    mold_ds = xarray.Dataset()
    ds = virtual_dataset('/dev/null', [], mold_ds, 'time', [])
    assert len(ds) == 0

    # Only the variables in ``aggregate_vars`` are created
    mold_ds = xarray.Dataset()
    mold_ds['a'] = xarray.DataArray(numpy.zeros((2,)), dims=['time'])
    ds = virtual_dataset('/dev/null', [], mold_ds, 'time', numpy.array([1, 2]))
    assert len(ds) == 0
    ds = virtual_dataset('/dev/null', ['a'],
                         mold_ds, 'time', numpy.array([1, 2]))
    assert len(ds) == 1
    assert ds['a'].shape == (2,)

    # The ``dim`` axis comes from ``dim_values``, not mold
    mold_ds = xarray.Dataset()
    mold_ds['a'] = xarray.DataArray(numpy.zeros((6, 3)), dims=['time', 'x'])
    ds = virtual_dataset('/dev/null', ['a'],
                         mold_ds, 'time', numpy.array([1, 2]))
    assert ds['a'].shape == (2, 3)

    # Dimension ordering doesn't matter
    mold_ds = xarray.Dataset()
    mold_ds['a'] = xarray.DataArray(numpy.zeros((3, 6)), dims=['x', 'time'])
    ds = virtual_dataset('/dev/null', ['a'],
                         mold_ds, 'time', numpy.array([1, 2]))
    assert ds['a'].shape == (3, 2)

    # Same behaviour when mold_ds is a netCDF4 dataset
    mold_ds = xarray.Dataset()
    mold_ds['a'] = xarray.DataArray(
        numpy.random.rand(3, 6), dims=['x', 'time'])
    a_path = tmpdir.join('a.nc')
    mold_ds.to_netcdf(a_path)
    mold_ds = netCDF4.Dataset(a_path)
    ds = virtual_dataset(a_path, ['a'], mold_ds,
                         'time', numpy.array([1, 2, 3, 4, 5, 6]))
    assert ds['a'].shape == (3, 6)

    # Test using separate mold and source datasets
    # Data should come from the source file
    target_ds = xarray.Dataset()
    target_ds['a'] = xarray.DataArray(
        numpy.random.rand(3, 5), dims=['x', 'time'])
    b_path = tmpdir.join('b.nc')
    target_ds.to_netcdf(b_path)
    ds = virtual_dataset(b_path, ['a'], mold_ds,
                         'time', numpy.array([7, 8, 9, 10, 11]))
    assert ds['a'].shape == (3, 5)
    assert type(ds['a']) == dask.array.Array
    numpy.testing.assert_array_equal(ds['a'], target_ds['a'])


def test_unsafe_mfdataset(tmpdir):
    path0 = tmpdir.join('0.nc')
    ds0 = xarray.Dataset()
    ds0['a'] = xarray.DataArray(numpy.random.rand(3, 6), dims=['x', 'time'])
    ds0.to_netcdf(path0)

    path1 = tmpdir.join('1.nc')
    ds1 = xarray.Dataset()
    ds1['a'] = xarray.DataArray(numpy.random.rand(3, 6), dims=['x', 'time'])
    ds1.to_netcdf(path1)

    s0 = pandas.Series(path0, index=range(6))
    s1 = pandas.Series(path1, index=range(6, 12))
    s = pandas.concat([s0, s1])

    ds = unsafe_mfdataset(s)
    assert ds['a'].shape == (3, 12)
    assert ds['time'].shape == (12,)
    numpy.testing.assert_array_equal(ds['time'], range(12))
