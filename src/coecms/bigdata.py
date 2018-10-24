#!/usr/bin/env python
"""
file:   src/coecms/bigdata.py
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

import dask
import netCDF4
import xarray


def unsafe_mfdataset(path_series, dim='time'):
    """Virtually concatenate files, without checking dimensions

    xarray.open_mfdataset will check each file's dimensions to make sure they
    are consistent, which is time consuming for large collections. This
    function trusts that the metadata is consistent, which you should expect in
    a published dataset.

    Args:
        path_series (pandas.Series): A Pandas series indexed by the whole
            dataset's `dim` axis, with values of the NetCDF4-compatible path
            (file path or OpenDAP URL) containing that value of `dim`.

            It is assumed that all values of the file's `dim` axis are present
            in the index, the values of `dim` in the file are in the same
            order as the index, and that the index values for each individual
            file are contiguous.

        dim (str): Dimension name to concatenate along

    Returns: xarray.Dataset containing a virtual aggregate of the files in
        ``path_series``. The returned dataset's ``dim`` coordinate will be the
        index of ``path_series``
    """
    # Group the series by the path value, so we have groups of (path, [time_axis])
    groups = path_series.groupby(path_series).groups

    paths = list(groups.keys())
    path_dates = list(groups.values())

    # Open just the first path, to get the metadata
    mold_ds = xarray.open_dataset(paths[0])

    # Work out which variables contain ``dim`` and which do not
    aggregate_vars = []
    static_vars = []
    for k, v in mold_ds.variables.items():
        if dim in v.dims:
            aggregate_vars.append(k)
        else:
            static_vars.append(k)

    # Get the dask arrays for each file
    # delayed_data is a list of dicts, each dict having the variables of a single file
    delayed_data = []
    for path, dates in groups.items():
        delayed_data.append(virtual_dataset(
            path, aggregate_vars, mold_ds, dim, dates))

    # Construct the new dataset
    ds = xarray.Dataset()
    ds.attrs = mold_ds.attrs

    # Construct DataArrays for each variable
    for v in aggregate_vars:
        # Aggregate Dask arrays
        concat_axis = mold_ds[v].dims.index(dim)
        data = dask.array.concatenate(
            [d[v] for d in delayed_data], axis=concat_axis)

        # Copy metadata from the mold dataset
        da = xarray.DataArray(data,
                              dims=mold_ds[v].dims,
                              attrs=mold_ds[v].attrs,
                              name=mold_ds[v].name)

        # Add the now concatenated DataArray to the output dataset
        ds[v] = da

    # Copy the non-collated variables from the first file
    for v in static_vars:
        ds[v] = mold_ds[v]

    return ds


@dask.delayed
def delayed_open(path):
    """Delayed open of a single file

    Use this with dask.array.from_delayed to create a lazily loaded array
    without actually opening a netCDF file
    """
    return netCDF4.Dataset(path)


def virtual_dataset(path, aggregate_vars, mold_ds, dim, dim_values):
    """Prepare Dask arrays for each variables in the file, without opening it

    Only opens the file when the variables needs to be read

    Args:
        path (str): netCDF4-compatible path to open
        aggregate_vars (list of str): Variable names to return from the file
        mold_ds (netCDF4.Dataset): Sample file showing the variable types and sizes
        dim (str): Dimension name that changes between paths
        dim_values (numpy.array): Values of ``dim`` for this path

    Returns: dict mapping each variable name in ``aggregate_vars`` to a
        dask.array containing the values of that variable in ``path``
    """
    # Delayed opening of the file
    ds = delayed_open(path)

    data = {}

    for v in aggregate_vars:
        if v == dim:  # No need for a Dask array
            data[v] = dim_values
            continue

        # Get the variable attributes from the mold Dataset
        dtype = mold_ds[v].dtype
        shape = list(mold_ds[v].shape)

        # Overwrite the size on the concat dimension
        try:
            concat_axis = mold_ds[v].dims.index(dim)
        except AttributeError:
            # NetCDF4 instead of xarray
            concat_axis = mold_ds[v].dimensions.index(dim)
        shape[concat_axis] = dim_values.size

        # Construct a Dask array for the variable
        data[v] = dask.array.from_delayed(ds[v], shape=shape, dtype=dtype)

    return data
