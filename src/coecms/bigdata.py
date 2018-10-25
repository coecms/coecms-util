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

import xarray
import dask


def unsafe_mfdataset(paths, concat_dim='time', chunks={}, decode_cf=True,
        decode_times=True, **kwargs):
    """Virtually concatenate files, without checking dimensions

    xarray.open_mfdataset will check each file's dimensions to make sure they
    are consistent, which is time consuming for large collections. This
    function trusts that the metadata is consistent, which you should expect in
    a published dataset.

    Args:
        paths (iterable of str): An iterable with the path names to open. Files
            will be concatenated in this order

        concat_dim (str): Dimension name to concatenate along

        chunks (dict): Dask chunk size

        decode_cf (bool): Decode CF metadata

        decode_times (bool): Decode CF time axis to Python datetimes

        **kwargs: Passed to xarray.open_dataset

    Returns: xarray.Dataset containing a virtual aggregate of the files in
        ``paths``.
    """

    fs = []
    for p in paths:
        fs.append(xarray.open_dataset(p, chunks=chunks, decode_cf=False, **kwargs))

    ds = xarray.Dataset(attrs=fs[0].attrs)

    for k,v in fs[0].variables.items():
        if concat_dim in v.dims:
            concat_axis = v.dims.index(concat_dim)
            ds[k] = xarray.DataArray(
                    dask.array.concatenate([f[k].data for f in fs], axis=concat_axis),
                    attrs=v.attrs,
                    dims=v.dims,
                    )
        else:
            ds[k] = v

    if decode_cf:
        ds = xarray.decode_cf(ds, decode_times=decode_times)

    return ds

