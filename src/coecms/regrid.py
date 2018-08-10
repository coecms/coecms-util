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

from coecms.grid import identify_grid

import subprocess
import xarray
import numpy
import tempfile
import scipy.sparse


def cdo_generate_weights(source_data, target_grid, method):
    """
    Generate weights for regridding using CDO

    Args:
        source_data (xarray.Dataset): Source dataset
        target_grid (string): Path to file containing the target grid
            description
        method: Regridding method

    Returns:
        xarray.Dataset: Regridding weights
    """

    source_data_file = tempfile.NamedTemporaryFile()
    target_grid_file = tempfile.NamedTemporaryFile()
    weight_file = tempfile.NamedTemporaryFile()

    try:
        identify_grid(source_data).to_netcdf(source_data_file.name)
        identify_grid(target_grid).to_cdo_grid(target_grid_file)

        subprocess.check_output([
            "cdo",
            "genbil,%s"%target_grid_file.name,
            source_data_file.name,
            weight_file.name],
            stderr=subprocess.PIPE)

        weights = xarray.open_dataset(weight_file.name)
        return weights

    finally:
        source_data_file.close()
        target_grid_file.close()
        weight_file.close()



def apply_weights(source_data, weights):
    """
    Apply the CDO weights ``weights`` to ``source_data``, performing a regridding operation

    Args:
        source_data (xarray.Dataset): Source dataset
        weights (xarray.Dataset): CDO weights information

    Returns:
        xarray.Dataset: Regridded version of the source dataset
    """
    w = weights

    weight_matrix = scipy.sparse.coo_matrix((w.remap_matrix[:,0],
        (w.src_address.data - 1, w.dst_address.data -1)))

    lat = xarray.DataArray(w.dst_grid_center_lat.data.reshape(w.dst_grid_dims.data),
            name='lat', attrs = w.dst_grid_center_lat.attrs, dims=['i','j'])

    lon = xarray.DataArray(w.dst_grid_center_lon.data.reshape(w.dst_grid_dims.data),
            name='lon', attrs = w.dst_grid_center_lon.attrs, dims=['i','j'])

    data = (source_data.data.reshape(-1) * weight_matrix).reshape(w.dst_grid_dims.data)
    return xarray.DataArray(data, dims=['i', 'j'], coords={'lat': lat, 'lon': lon},
            name=source_data.name, attrs=source_data.attrs)


class Regridder(object):
    """
    Set up the regridding operation

    Args:
        source_grid (:class:`coecms.grid.Grid` or xarray.Dataset): Source grid / sample dataset
        target_grid (:class:`coecms.grid.Grid` or xarray.Dataset): Target grid / sample dataset
        method: Regridding method
    """

    def __init__(self, source_grid, target_grid, method):
        self._weights = cdo_generate_weights(source_grid, target_grid, method)


    def regrid(self, source_data):
        """
        Regrid the xarray.Dataset ``source_data`` to match the target grid,
        using the weights stored in the regridder

        Args:
            source_data (xarray.Dataset): Source dataset

        Returns:
            xarray.Datset: Regridded version of the source dataset
        """

        return apply_weights(source_data, self._weights)


def regrid(source_data, target_grid, method):
    """
    A simple regrid. Inefficient if you are regridding more than one dataset
    to the target grid because it re-generates the weights each time you call
    the function.

    To save the weights use :class:`Regridder`.

    Args:
        source_data (xarray.Dataset): Source dataset
        target_grid (:class:`coecms.grid.Grid` or xarray.Dataset): Target grid / sample dataset
        method: Regridding method

    Returns:
        xarray.Datset: Regridded version of the source dataset
    """

    regridder = Regridder(source_data, target_grid, method)

    return regridder.regrid(source_data)
