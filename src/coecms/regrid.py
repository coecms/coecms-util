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

from .grid import *

import subprocess
import xarray
import sparse
import dask.array
import tempfile
import sys


def cdo_generate_weights(source_grid, target_grid, method):
    """
    Generate weights for regridding using CDO

    Args:
        source_grid (xarray.Dataset): Source dataset
        target_grid (string): Path to file containing the target grid
            description
        method: Regridding method

    Returns:
        xarray.Dataset: Regridding weights
    """

    source_grid_file = tempfile.NamedTemporaryFile()
    target_grid_file = tempfile.NamedTemporaryFile()
    weight_file = tempfile.NamedTemporaryFile()

    try:
        source_grid.to_netcdf(source_grid_file.name)
        target_grid.to_cdo_grid(target_grid_file)

        subprocess.check_output([
            "cdo",
            "genbil,%s"%target_grid_file.name,
            source_grid_file.name,
            weight_file.name],
            stderr=subprocess.PIPE)

        weights = xarray.open_dataset(weight_file.name)
        return weights

    except subprocess.CalledProcessError as e:
        # Print the CDO error message
        print(e.stderr.decode(), file=sys.stderr)
        raise

    finally:
        source_grid_file.close()
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
    # Alias the weights dataset
    w = weights

    # Use sparse instead of scipy as it behaves with Dask
    weight_matrix = sparse.COO([w.src_address.data - 1, w.dst_address.data - 1], w.remap_matrix[:,0])

    lat = xarray.DataArray(w.dst_grid_center_lat.data.reshape(w.dst_grid_dims.data[::-1], order='F'),
            name='lat', attrs = w.dst_grid_center_lat.attrs, dims=['i','j'])

    lon = xarray.DataArray(w.dst_grid_center_lon.data.reshape(w.dst_grid_dims.data[::-1], order='F'),
            name='lon', attrs = w.dst_grid_center_lon.attrs, dims=['i','j'])

    data = dask.array.matmul(source_data.data.reshape(-1), weight_matrix)

    data = data.reshape(w.dst_grid_dims.data[::-1])

    return xarray.DataArray(data, dims=['i', 'j'], coords={'lat': lat, 'lon': lon},
            name=source_data.name, attrs=source_data.attrs)


class Regridder(object):
    """
    Set up the regridding operation

    For large grids you may wish to pre-calculate the weights using ESMF_RegridWeightGen, if not supplied weights will
    be calculated using CDO.

    Args:
        source_grid (:class:`coecms.grid.Grid` or xarray.Dataset): Source grid / sample dataset
        target_grid (:class:`coecms.grid.Grid` or xarray.Dataset): Target grid / sample dataset
        weights (xarray.Dataset): Pre-computed interpolation weights
        method: Regridding method
    """

    def __init__(self, source_grid, target_grid=None, method='bilinear', weights=None):

        if target_grid is None and weights is None:
            raise Exception("Either weights or target_grid must be supplied")

        # Is there already a weights file?
        if weights is not None:
            self._weights = weights
        else:
            # Generate the weights with CDO
            _source_grid = identify_grid(source_grid)
            _target_grid = identify_grid(target_grid)
            self._weights = cdo_generate_weights(_source_grid, _target_grid, method)


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


def regrid(source_data, target_grid=None, method='bilinear', weights=None):
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

    regridder = Regridder(source_data, target_grid=target_grid, weights=weights, method=method)

    return regridder.regrid(source_data)
