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
from .dimension import remove_degenerate_axes

import subprocess
import xarray
import sparse
import dask.array
import tempfile
import sys
import math


def cdo_generate_weights(source_grid, target_grid, method):
    """
    Generate weights for regridding using CDO

    Args:
        source_grid (xarray.Dataset): Source grid
        target_grid (xarray.Dataset): Target grid
            description
        method: Regridding method

    Returns:
        xarray.Dataset: Regridding weights
    """

    # Make some temporary files that we'll feed to CDO
    source_grid_file = tempfile.NamedTemporaryFile()
    target_grid_file = tempfile.NamedTemporaryFile()
    weight_file = tempfile.NamedTemporaryFile()

    try:
        source_grid.to_netcdf(source_grid_file.name)
        target_grid.to_cdo_grid(target_grid_file)

        # Run CDO
        subprocess.check_output([
            "cdo",
            "genbil,%s" % target_grid_file.name,
            source_grid_file.name,
            weight_file.name],
            stderr=subprocess.PIPE)

        # Grab the weights file it outputs as a xarray.Dataset
        weights = xarray.open_dataset(weight_file.name)
        return weights

    except subprocess.CalledProcessError as e:
        # Print the CDO error message
        print(e.stderr.decode(), file=sys.stderr)
        raise

    finally:
        # Clean up the temporary files
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
    # Alias the weights dataset from CDO
    w = weights

    # The weights file contains a sparse matrix, that we need to multiply the
    # source data's horizontal grid with to get the regridded data.
    #
    # A bit of messing about with `.stack()` is needed in order to get the
    # dimensions to conform - the horizontal grid needs to be converted to a 1d
    # array, multiplied by the weights matrix, then unstacked back into a 2d
    # array

    # Use sparse instead of scipy as it behaves better with Dask
    weight_matrix = sparse.COO([w.src_address.data - 1, w.dst_address.data - 1],
                               w.remap_matrix[:, 0],
                               shape=(w.sizes['src_grid_size'], w.sizes['dst_grid_size'])
                               )

    # Grab the target grid lats and lons - these are 1d arrays that we need to reshape to the correct size
    lat = xarray.DataArray(w.dst_grid_center_lat.data.reshape(w.dst_grid_dims.data[::-1], order='C'),
                           name='lat', attrs=w.dst_grid_center_lat.attrs, dims=['i', 'j'])
    lon = xarray.DataArray(w.dst_grid_center_lon.data.reshape(w.dst_grid_dims.data[::-1], order='C'),
                           name='lon', attrs=w.dst_grid_center_lon.attrs, dims=['i', 'j'])


    # Reshape the source dataset, so that the last dimension is a 1d array over
    # the lats and lons that we can multiply against the weights array
    stacked_source = source_data.stack(latlon=('lat', 'lon'))

    # With the horizontal grid as a 1d array in the last dimension,
    # dask.array.matmul will multiply the horizontal grid by the weights for
    # each time/level for free, so we can avoid manually looping
    data = dask.array.matmul(stacked_source.data, weight_matrix)

    # Convert the regridded data into a xarray.DataArray. A bit of trickery is
    # required with the coordinates to get them back into two dimensions - at
    # this stage the horizontal grid is still stacked into one dimension
    out = xarray.DataArray(data,
                           dims=stacked_source.dims,
                           coords={k: v for k, v in stacked_source.coords.items() if k != 'latlon'},
                           name=source_data.name,
                           attrs=source_data.attrs)

    # Add the new grid coordinates, stacking them the same way we stacked the source data
    out.coords['lat'] = lat.stack(latlon=('i', 'j'))
    out.coords['lon'] = lon.stack(latlon=('i', 'j'))

    # Now the coordinates have been added we can unstack the 'latlon' dimension
    # back into ('i', 'j').
    unstacked_out = out.unstack('latlon')

    # The coordinates in the weight file are 2d. If this is just a lat-lon grid
    # we should remove the unneccessary dimension from the coordinates
    unstacked_out.coords['lat'] = remove_degenerate_axes(unstacked_out.lat)
    unstacked_out.coords['lon'] = remove_degenerate_axes(unstacked_out.lon)

    # Convert from radians to degrees
    unstacked_out.coords['lat'] = unstacked_out.lat / math.pi * 180.0
    unstacked_out.coords['lon'] = unstacked_out.lon / math.pi * 180.0

    # Add metadata to the coordinates
    unstacked_out.coords['lat'].attrs['units'] = 'degrees_north'
    unstacked_out.coords['lat'].attrs['standard_name'] = 'latitude'
    unstacked_out.coords['lon'].attrs['units'] = 'degrees_east'
    unstacked_out.coords['lon'].attrs['standard_name'] = 'longitude'

    return unstacked_out


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
            self.weights = weights
        else:
            # Generate the weights with CDO
            _source_grid = identify_grid(source_grid)
            _target_grid = identify_grid(target_grid)
            self.weights = cdo_generate_weights(_source_grid, _target_grid, method)

    def regrid(self, source_data):
        """
        Regrid the xarray.Dataset ``source_data`` to match the target grid,
        using the weights stored in the regridder

        Args:
            source_data (xarray.Dataset): Source dataset

        Returns:
            xarray.Datset: Regridded version of the source dataset
        """

        return apply_weights(source_data, self.weights)


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
