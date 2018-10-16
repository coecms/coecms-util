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
from datetime import datetime
from shutil import which


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


def esmf_generate_weights(
        source_grid,
        target_grid,
        method='bilinear',
        extrap_method='nearestidavg',
        ):
    """Generate regridding weights with ESMF

    https://www.earthsystemcog.org/projects/esmf/regridding

    Args:
        source_grid (:obj:`xarray.Dataarray`): Source grid. If masked the mask
            will be used in the regridding
        target_grid (:obj:`xarray.Dataarray`): Target grid. If masked the mask
            will be used in the regridding
        method (str): ESMF Regridding method, see ``ESMF_RegridWeightGen --help``
        extrap_method (str): ESMF Extrapolation method, see ``ESMF_RegridWeightGen --help``

    Returns:
        :obj:`xarray.Dataset` with regridding information from
            ESMF_RegridWeightGen
    """
    # Make some temporary files that we'll feed to ESMF
    source_file = tempfile.NamedTemporaryFile()
    target_file = tempfile.NamedTemporaryFile()
    weight_file = tempfile.NamedTemporaryFile()

    rwg = 'ESMF_RegridWeightGen'

    if which(rwg) is None:
        rwg = '/apps/esmf/7.1.0r-intel/bin/binO/Linux.intel.64.openmpi.default/ESMF_RegridWeightGen'

    if '_FillValue' not in source_grid.encoding:
        source_grid.encoding['_FillValue'] = -999999

    if '_FillValue' not in target_grid.encoding:
        target_grid.encoding['_FillValue'] = -999999

    try:
        source_grid.to_netcdf(source_file.name)
        target_grid.to_netcdf(target_file.name)

        command = [rwg,
            '--source', source_file.name,
            '--destination', target_file.name,
            '--weight', weight_file.name,
            '--method', method,
            '--extrap_method', extrap_method,
            '--ignore_unmapped',
            '--src_missingvalue', source_grid.name,
            '--dst_missingvalue', target_grid.name,
            '--no-log',
            ]

        out = subprocess.check_output(args=command,
            stderr=subprocess.PIPE)
        print(out.decode('utf-8'))

        weights = xarray.open_dataset(weight_file.name)
        return weights

    finally:
        # Clean up the temporary files
        source_file.close()
        target_file.close()
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

    if w.title.startswith('ESMF'):
        src_address = w.col - 1
        dst_address = w.row - 1
        remap_matrix = w.S
        w_shape = (w.sizes['n_a'], w.sizes['n_b'])

        dst_grid_shape = w.dst_grid_dims.data
        dst_grid_center_lat = w.yc_b.data.reshape(dst_grid_shape[::-1], order='C')
        dst_grid_center_lon = w.xc_b.data.reshape(dst_grid_shape[::-1], order='C')

    else:
        src_address = w.src_address - 1
        dst_address = w.dst_address - 1
        remap_matrix = w.remap_matrix[:,0]
        w_shape=(w.sizes['src_grid_size'], w.sizes['dst_grid_size'])

        dst_grid_shape = w.dst_grid_dims.data
        dst_grid_center_lat = w.dst_grid_center_lat.data.reshape(dst_grid_shape[::-1], order='C')
        dst_grid_center_lon = w.dst_grid_center_lon.data.reshape(dst_grid_shape[::-1], order='C')

    # Use sparse instead of scipy as it behaves better with Dask
    weight_matrix = sparse.COO([src_address.data, dst_address.data],
                               remap_matrix.data,
                               shape=w_shape,
                               )

    # Grab the target grid lats and lons - these are 1d arrays that we need to
    # reshape to the correct size
    lat = xarray.DataArray(dst_grid_center_lat, name='lat', dims=['i', 'j'])
    lon = xarray.DataArray(dst_grid_center_lon, name='lon', dims=['i', 'j'])

    # Reshape the source dataset, so that the last dimension is a 1d array over
    # the lats and lons that we can multiply against the weights array
    stacked_source = source_data.stack(latlon=('lat', 'lon'))
    stacked_source_masked = dask.array.ma.fix_invalid(stacked_source)

    # With the horizontal grid as a 1d array in the last dimension,
    # dask.array.matmul will multiply the horizontal grid by the weights for
    # each time/level for free, so we can avoid manually looping
    data = dask.array.matmul(stacked_source_masked, weight_matrix)
    mask = dask.array.matmul(dask.array.ma.getmaskarray(stacked_source_masked), weight_matrix)

    # Convert the regridded data into a xarray.DataArray. A bit of trickery is
    # required with the coordinates to get them back into two dimensions - at
    # this stage the horizontal grid is still stacked into one dimension
    out = xarray.DataArray(dask.array.ma.masked_array(data, mask=(mask != 0)),
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
    unstacked_out.coords['lat'] = unstacked_out.lat# / math.pi * 180.0
    unstacked_out.coords['lon'] = unstacked_out.lon# / math.pi * 180.0

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

    def __init__(self, source_grid=None, target_grid=None, method='bilinear', weights=None):

        if (source_grid is None or target_grid is None) and weights is None:
            raise Exception("Either weights or source_grid/target_grid must be supplied")

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
