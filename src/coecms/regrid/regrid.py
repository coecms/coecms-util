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

import subprocess
import xarray
import numpy
import tempfile


def latlon_to_cdo(lat, lon, outfile):
    """
    Create a CDO gridfile for a latlon grid

    Args:
        lat (numpy.array): Lat of the grid, in ``degrees_north``
        lon (numpy.array): Lon of the grid, in ``degrees_east``
        outfile (file): File to write the grid to
    """

    outfile.write('gridtype = lonlat\n')

    outfile.write('xsize = %d\n'%lon.size)
    outfile.write('xvals = %s\n'%(','.join(['%f'%x for x in lon])))

    outfile.write('ysize = %d\n'%lat.size)
    outfile.write('yvals = %s\n'%(','.join(['%f'%x for x in lat])))

    outfile.flush()


def latlon_to_scrip(lat, lon):
    """
    Convert a lat-lon grid to SCRIP format for regridding

    Args:
        lat (numpy.array): Lat of the grid, in ``degrees_north``
        lon (numpy.array): Lon of the grid, in ``degrees_east``

    Returns:
        xarray.Dataset: Dataset containing a SCRIP description of the grid
    """
    lon = lon % 360

    top = (lat.shift(lat=-1)+lat)/2.0
    top[-1] = 90

    bot = (lat.shift(lat=1) + lat)/2.0
    bot[0] = -90

    left  = ((lon - (lon - lon.roll(lon=1).values)%360)/2.0) % 360
    right = (lon + ((lon.roll(lon=-1).values-lon)%360)/2.0) % 360

    center_lon, center_lat = numpy.meshgrid(lon, lat)

    corner_lon0, corner_lat0 = numpy.meshgrid(left, top)
    corner_lon1, corner_lat1 = numpy.meshgrid(left, bot)
    corner_lon2, corner_lat2 = numpy.meshgrid(right, bot)
    corner_lon3, corner_lat3 = numpy.meshgrid(right, top)

    corner_lat = numpy.array([x.reshape(-1) for x in [corner_lat0, corner_lat1, corner_lat2, corner_lat3]])
    corner_lon = numpy.array([x.reshape(-1) for x in [corner_lon0, corner_lon1, corner_lon2, corner_lon3]])

    scrip = xarray.Dataset(
            coords = {
                'grid_dims': (['grid_rank'], [lon.size,lat.size]),
                'grid_center_lat': (['grid_size'], center_lat.reshape(-1)),
                'grid_center_lon': (['grid_size'], center_lon.reshape(-1)),
                'grid_imask': (['grid_size'], numpy.ones(center_lat.size)),
                'grid_corner_lat': (['grid_size', 'grid_corners'], corner_lat.T),
                'grid_corner_lon': (['grid_size', 'grid_corners'], corner_lon.T),
                })

    scrip.grid_center_lat.attrs['units'] = 'degrees_north'
    scrip.grid_center_lon.attrs['units'] = 'degrees_east'
    scrip.grid_corner_lat.attrs['units'] = 'degrees_north'
    scrip.grid_corner_lon.attrs['units'] = 'degrees_east'

    return scrip


def cdo_generate_weights(source_data, target_grid_file):
    """
    Generate weights for regridding using CDO

    Args:
        source_data (xarray.Dataset): Source dataset
        target_grid (string): Path to file containing the target grid
            description

    Returns:
        xarray.Dataset: Regridding weights
    """
    source_data_file = tempfile.NamedTemporaryFile()
    weight_file = tempfile.NamedTemporaryFile()

    source_data.to_netcdf(source_data_file.name)

    subprocess.check_call(["cdo", "genbil,%s"%target_grid_file, source_data_file.name, weight_file.name])

    weights = xarray.open_dataset(weight_file.name)

    source_data_file.close()
    weight_file.close()

    return weights


def regrid(source_data, target_grid):
    """
    Regrid the xarray.Dataset ``source_data`` to match the target grid
    """

    target_scrip = grid_to_scrip(target_grid)

    target_scrip_file = "target.scrip.nc"
    source_grid_file = "source.nc"
    weight_file = "weight.nc"

    source_data.to_netcdf(source_grid_file)
    target_scrip.to_netcdf(target_scrip_file)

    subprocess.check_call(["cdo", "genbil,%s"%target_scrip_file, source_grid_file, weight_file])

    xarray.open_dataset(weight_file)

    return source_data
