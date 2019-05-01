#!/usr/bin/env python
#
# Copyright 2019 Scott Wales
#
# Author: Scott Wales <scott.wales@unimelb.edu.au>
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

from .main import cli
from ..regrid import regrid, esmf_generate_weights
import click
import pandas
import mule
import iris
import xarray
from dask.diagnostics import ProgressBar
import dask.distributed

@cli.group()
def um():
    """
    Tools for working with the Unified Model
    """
    pass

@um.group()
def ancil():
    """
    Tools for working with ancil files
    """
    pass

def validate_date(ctx, param, value):
    """
    Ensures an argument is a valid date
    """
    try:
        return pandas.to_datetime(value, utc=True, dayfirst=True)
    except ValueError:
        raise click.BadParameter(f'unable to parse "{value}" as a date')

def validate_um_ancil(ctx, param, value):
    """
    Ensures an argument is a UM file
    """
    try:
        return mule.AncilFile.from_file(value)
    except:
        raise click.BadParameter(f'"{value}" does not seem to be a UM ancil file')

@ancil.command()
@click.option('--start-date', callback=validate_date, required=True)
@click.option('--end-date', callback=validate_date, required=True)
@click.option('--target-mask',
              type=click.Path(exists=True, dir_okay=False))
@click.option('--output', required=True,
              type=click.Path(writable=True, dir_okay=False))
@click.option('--frequency', default='24', type=click.Choice(['6','12','24']))
def era_sst(start_date, end_date, target_mask, output, frequency):
    """
    Create ancil files from ERA reanalysis data
    """

    #c = dask.distributed.Client(n_workers=1, threads_per_worker=1, memory_limit='500mb')
    #print(c)

    mule_mask = mule.load_umfile(target_mask)
    global_mask = mule_mask.fixed_length_header.horiz_grid_type == 0

    mask = iris.load_cube(target_mask, iris.AttributeConstraint(STASH='m01s00i030'))
    mask.coord('latitude').var_name = 'lat'
    mask.coord('longitude').var_name = 'lon'

    mask = xarray.DataArray.from_iris(mask).load()
    mask = mask.where(mask == 0)

    mask.lon.attrs['standard_name'] = 'longitude'
    mask.lat.attrs['standard_name'] = 'latitude'
    mask.lon.attrs['units'] = 'degrees_east'
    mask.lat.attrs['units'] = 'degrees_north'

    with ProgressBar():

        file_start = start_date - pandas.offsets.MonthBegin()
        file_end = end_date + pandas.offsets.MonthEnd()
        file_a = pandas.date_range(file_start,file_end,freq='MS')
        file_b = file_a + pandas.offsets.MonthEnd()

        dates = [f'{a.strftime("%Y%m%d")}_{b.strftime("%Y%m%d")}'
                 for a,b in zip(file_a, file_b)]

        # Read and slice the source data
        tos = xarray.open_mfdataset(['/g/data1a/ub4/erai/netcdf/6hr/ocean/'
                                     'oper_an_sfc/v01/tos/'
                                     'tos_6hrs_ERAI_historical_an-sfc_'+d+'.nc'
                                     for d in dates],
                                     chunks={'time': 1, 'lat': 10})
        sic = xarray.open_mfdataset(['/g/data1a/ub4/erai/netcdf/6hr/seaIce/'
                                     'oper_an_sfc/v01/sic/'
                                     'sic_6hrs_ERAI_historical_an-sfc_'+d+'.nc'
                                     for d in dates],
                                     chunks={'time': 1, 'lat': 10})
        ds = xarray.Dataset({'tos': tos.tos, 'sic': sic.sic})
        ds = ds.sel(time=slice(start_date, end_date))

        print(ds)

        weights = esmf_generate_weights(tos.isel(time=0), mask, method='patch')
        newds = regrid(ds, weights=weights)

        print(newds)

        newds['time'] = newds['time'].astype('i4')
        newds.isel(time=slice(0,50)).to_netcdf(output)
