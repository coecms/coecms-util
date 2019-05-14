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
from ..grid import UMGrid
from ..regrid import regrid, esmf_generate_weights
from ..um.create_ancillary import create_surface_ancillary
import click
import pandas
import mule
import iris
import xarray
from dask.diagnostics import ProgressBar
import dask.distributed
import matplotlib.pyplot as plt

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
def era_sst(start_date, end_date, target_mask, output):
    """
    Create ancil files from ERA reanalysis data
    """

    um_grid = UMGrid.from_mask(target_mask)

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
                                 chunks={'time': 1,})
    sic = xarray.open_mfdataset(['/g/data1a/ub4/erai/netcdf/6hr/seaIce/'
                                 'oper_an_sfc/v01/sic/'
                                 'sic_6hrs_ERAI_historical_an-sfc_'+d+'.nc'
                                 for d in dates],
                                 chunks={'time': 1,})
    ds = xarray.Dataset({'tos': tos.tos, 'sic': sic.sic})
    ds = ds.sel(time=slice(start_date, end_date))

    weights = esmf_generate_weights(tos.tos.isel(time=0), um_grid, method='patch')
    newds = regrid(ds, weights=weights)

    print(newds)

    ancil = create_surface_ancillary(newds, {'tos': 24, 'sic': 31})
    ancil.to_file(output)
