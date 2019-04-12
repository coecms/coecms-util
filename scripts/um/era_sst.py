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

"""
Set up ERA-Interim SSTs and sea ice for a UM run
"""

from coecms.regrid import esmf_generate_weights, regrid
import argparse
import xarray
import iris
from dask.diagnostics import ProgressBar

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-date', help='ISO-formatted start date')
    parser.add_argument('--end-date', help='ISO-formatted end date')
    parser.add_argument('--output', '-o', help='Output file name', required=True)
    parser.add_argument('--target-mask', help='Target UM land mask', required=True)
    parser.add_argument('--frequency', choices=[6, 12, 24],
                        type=int, help='Update frequency (hours)', default=24)
    args = parser.parse_args()

    # Read in the source mask
    tos = xarray.open_mfdataset('/g/data1a/ub4/erai/netcdf/6hr/ocean/'
                                'oper_an_sfc/v01/tos/'
                                'tos_6hrs_ERAI_historical_an-sfc_2001*.nc',
                                coords='all')
    src_mask = tos.tos.isel(time=0)

    # Read in the target mask
    mask_iris = iris.load_cube(args.target_mask, iris.AttributeConstraint(STASH='m01s00i030'))
    mask_iris.coord('latitude').var_name = 'lat'
    mask_iris.coord('longitude').var_name = 'lon'
    tgt_mask = xarray.DataArray.from_iris(mask_iris).load()
    tgt_mask = tgt_mask.where(tgt_mask == 0)

    tgt_mask.lon.attrs['standard_name'] = 'longitude'
    tgt_mask.lat.attrs['standard_name'] = 'latitude'
    tgt_mask.lon.attrs['units'] = 'degrees_east'
    tgt_mask.lat.attrs['units'] = 'degrees_north'

    print(tgt_mask)

    weights = esmf_generate_weights(src_mask, tgt_mask, method='patch')

    with ProgressBar():

        # Read and slice the source data
        tos = xarray.open_mfdataset('/g/data1a/ub4/erai/netcdf/6hr/ocean/'
                                    'oper_an_sfc/v01/tos/'
                                    'tos_6hrs_ERAI_historical_an-sfc_2001*.nc',
                                    coords='all')
        sic = xarray.open_mfdataset('/g/data1a/ub4/erai/netcdf/6hr/seaIce/'
                                    'oper_an_sfc/v01/sic/'
                                    'sic_6hrs_ERAI_historical_an-sfc_2001*.nc',
                                    coords='all')
        ds = xarray.Dataset({'tos': tos.tos, 'sic': sic.sic})
        ds = ds.sel(time=slice(args.start_date, args.end_date))
        print(ds)

        newds = regrid(ds, weights=weights)

        newds['time'] = newds['time'].astype('i4')
        newds.to_netcdf(args.output)

if __name__ == '__main__':
    main()
