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

from datetime import date
import dask
import xarray
import pandas
import numpy


@dask.delayed
def open_erai_file_delayed(monstart, monend, domain, variable):
    path = ('/g/data1/ub4/erai/netcdf/6hr/%(domain)s/oper_an_sfc/v01/%(variable)s/'
            '%(variable)s_6hrs_ERAI_historical_an-sfc_%(monstart)s_%(monend)s.nc'
            %{
                'domain': domain,
                'variable': variable,
                'monstart': monstart.strftime('%Y%m%d'),
                'monend': monend.strftime('%Y%m%d'),
                })
    return xarray.open_dataset(path, chunks={'time': 1})[variable]


def open_erai_file(date, domain, variable, mold=None):
    times = pandas.date_range(
            date,
            date + pandas.tseries.offsets.MonthBegin(),
            freq='6H',
            closed='left')

    data = open_erai_file_delayed(times[0], times[-1], domain, variable)

    if mold is not None:
        # Return the variable contents as a delayed
        return dask.array.from_delayed(data.data, (times.size, mold.lat.size, mold.lon.size), mold.dtype)

    else:
        # Return the full dataset
        return data.compute()


def open_erai_var(months, domain, variable):
    mold = open_erai_file(months[0], domain, variable)

    times = pandas.date_range(
            months[0],
            months[-1] + pandas.tseries.offsets.MonthBegin(),
            freq='6H',
            closed='left')

    data = dask.array.concatenate(
            [open_erai_file(month, domain, variable, mold)
                for month in months],
            axis=0)

    da = xarray.DataArray(data,
            dims = mold.dims,
            coords = {'time': times, 'lat': mold.lat, 'lon': mold.lon},
            name = mold.name,
            attrs = mold.attrs,
            )
           
    return da


def erai(dataset):
    months = pandas.date_range('19790101', date.today(), freq='MS')

    erai_domain_vars = {
        'ocean': ['tos'],
        'seaIce': ['sic'],
        }

    ds = xarray.Dataset()

    for d, vs in erai_domain_vars.items():
        for v in vs:
            ds[v] = open_erai_var(months, d, v)

    return ds
