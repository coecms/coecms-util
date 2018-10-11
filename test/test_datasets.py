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

from coecms.datasets import *

from unittest.mock import patch
import xarray
import dask

def test_open_erai_file():
    with patch('xarray.open_dataset') as open_dataset:
        open_erai_file(pandas.to_datetime('20000101'), 'ocean', 'tos')

        open_dataset.assert_called_with('/g/data1/ub4/erai/netcdf/6hr/ocean/oper_an_sfc/v01/tos/tos_6hrs_ERAI_historical_an-sfc_20000101_20000131.nc', chunks={})


def test_open_erai_var():
    months = pandas.date_range('20000101', '20181006', freq='MS')

    def sample_data(*args, **kwargs):
        ds =  xarray.DataArray(numpy.zeros((2,2,2)),
                coords=[
                    ('time', [0,1]),
                    ('lat', [0,1]),
                    ('lon', [0,1]),
                    ])
        return xarray.Dataset({'tos': ds})

    with patch('xarray.open_dataset', side_effect=sample_data) as open_dataset:
        open_erai_var(months, 'ocean', 'tos')

        open_dataset.assert_called_once()
        open_dataset.assert_called_with('/g/data1/ub4/erai/netcdf/6hr/ocean/oper_an_sfc/v01/tos/tos_6hrs_ERAI_historical_an-sfc_20000101_20000131.nc', chunks={})
