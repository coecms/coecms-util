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

from coecms.um.create_ancillary import *

import xarray
import numpy


def test_create_surface_ancillary():
    da = xarray.Dataset(
        {'sst': (['time', 'lat', 'lon'], numpy.zeros((3, 3, 3)))},
        coords={
            'time': ('time', [1, 2, 3], {'units': 'days since 1900-01-01'}),
            'lat': ('lat', [1, 2, 3], {'axis': 'Y'}),
            'lon': ('lon', [1, 2, 3], {'axis': 'X'}),
        })
    da = xarray.decode_cf(da)

    ancil = create_surface_ancillary(da, {'sst': 507})

    # The file should pass Mule's internal checks
    ancil.validate()
