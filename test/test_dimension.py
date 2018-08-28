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

from coecms.dimension import *
import xarray
import numpy


def test_remove_degenerate_axes():
    a = xarray.DataArray([1, 2], dims=['i'])
    o = remove_degenerate_axes(a)

    numpy.testing.assert_array_equal(a.data, o.data)

    b = xarray.DataArray([[1, 2], [1, 2]], dims=['i', 'j'])
    o = remove_degenerate_axes(b)

    numpy.testing.assert_array_equal([1, 2], o.data)
