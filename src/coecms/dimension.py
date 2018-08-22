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

def remove_degenerate_axes(coord):
    """
    Remove any degenerate axes from the coordinate, where all the values along a dimension are identical

    Args:
        coord (xarray.DataArray): Co-ordinate to operate on

    Returns:
        xarray.DataArray with degenerate axes removed
    """

    for d in coord.dims:
        if (coord.isel({d:0}) == coord.mean(dim=d)).all():
            coord = coord.mean(dim=d)

    return coord