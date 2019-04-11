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
Remove masked values from an ancil file using nearest grid point interpolation
"""

import mule
import numpy
import xarray
import argparse
from coecms.grid import LonLatGrid
from coecms.regrid import esmf_generate_weights, regrid


class RegridOperator(mule.DataOperator):
    def __init__(self, weights):
        super()
        self.weights = weights

    def new_field(self, source):
        return source.copy()

    def transform(self, source, dest):
        data = xarray.DataArray(
            source.get_data().astype('f4'), dims=['lat', 'lon'])

        newdata = regrid(data, weights=self.weights).values

        if source.lbuser4 in [217]:
            # Floor of 0
            newdata = numpy.where(newdata > 0, newdata, 0)

        return newdata


def demask(input_path, output_path):
    """
    Remove a mask from a UM ancil file by doing a one-to-one regrid from a
    masked field to an unmasked field with extrapolation
    """
    anc = mule.AncilFile.from_file(input_path)

    field0 = anc.fields[0]
    mask = numpy.where(field0.get_data() == -1073741824, 0, 1)

    lats = xarray.DataArray(
        field0.bzy + (1 + numpy.array(range(mask.shape[0]))) * field0.bdy,
        dims='lat')
    lons = xarray.DataArray(
        field0.bzx + (1 + numpy.array(range(mask.shape[1]))) * field0.bdx,
        dims='lon')

    src_grid = LonLatGrid(lats=lats, lons=lons, mask=mask).to_scrip()
    tgt_grid = LonLatGrid(lats=lats, lons=lons).to_scrip()

    weights = esmf_generate_weights(src_grid, tgt_grid, method='neareststod',
                                    line_type='greatcircle', extrap_method='neareststod')

    op = RegridOperator(weights=weights)

    anc_out = anc.copy()

    def no_validate(*args, **kwargs):
        pass
    anc_out.validate = no_validate

    for f in anc.fields:
        anc_out.fields.append(op(f))
    anc_out.to_file(output_path)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('input', help='Input filename')
    parser.add_argument('--output', '-o', help='Output filename', required=True)
    args = parser.parse_args()

    demask(args.input, args.output)


if __name__ == '__main__':
    main()
