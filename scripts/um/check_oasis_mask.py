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

import mule
import xarray
import matplotlib.pyplot as plt
import argparse
import numpy


def check_mask(oasis, um):
    oasis_mask = xarray.open_dataset(oasis)['um_t.msk']

    um_mask = numpy.where(mule.AncilFile.from_file(
        um).fields[0].get_data() < 1, 1, 0)

    plt.pcolormesh(1 - oasis_mask - um_mask)
    plt.colorbar()
    plt.show()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('um')
    parser.add_argument('oasis')
    args = parser.parse_args()

    check_mask(args.oasis, args.um)
