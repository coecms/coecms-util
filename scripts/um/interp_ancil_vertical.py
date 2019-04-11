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
Interpolate a UM ancil file vertically to a new level set
"""

from coecms.um.vertical_interpolate import vertical_interpolate
import argparse

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('input', help='Input filename')
parser.add_argument('--output', '-o', required=True, help='Output filename')
parser.add_argument('--vertlevs', '-L', required=True, help='Target UM vertical namelist')
parser.add_argument('--orography', '-S', required=True, help='UM Orography ancil file')
args = parser.parse_args()

vertical_interpolate(infile=args.input, outfile=args.output,
                     orogfile=args.orography, vertlevs=args.vertlevs)
