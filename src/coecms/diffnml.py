#!/usr/bin/env python
# Copyright 2019 ARC Centre of Excellence for Climate Extremes
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

import f90nml
import argparse
from collections import OrderedDict


def diffnml(a, b):
    """
    Create a diff of two f90nml instances

    Returns an ordered dict mapping tuples

       (section, key) -> (left, right)

    for (section, key) pairs where there is a difference, with left and right
    being the values of that (section, key) from namelists a and b respectively

    If that (section, key) is missing from one file then that file will report
    None
    """
    groups = set([*a.keys(), *b.keys()])
    output = OrderedDict()

    for g in groups:
        a_g = a.get(g, {})
        b_g = b.get(g, {})

        keys = set([*a_g.keys(), *b_g.keys()])
        for k in keys:
            a_k = a_g.get(k, None)
            b_k = b_g.get(k, None)

            if a_k != b_k:
                output[(g,k)] = (a_k, b_k)

    return output


def main(argv=None):
    parser = argparse.ArgumentParser(description="Creates a diff of two namelist files")
    parser.add_argument('filea', metavar='FILE1')
    parser.add_argument('fileb', metavar='FILE2')
    args = parser.parse_args(argv)

    print("< %s"%args.filea)
    print("> %s"%args.fileb)

    a = f90nml.read(args.filea)
    b = f90nml.read(args.fileb)

    for section, diff in diffnml(a,b).items():
        group, key = section
        left, right = diff
        print(f'&{group}%{key}:\n\t<   {left}\n\t>   {right}')


if __name__ == '__main__':
    main()

