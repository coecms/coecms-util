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

import argparse
import mule
import numpy
import argparse


class CleanAstartOperator(mule.DataOperator):
    def __init__(self):
        super()

    def new_field(self, source):
        return source.copy()

    def transform(self, source, dest):
        data = source.get_data()

        if source.lbuser4 == 241:
            data = numpy.where(data > 0, data, 0)

        return data


def clean_astart(input_path, output_path):
    astart = mule.DumpFile.from_file(input_path)
    astart_out = astart.copy()

    def no_validate(*args, **kwargs):
        pass
    astart_out.validate = no_validate

    op = CleanAstartOperator()
    for f in astart.fields:
        astart_out.fields.append(op(f))
    astart_out.to_file(output_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('input')
    parser.add_argument('--output', '-o')
    args = parser.parse_args()

    clean_astart(args.input, args.output)
