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
from __future__ import print_function

from coecms.diffnml import *
import f90nml
import io

def make_nml(string):
    return f90nml.read(io.StringIO(string))


def test_diffnml():
    """
    Check diff logic
    """
    a = make_nml("""
    &foo
    bar = 1
    /
    """)
    b = make_nml("""
    &foo
    bar = 'a'
    baz = 1
    /
    """)

    d = diffnml(a,b)

    assert d[('foo','bar')] == (1, 'a')
    assert d[('foo','baz')] == (None, 1)


def test_main(tmpdir, capsys):
    """
    Check output formatting
    """
    a = tmpdir.join('a')
    b = tmpdir.join('b')

    a.write("""
    &foo
    bar = 1
    /
    """)
    b.write("""
    &foo
    bar = 2
    /
    """)

    main([str(a),str(b)])
    
    captured = capsys.readouterr()
    assert captured.out == f"< {str(a)}\n> {str(b)}\n&foo%bar:\n\t<   1\n\t>   2\n"
