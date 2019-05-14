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

from coecms.cli.um import *

import pytest
import platform
import mule
from click.testing import CliRunner

nci_only = pytest.mark.skipif(not platform.node().startswith('raijin'),
        reason='Test works only on Raijin')

@nci_only
def test_ancil_era_sst(tmpdir):
    output = tmpdir + 'era_sst'

    runner = CliRunner()
    runner.invoke(era_sst, 
            ['--start-date','19960102',
             '--end-date', '19960103',
             '--target-mask', '/g/data/access/TIDS/UM/ancil/atmos/n48e/land_sea_mask/igbp/v2/qrparm.mask',
             '--output', str(output)],
            catch_exceptions=False)

    ancil = mule.ancil.AncilFile.from_file(str(output))

    assert len(ancil.fields) == 10
    assert ancil.fields[-1].lbyr == 1996
    assert ancil.fields[-1].lbmon == 1

    assert ancil.fields[0].lbdat == 2
    assert ancil.fields[-1].lbdat == 3

    assert ancil.fields[0].lbhr == 0
    assert ancil.fields[-1].lbhr == 0
