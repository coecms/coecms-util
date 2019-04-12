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
:mod:`coecms.um.vertical_interpolate`
------------------------------------------------------------------------------------------

Functions for vertical interpolation of UM files
"""

import mule
import pandas
import numpy
import f90nml
import stratify

def vertical_interpolate(infile, outfile, orogfile, vertlevs):
    """
    Perform a vertical interpolation of ancil file 'infile', using the level
    definition namelist 'vertlevs'

    Args:
        infile (string): Path to input UM ancil file
        outfile (string): Path to output UM ancil file
        orogfile (string): Path to UM orography for true level calculations
        vertlevs (string): Path to UM vertical namelist file for target levels
    """

    ancil = mule.AncilFile.from_file(infile)

    def categorise_fields(m):
        df = pandas.DataFrame({'field': m.fields})
        df['year'] = df['field'].apply(lambda f: f.lbyr)
        df['month'] = df['field'].apply(lambda f: f.lbmon)
        df['day'] = df['field'].apply(lambda f: f.lbdat)
        df['hour'] = df['field'].apply(lambda f: f.lbhr)
        df['minute'] = df['field'].apply(lambda f: f.lbmin)
        df['second'] = df['field'].apply(lambda f: f.lbsec)
        df['stash'] = df['field'].apply(lambda f: f.lbuser4)
        df['vertical_type'] =df['field'].apply(lambda f: f.lbvc)
        df['level'] = df['field'].apply(lambda f: f.lblev)
        df['pseudo'] = df['field'].apply(lambda f: f.lbuser5)

        #df['bulev'] = df['field'].apply(lambda f: f.bulev)
        df['blev'] = df['field'].apply(lambda f: f.blev)
        df['brlev'] = df['field'].apply(lambda f: f.brlev)

        #df['bhulev'] = df['field'].apply(lambda f: f.bhulev)
        df['bhlev'] = df['field'].apply(lambda f: f.bhlev)
        df['bhrlev'] = df['field'].apply(lambda f: f.bhrlev)

        return df

    # Categorise the 2d slices in the input file
    df = categorise_fields(ancil)

    # Get the orography
    orog_file = mule.AncilFile.from_file(orogfile)
    orog = orog_file.fields[0].get_data()

    levtype = 'theta'
    target_levels = f90nml.read(vertlevs)['VERTLEVS']

    if levtype == 'rho':
        # Rho levels
        eta = numpy.array(target_levels['eta_rho'])
        const_lev = target_levels['first_constant_r_rho_level']-1
    if levtype == 'theta':
        # Theta levels
        eta = numpy.array(target_levels['eta_theta'])
        const_lev = target_levels['first_constant_r_rho_level']-1

    # True height of the target levels
    target_Zsea = target_levels['z_top_of_model'] * eta
    target_C = (1 - eta/eta[const_lev])**2
    target_C[const_lev:] = 0
    target_Z = target_Zsea[:, numpy.newaxis, numpy.newaxis] + numpy.multiply.outer(target_C,orog)

    ancil_out = ancil.copy()

    # Group the 2d slices with the same field and time value together
    for name, g in df.groupby(['year','month','day','hour','minute','second', 'stash']):
        print("%04d%02d%02dT%02d:%02d:%02d STASH %d"%name)

        # Stack the slices into a 3d array
        cube = numpy.stack(g['field'].apply(lambda f: f.get_data()))

        # True height of each position
        Zsea = g['blev']
        C = g['bhlev']
        Z = Zsea[:, numpy.newaxis, numpy.newaxis] + numpy.multiply.outer(C,orog)

        # Interpolate from the source true height to the target true height
        new_cube = stratify.interpolate(target_Z, Z, cube, axis=0, extrapolation='nearest')

        for level in range(1,new_cube.shape[0]):
            f = g.iloc[0].at['field'].copy()

            f.lblev = level+1
            f.blev = target_Zsea[level]
            f.brlev = -1073741824
            f.bhlev = target_C[level]
            f.bhrlev = -1073741824

            f.set_data_provider(mule.ArrayDataProvider(new_cube[level,:,:]))
            ancil_out.fields.append(f)

    ancil_out.to_file(outfile)

