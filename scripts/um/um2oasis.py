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


from coecms.um.um2oasis import *


def main():
    maskfile = '/g/data1a/access/TIDS/UM/ancil/atmos/n48e/orca1/land_sea_mask/etop01/v1/qrparm.landfrac'
    frac_iris = iris.load_cube(maskfile, iris.AttributeConstraint(STASH='m01s00i505'))
    frac_iris.coord('latitude').var_name = 'lat'
    frac_iris.coord('longitude').var_name = 'lon'
    um_grid = xarray.DataArray.from_iris(frac_iris)

    mom_gridspec = xarray.open_dataset('/projects/access/access-cm2/input_b/mom4/grid_spec.auscom.20110618.nc')

    lfrac = create_um_lfrac_from_mom(mom_gridspec, um_grid)
    correct_ancils(lfrac)

    print(lfrac)
    lfrac.plot()
    plt.show()

    
    scrip_grids = um_endgame_scrip_grids(lfrac)
    scrip_grids['momt'] = mom_t_scrip_grid(mom_gridspec)

    oasis_grids = merge_scrip_for_oasis(scrip_grids)

    oasis_grids['masks'].to_netcdf('masks.nc')
    oasis_grids['grids'].to_netcdf('grids.nc')
    oasis_grids['areas'].to_netcdf('areas.nc')

    for grid in ['um_t']:
        weights = esmf_generate_weights(
                scrip_grids['momt'].reset_index('grid_size'),
                scrip_grids[grid].reset_index('grid_size'),
                method='conserve',
                norm_type='fracarea',
                extrap_method='none',
                ignore_unmapped=False,
                )
        rename_weights_esmf_to_scrip(weights).to_netcdf(f'rmp_momt_to_{grid}_CONSERV_FRACAREA.nc')
        weights = esmf_generate_weights(
                scrip_grids[grid].reset_index('grid_size'),
                scrip_grids['momt'].reset_index('grid_size'),
                method='conserve',
                norm_type='fracarea',
                extrap_method='none',
                ignore_unmapped=False,
                )
        rename_weights_esmf_to_scrip(weights).to_netcdf(f'rmp_{grid}_to_momt_CONSERV_FRACAREA.nc')

    for grid in ['um_t', 'um_u', 'um_v']:
        weights = esmf_generate_weights(
                scrip_grids['momt'].reset_index('grid_size'),
                scrip_grids[grid].reset_index('grid_size'),
                method='patch',
                extrap_method='nearestidavg',
                line_type='greatcircle',
                pole='none',
                ignore_unmapped=True,
                )
        rename_weights_esmf_to_scrip(weights).to_netcdf(f'rmp_momt_to_{grid}_PATCH.nc')
        weights = esmf_generate_weights(
                scrip_grids[grid].reset_index('grid_size'),
                scrip_grids['momt'].reset_index('grid_size'),
                method='patch',
                extrap_method='nearestidavg',
                line_type='greatcircle',
                pole='none',
                ignore_unmapped=True,
                )
        rename_weights_esmf_to_scrip(weights).to_netcdf(f'rmp_{grid}_to_momt_PATCH.nc')

if __name__ == '__main__':
    main()
