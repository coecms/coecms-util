#!/usr/bin/env python
"""
Copyright 2018 

author:  <scott.wales@unimelb.edu.au>

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from __future__ import print_function

import iris
import xarray
import numpy as np
import matplotlib.pyplot as plt
import pandas
import numpy
from coecms.regrid import esmf_generate_weights, regrid


def scrip_grid_from_mask(mask):
    dims = ['ny', 'nx']

    dlat = (mask.lat[1] - mask.lat[0]).data
    dlon = (mask.lon[1] - mask.lon[0]).data

    lo, la = np.meshgrid(mask.lon, mask.lat)

    mask = xarray.DataArray(numpy.logical_not(mask.data), dims=dims)

    grid_dims = xarray.DataArray(list(reversed(mask.shape)), dims=['grid_rank'])

    lo = xarray.DataArray(lo, dims=dims)
    lo.attrs['units'] = 'degrees'
    la = xarray.DataArray(la, dims=dims)
    la.attrs['units'] = 'degrees'

    d1 = np.stack([lo, lo, lo, lo])
    d2 = np.stack([lo-dlon/2, lo+dlon/2, lo+dlon/2, lo-dlon/2])
    # Find the cell corners (starting at the bottom left, working anticlockwise)
    clo = xarray.DataArray(np.stack([lo-dlon/2, lo+dlon/2, lo+dlon/2, lo-dlon/2]),
            dims=['grid_corners', dims[0], dims[1]])
    clo.attrs['units'] = 'degrees'
    cla = xarray.DataArray(np.clip(np.stack([la-dlat/2, la-dlat/2, la+dlat/2, la+dlat/2]), -90, 90),
            dims=['grid_corners', dims[0], dims[1]])
    cla.attrs['units'] = 'degrees'

    # Calculate the cell area
    dlonr = dlon / 180.0 * np.pi
    la_low = np.clip((la - dlat/2) / 180 * np.pi, -np.pi/2, np.pi/2)
    la_high = np.clip((la + dlat/2) / 180 * np.pi, -np.pi/2, np.pi/2)
    area = xarray.DataArray(6371229.0**2 * dlonr * (np.sin(la_high) - np.sin(la_low)), dims=dims)
    area.attrs['units'] = 'm^2'
    area.attrs['planet_radius'] = 6371229.0

    ds = xarray.Dataset({'grid_center_lat': la, 'grid_center_lon': lo, 'grid_corner_lon': clo, 'grid_corner_lat': cla, 'grid_imask': mask, 'grid_area': area, 'grid_dims': grid_dims})
    ds = ds.stack(grid_size=('ny','nx')).reset_index('grid_size')

    ds['grid_corner_lon'] = ds['grid_corner_lon'].transpose()
    ds['grid_corner_lat'] = ds['grid_corner_lat'].transpose()

    return ds


def make_scrip_grids(mask_t):
    """
    Create SCRIP descriptions of the UM ENDGAME t, u and v grids from the t mask
    """
    dlat = (mask_t.lat[1] - mask_t.lat[0]).data
    dlon = (mask_t.lon[1] - mask_t.lon[0]).data
    lat_v = np.clip(np.append(mask_t.lat - dlat/2, mask_t.lat[-1] + dlat/2), -90, 90)
    lon_u = mask_t.lon - dlon/2

    mask_v = xarray.DataArray(np.zeros((lat_v.size, mask_t.lon.size), dtype='i4'), dims=['lat', 'lon'], coords={'lat': lat_v, 'lon': mask_t.lon})
    mask_u = xarray.DataArray(np.zeros((mask_t.lat.size, lon_u.size), dtype='i4'), dims=['lat', 'lon'], coords={'lat': mask_t.lat, 'lon': lon_u})

    ds_t = scrip_grid_from_mask(mask_t)
    ds_v = scrip_grid_from_mask(mask_v)
    ds_u = scrip_grid_from_mask(mask_u)

    return {'um_t': ds_t, 'um_u': ds_u, 'um_v': ds_v}


def merge_scrip_for_oasis(scrip_grids):
    """
    Merge the SCRIP grids to Oasis components
    """
    masks = xarray.Dataset()
    grids = xarray.Dataset()
    areas = xarray.Dataset()
    
    for name, scrip in scrip_grids.items():
        if 'nx' in scrip:
            del scrip['nx']
        if 'ny' in scrip:
            del scrip['ny']
        if 'grid_corners' in scrip:
            del scrip['grid_corners']

        scrip['grid_size'] = pandas.MultiIndex.from_product(
                [range(scrip.grid_dims.values[1]), range(scrip.grid_dims.values[0])],
                names=['ny','nx'])
        scrip = scrip.unstack('grid_size').transpose('grid_rank','grid_corners','ny','nx')

        scrip = scrip.rename({v: f'{name}.{v}' for v in scrip.variables})

        del scrip[f'{name}.nx']
        del scrip[f'{name}.ny']

        masks[f'{name}.msk'] = scrip[f'{name}.grid_imask'].astype('i4')

        grids[f'{name}.lon'] = scrip[f'{name}.grid_center_lon'].astype('f8')
        grids[f'{name}.lat'] = scrip[f'{name}.grid_center_lat'].astype('f8')
        grids[f'{name}.clo'] = scrip[f'{name}.grid_corner_lon'].astype('f8')
        grids[f'{name}.cla'] = scrip[f'{name}.grid_corner_lat'].astype('f8')

        areas[f'{name}.srf'] = scrip[f'{name}.grid_area'].astype('f8')

    return {'masks': masks, 'grids': grids, 'areas': areas}


def ocean_t_scrip(gridspec):
    grid_dims = xarray.DataArray(list(reversed(gridspec.wet.shape)), dims='grid_rank')

    area = gridspec.AREA_OCN.values
    mask = xarray.where(area > 0, 1, 0)

    ds = xarray.Dataset({
            'grid_dims': grid_dims,
            'grid_center_lat': gridspec.y_T,
            'grid_center_lon': gridspec.x_T,
            'grid_corner_lat': gridspec.y_vert_T,
            'grid_corner_lon': gridspec.x_vert_T,
            'grid_area': (['grid_y_T', 'grid_x_T'], area),
            'grid_imask': (['grid_y_T', 'grid_x_T'], mask),
        })
    ds = ds.rename({'vertex': 'grid_corners'})
    ds.grid_center_lat.attrs['units'] = 'degrees'
    ds.grid_center_lon.attrs['units'] = 'degrees'
    ds.grid_corner_lat.attrs['units'] = 'degrees'
    ds.grid_corner_lon.attrs['units'] = 'degrees'
    ds = ds.stack(grid_size=('grid_y_T','grid_x_T')).reset_index('grid_size')
    ds['grid_corner_lon'] = ds['grid_corner_lon'].transpose()
    ds['grid_corner_lat'] = ds['grid_corner_lat'].transpose()
    ds['grid_imask'] = numpy.logical_not(ds['grid_imask'])
    del ds['grid_x_T']
    del ds['grid_y_T']
    return ds


def rename_esmf_to_oasis(ds):
    ds = ds.rename({
        'n_s': 'num_links',
        'col': 'src_address',
        'row': 'dst_address',
        'S': 'remap_matrix',
        })
    ds.remap_matrix.expand_dims('num_wgts')
    return ds


def main():
    maskfile = '/g/data1a/access/TIDS/UM/ancil/atmos/n48e/orca1/land_sea_mask/etop01/v1/qrparm.landfrac'
    mask_iris = iris.load_cube(maskfile, iris.AttributeConstraint(STASH='m01s00i505'))
    mask_iris.coord('latitude').var_name = 'lat'
    mask_iris.coord('longitude').var_name = 'lon'
    frac_t = xarray.DataArray.from_iris(mask_iris).astype('i4')
    mask_t = xarray.where(frac_t < 1.0, 1, 0)

    scrip_grids = make_scrip_grids(mask_t)
    mom_gridspec = xarray.open_dataset('/projects/access/access-cm2/input_b/mom4/grid_spec.auscom.20110618.nc')
    scrip_grids['momt'] = ocean_t_scrip(mom_gridspec)

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
                ignore_unmapped=True,
                )
        rename_esmf_to_oasis(weights).to_netcdf(f'rmp_momt_to_{grid}_CONSERV_FRACAREA.nc')
        weights = esmf_generate_weights(
                scrip_grids[grid].reset_index('grid_size'),
                scrip_grids['momt'].reset_index('grid_size'),
                method='conserve',
                norm_type='fracarea',
                extrap_method='none',
                ignore_unmapped=True,
                )
        rename_esmf_to_oasis(weights).to_netcdf(f'rmp_{grid}_to_momt_CONSERV_FRACAREA.nc')

    for grid in ['um_t', 'um_u', 'um_v']:
        weights = esmf_generate_weights(
                scrip_grids['momt'].reset_index('grid_size'),
                scrip_grids[grid].reset_index('grid_size'),
                method='bilinear',
                extrap_method='neareststod',
                ignore_unmapped=True,
                )
        rename_esmf_to_oasis(weights).to_netcdf(f'rmp_momt_to_{grid}_BILINEAR.nc')
        weights = esmf_generate_weights(
                scrip_grids[grid].reset_index('grid_size'),
                scrip_grids['momt'].reset_index('grid_size'),
                method='bilinear',
                extrap_method='neareststod',
                ignore_unmapped=True,
                )
        rename_esmf_to_oasis(weights).to_netcdf(f'rmp_{grid}_to_momt_BILINEAR.nc')

if __name__ == '__main__':
    main()
