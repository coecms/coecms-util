#!/usr/bin/env python
# Copyright 2018
#
# author:  <scott.wales@unimelb.edu.au>
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
:mod:`coecms.um.um2oasis`
------------------------------------------------------------------------------------------

Functions for creating OASIS regridding files
"""

from __future__ import print_function

import xarray
import numpy as np
import pandas
import numpy
import mule
from coecms.regrid import esmf_generate_weights, regrid


def latlon_scrip_grid(mask, planet_radius=6371229.0):
    """
    Create a SCRIP description of a lat-lon grid from a mask

    Coordinate information is read from the mask's coordinates. Regular spacing of coordinates is assumed.

    Args:
        mask (xarray.DataArray): Source grid mask
        planet_radius (float): Planet radius for cell area calculation

    Returns:
        xarray.DataArray containing the SCRIP description
    """
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

    area = xarray.DataArray(planet_radius**2 * dlonr * (np.sin(la_high) - np.sin(la_low)), dims=dims)
    area.attrs['units'] = 'm^2'
    area.attrs['planet_radius'] = planet_radius

    ds = xarray.Dataset({'grid_center_lat': la, 'grid_center_lon': lo, 'grid_corner_lon': clo, 'grid_corner_lat': cla, 'grid_imask': mask, 'grid_area': area, 'grid_dims': grid_dims})
    ds = ds.stack(grid_size=('ny','nx')).reset_index('grid_size')

    ds['grid_corner_lon'] = ds['grid_corner_lon'].transpose()
    ds['grid_corner_lat'] = ds['grid_corner_lat'].transpose()

    return ds


def um_endgame_scrip_grids(frac_t):
    """
    Create SCRIP descriptions of the UM ENDGAME t, u and v grids from the land fraction

    Args:
        frac_t (xarray.DataArray): UM land fractions on the theta grid

    Returns:
        dict mapping UM grid names to SCRIP descriptions of that grid
    """
    mask_t = xarray.where(frac_t < 1.0, 0, 1).astype('i4')

    dlat = (mask_t.lat[1] - mask_t.lat[0]).data
    dlon = (mask_t.lon[1] - mask_t.lon[0]).data
    lat_v = np.clip(np.append(mask_t.lat - dlat/2, mask_t.lat[-1] + dlat/2), -90, 90)
    lon_u = mask_t.lon - dlon/2

    mask_v = xarray.DataArray(np.zeros((lat_v.size, mask_t.lon.size), dtype='i4'), coords=[('lat', lat_v), ('lon', mask_t.lon)])
    mask_u = xarray.DataArray(np.zeros((mask_t.lat.size, lon_u.size), dtype='i4'), coords=[('lat', mask_t.lat), ('lon', lon_u)])

    ds_t = latlon_scrip_grid(mask_t)
    ds_v = latlon_scrip_grid(mask_v)
    ds_u = latlon_scrip_grid(mask_u)

    ds_t['grid_area'] = ds_t.grid_area

    return {'um_t': ds_t, 'um_u': ds_u, 'um_v': ds_v}


def mom_t_scrip_grid(gridspec):
    """
    Create a SCRIP description of the MOM tracer grid

    Args:
        gridspec (xarray.DataArray): MOM grid_spec.nc data

    Returns:
        xarray.DataArray containing the SCRIP description
    """
    grid_dims = xarray.DataArray(list(reversed(gridspec.wet.shape)), dims='grid_rank')

    area = gridspec.AREA_OCN.values
    mask = gridspec.wet

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

    del ds['grid_x_T']
    del ds['grid_y_T']
    return ds


def merge_scrip_for_oasis(scrip_grids):
    """
    Merge a set of SCRIP grids into OASIS grid description files

    Args:
        scrip_grids: A dict mapping OASIS grid names to SCRIP descriptions

    Returns:
        dict mappining keys 'masks', 'grids' and 'areas' to xarray.DataArrays
        with the relevant fields copied from the SCRIP descriptions
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

        masks[f'{name}.msk'] = (1 - scrip[f'{name}.grid_imask']).astype('i4')

        grids[f'{name}.lon'] = scrip[f'{name}.grid_center_lon'].astype('f8')
        grids[f'{name}.lat'] = scrip[f'{name}.grid_center_lat'].astype('f8')
        grids[f'{name}.clo'] = scrip[f'{name}.grid_corner_lon'].astype('f8')
        grids[f'{name}.cla'] = scrip[f'{name}.grid_corner_lat'].astype('f8')

        areas[f'{name}.srf'] = scrip[f'{name}.grid_area'].astype('f8')

    return {'masks': masks, 'grids': grids, 'areas': areas}


def rename_weights_esmf_to_scrip(ds):
    """
    Rename fields in a ESMF_RegridWeightGen weights file to the format used by
    SCRIP

    Args:
        ds (xarray.DataArray): ESMF weights data

    Returns:
        xarray.DataArray with SCRIP field names
    """

    ds = ds.rename({
        'n_s': 'num_links',
        'n_a': 'src_grid_size',
        'n_b': 'dst_grid_size',
        'nv_a': 'src_grid_corners',
        'nv_b': 'dst_grid_corners',
        'area_a': 'src_grid_area',
        'area_b': 'dst_grid_area',
        'frac_a': 'src_grid_frac',
        'frac_b': 'dst_grid_frac',
        'xc_a': 'src_grid_center_lat',
        'xc_b': 'dst_grid_center_lat',
        'yc_a': 'src_grid_center_lon',
        'yc_b': 'dst_grid_center_lon',
        'xv_a': 'src_grid_corner_lat',
        'xv_b': 'dst_grid_corner_lat',
        'yv_a': 'src_grid_corner_lon',
        'yv_b': 'dst_grid_corner_lon',
        'col': 'src_address',
        'row': 'dst_address',
        'S': 'remap_matrix',
        })
    ds['remap_matrix'] = ds.remap_matrix.expand_dims('num_wgts').transpose()
    ds.attrs['conventions'] = 'SCRIP'
    return ds


def create_um_lfrac_from_mom(gridspec, targetgrid):
    """
    Sets up UM land fraction consistent with the MOM mask by interpolating the
    MOM 'wet' field conservatively

    Args:
        gridspec (xarray.DataArray): MOM grid_spec.nc data
        targetgrid (xarray.DataArray): Sample field on the UM target grid

    Returns:
        xarray.DataArray containing the UM land fraction on the target grid
    """
    src_scrip = mom_t_scrip_grid(gridspec)
    tgt_scrip = latlon_scrip_grid(targetgrid)

    src_scrip['grid_imask'] = src_scrip.grid_imask * 0 + 1
    tgt_scrip['grid_imask'] = tgt_scrip.grid_imask * 0 + 1

    weights = esmf_generate_weights(
            src_scrip,
            tgt_scrip,
            method='conserve',
            norm_type='fracarea',
            extrap_method='none',
            ignore_unmapped=True,
            )

    mom_wet_on_um = regrid(xarray.DataArray(gridspec.wet.values, dims=['lat', 'lon']), weights=weights)

    um_land_frac = 1 - mom_wet_on_um

    # Clean the field limits
    um_land_frac = um_land_frac.where(um_land_frac < 0.99, 1.0)
    um_land_frac = um_land_frac.where(um_land_frac > 0.01, 0.0)

    return um_land_frac


class LFracCorrector(mule.DataOperator):
    """
    Mule operator to override the land mask and land fraction fields
    """
    def __init__(self, lfrac):
        """
        Args:
            lfrac (xarray.DataArray): New land fraction field
        """
        super()
        self.lfrac = lfrac #: Target land fraction

    def new_field(self, source):
        """
        Creates a new field
        """
        return source.copy()

    def transform(self, source, dest):
        """
        Replaces data in the field with the values from `self.lfrac`
        """
        data = source.get_data()
        data_orig = numpy.copy(data)

        if source.lbuser4 == 30:
            # Land mask
            data = numpy.where(self.lfrac > 0, 1, 0)

        if source.lbuser4 == 33:
            # Orography - set to minimum 1 where there's land
            data = numpy.where(numpy.logical_and(data < 0, self.lfrac > 0), 1, data)

        if source.lbuser4 == 505:
            # Land fraction
            data = self.lfrac

        # Remove missing data that's been unmasked
        data = numpy.where(numpy.logical_and(data == -1073741824, self.lfrac > 0), 0, data)

        return data


def correct_ancils(lfrac, mask_ancil, frac_ancil, outdir):
    """
    Create new UM ancil files by replacing the land fraction and mask fields in
    ``mask_ancil`` and ``frac_ancil`` with the values from ``lfrac``

    Creates new files ``qrparm.mask`` and ``qrparm.landfrac`` in ``outdir``

    Args:
        lfrac (xarray.DataArray): Land fractions on the UM grid
        mask_ancil (string): Path to the source mask ancil file
        lfrac_ancil (string): Path to the source lfrac ancil file
        outdir (string): Output directory for new ancil files
    """

    lcorrect = LFracCorrector(lfrac)

    def do_correct(infile, outfile):
        ff = mule.AncilFile.from_file(infile)
        ff_out = ff.copy()
        for f in ff.fields:
            ff_out.fields.append(lcorrect(f))
        ff_out.to_file(outfile)

    do_correct(mask_ancil, os.path.join(outdir, 'qrparm.mask'))
    do_correct(frac_ancil, os.path.join(outdir, 'qrparm.landfrac'))


