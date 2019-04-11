Regridding
==========

The coecms regridder makes use of CDO to generate its regridding weights.

Effectively behind the scenes it runs::

    cdo genbil,gridspec sourcegrid.nc weights.nc
    cdo remap,gridspec,weights.nc source.nc dest.nc

with some added Dask magic.

There are two ways to run the regridder. You can create a
:class:`coecms.regrid.Regridder`, which stores the weights for re-use, or you
can call :func:`coecms.regrid.regrid` to quickly regrid a single dataset.

.. autoclass:: coecms.regrid.Regridder
    :members:

.. autofunction:: coecms.regrid.regrid

.. autofunction:: coecms.regrid.cdo_generate_weights

.. autofunction:: coecms.regrid.esmf_generate_weights
