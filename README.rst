CLEX CMS Utility Library
========================

.. image:: https://img.shields.io/readthedocs/coecms-util/stable.svg
    :target: https://coecms-util.readthedocs.io
.. image:: https://img.shields.io/circleci/project/github/coecms/coecms-util/master.svg
    :target: https://circleci.com/gh/coecms/coecms-util/tree/master
.. image:: https://img.shields.io/codecov/c/github/coecms/coecms-util.svg
    :target: https://codecov.io/gh/coecms/coecms-util
.. image:: https://img.shields.io/codacy/grade/3706e7a283fd439fa8b8d2f707f814e4/master.svg
    :target: https://www.codacy.com/app/ScottWales/coecms-util
.. image:: https://img.shields.io/conda/v/coecms/coecms-util.svg
    :target: https://anaconda.org/coecms/coecms-util

Development
-----------

To add to the repository, first create your own fork on Github at https://github.com/coecms/coecms-util/fork, then download new forked repository (either to Raijin or your own computer)

Create and activate a new conda environment so you can work on the library without affecting your other environments, then install the library in development mode using ``pip``::

    # # On Raijin/VDI only:
    # module use /g/data3/hh5/public/modules
    # module load conda

    # This creates an environment named 'coecms'
    conda env create -f conda/dev-environment.yml

    conda activate coecms

    # '-e' installs in editable mode
    pip install -e .

You can then run the tests to confirm everything is working correctly using::

    py.test

Making Changes
--------------

Changes need to be done in a new branch as a pull request::

    git checkout -b my-branch

    # Make changes...

    git push --set-upstream origin my-branch

Then make a pull request at https://github.com/coecms/coecms-util/pull/new

Please make sure code changes include tests and documentation

Changes must be reviewed by someone in the CMS team before they are committed to master

Adding Dependencies
-------------------

To add a dependency, you need to edit two files:

* conda/meta.yaml: This has the conda package names of dependencies. They might not be the same as the actual Python library names. This is used by `conda build` to create the conda package. Add dependencies to the ``requirements: run:`` list.

* setup.py: This has the Python name of dependencies. This is used if you `pip install` the package locally for testing. Add dependencies to the ``install_requires`` list.

Creating new versions
---------------------

To create a new version of the library, use the Github interface at https://github.com/coecms/coecms-util/releases/new 

Versions should be named like ``v1.2.3``, using `semantic versioning <https://semver.org/>`_.

The Python and Conda packages will automatically set their version based on the tag that Github creates (you may need to ``git fetch`` the tag first)

To upload a new version to conda, check out the tag (e.g. ``git checkout v1.2.3``) then run::

    conda build --user coecms --python=3.7 ./conda

setting the python version as desired (you will need to be a member of the coecms group on https://anaconda.org to do this)
