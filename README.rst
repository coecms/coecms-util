CLEX CMS Utility Library
========================

.. image:: https://img.shields.io/readthedocs/coecms-util/stable.svg
    :target: https://coecms-util.readthedocs.io
.. image:: https://img.shields.io/circleci/project/github/coecms/coecms-util.svg
    :target: https://circleci.com/gh/coecms/coecms-util/tree/master
.. image:: https://img.shields.io/codecov/c/github/coecms/coecms-util.svg
    :target: https://codecov.io/gh/coecms/coecms-util
.. image:: https://img.shields.io/codacy/grade/3706e7a283fd439fa8b8d2f707f814e4.svg
    :target: https://www.codacy.com/app/ScottWales/coecms-util
.. image:: https://img.shields.io/conda/v/coecms/coecms-util.svg
    :target: https://anaconda.org/coecms/coecms-util

Development
-----------

Create and activate a conda environment::

    conda env create -f conda/dev-environment.yml
    conda activate coecms

Install the package::

    pip install -e .

Run the tests::

    py.test

Making Changes
--------------

To add to the repository, first create your own fork on Github at https://github.com/coecms/coecms-util/fork

Changes need to be done in a new branch as a pull request::

    git checkout -b my-branch

    # Make changes...

    git push --set-upstream origin my-branch

Then make a pull request at https://github.com/coecms/coecms-util/pull/new

Please make sure code changes include tests and documentation

Changes must be reviewed by someone in the CMS team before they are committed to master
