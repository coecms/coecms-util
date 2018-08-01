CLEX CMS Utility Library
========================

.. image:: https://circleci.com/gh/coecms/coecms-util/tree/master.svg?style=svg
    :target: https://circleci.com/gh/coecms/coecms-util/tree/master
.. image:: https://codecov.io/gh/coecms/coecms-util/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/coecms/coecms-util
.. image:: https://api.codacy.com/project/badge/Grade/3706e7a283fd439fa8b8d2f707f814e4
    :target: https://www.codacy.com/app/ScottWales/coecms-util

Development
-----------

Create a dev conda environment::

    conda env create -f conda/dev-environment.yml

Install the package::

    pip install -e .

Run the tests::

    py.test

Making Changes
--------------

Changes need to be done in a new branch as a pull request::

    git checkout -b my-branch

    # Make changes...

    git push --set-upstream origin my-branch

Then make a pull request at https://github.com/coecms/coecms-util/pull/new

Please make sure code changes include tests and documentation
