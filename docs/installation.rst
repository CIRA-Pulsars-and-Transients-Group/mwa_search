Installation
============

The following will explain how to install mwa_search.

Simple
------

To run the pipelines contained in the ``nextflow`` directory requires `Nextflow <https://www.nextflow.io/>`_.

The repository's scripts can be installed using ::

    pip install .


With Conda
----------

There can be some installation errors due to installation of mpi4py and incorrect numpy versions.
To avoid this, we recommend using conda to install the package. This can be done by running the following commands::

    conda create --name mwa_search python=3.9
    conda activate mwa_search
    conda config --env --add channels conda-forge
    conda install mpi4py numpy==1.19.5
    pip install .
