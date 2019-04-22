===============================
NKOD TSA
===============================

.. image:: https://img.shields.io/github/release-pre/eghuro/nkod-ts.svg
     :alt: GitHub release

.. image:: https://img.shields.io/travis/com/eghuro/nkod-ts.svg
     :target: https://travis-ci.com/eghuro/nkod-ts

.. image:: https://badges.renovateapi.com/github/eghuro/nkod-ts
     :alt: Renovate

.. image:: https://img.shields.io/codeclimate/maintainability/eghuro/nkod-ts.svg
     :alt: Code Climate maintainability

.. image:: https://img.shields.io/github/license/eghuro/nkod-ts.svg
     :target: https://github.com/eghuro/nkod-ts/blob/master/LICENSE
     :alt: License



Indexing linked data and relationships between datasets.

Features:
 - index a distribution or a SPARQL endpoint
 - extract and index distributions from a DCAT catalog
 - extract a DCAT catalog from SPARQL endpoint and index distributions from it
 - generate a dataset profile
 - show related datasets based mainly on DataCube and SKOS vocabularies


Build & run with Docker
----------

For NKOD-TS service only:

.. code-block:: bash

    docker build . -t nkod-ts
    docker run -p 80:8000 --name nkod-ts nkod-ts

For the full environment use docker-compose:

.. code-block:: bash

    docker-compose up --build

Build & run manually
----------
CPython 3.6+ is supported.

Install redis server first. In following example we will assume it runs on localhost, port 6379 and DB 0 is used.

Run the following commands to bootstrap your environment ::

    git clone https://github.com/eghuro/nkod-ts
    cd nkod-ts
    pip install -r requirements.txt
    # Start redis server
    # Run concurrently
    REDIS=redis://localhost:6379/0 celery worker -l info -A tsa.celery
    REDIS=redis://localhost:6379/0 gunicorn -k gevent -w 4 -b 0.0.0.0:8000 autoapp:app

In general, before running shell commands, set the ``FLASK_APP`` and
``FLASK_DEBUG`` environment variables ::

    export FLASK_APP=autoapp.py
    export FLASK_DEBUG=1


Deployment
----------

To deploy::

    export FLASK_DEBUG=0
    # Start redis server
    # Run concurrently
    REDIS=redis://redis:6379/0 celery worker -l info -A tsa.celery --pool gevent --concurrency=500
    REDIS=redis://redis:6379/0 gunicorn -k gevent -w 4 -b 0.0.0.0:8000 autoapp:app

In your production environment, make sure the ``FLASK_DEBUG`` environment
variable is unset or is set to ``0``, so that ``ProdConfig`` is used.


Shell
-----

To open the interactive shell, run ::

    flask shell

By default, you will have access to the flask ``app``.


Running Tests
-------------

To run all tests, run ::

    flask test
