===============================
NKOD TSA
===============================

Time series analysis service for NKOD.


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

Only Python 3.6 is supported, 3.7 and higher are not yet supported because of Celery. Support for 3.7+ is expected with Celery 4.3 release.

First, set your app's secret key as an environment variable. For example,
add the following to ``.bashrc`` or ``.bash_profile``.

.. code-block:: bash

    export NKOD_TSA_SECRET='something-really-secret'

Run the following commands to bootstrap your environment ::

    git clone https://github.com/eghuro/crawlcheckio
    cd crawlcheckio
    pip install -r requirements.txt
    npm install
    npm run start-dev  # run the webpack dev server and flask server using concurrently

You will see a pretty welcome screen.

In general, before running shell commands, set the ``FLASK_APP`` and
``FLASK_DEBUG`` environment variables ::

    export FLASK_APP=autoapp.py
    export FLASK_DEBUG=1


Deployment
----------

To deploy::

    export FLASK_DEBUG=0
    npm start       # build assets with webpack and start gunicorn server

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
