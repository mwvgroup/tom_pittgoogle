.. tom_pittgoogle documentation master file, created by
   sphinx-quickstart on Wed Oct  6 01:30:01 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Pitt-Google integration with TOM Toolkit
=====================================================

This repo contains 3 proof-of-concept implementations of a TOM Toolkit ``GenericBroker``
class which fetch alerts from Pitt-Google.

Contact Troy Raen with questions or for authentication access
(Slack @troyraen, or troy.raen@pitt.edu).

.. list-table:: 3 methods
    :class: tight-table
    :widths: 20 15 20 45
    :header-rows: 1

    * - Method
      - Connects to
      - Via
      - Comments

    * - `StreamRest`
      - Pub/Sub streams
      - REST API
      - Closest to "standard" implementation using HTTP requests.
        Uses batch-style message pulls.

    * - `StreamPython`
      - Pub/Sub streams
      - Python client
      - **Recommended** for listening to a full night's stream. Uses a streaming pull.

    * - `DatabasePython`
      - BigQuery database
      - Python client
      -

Each method relies on 2 classes, a `Broker` and a `Consumer`:

.. list-table:: 2 classes for each method
    :class: tight-table
    :widths: 40 60
    :header-rows: 1

    * - `Broker`
      - `Consumer`

    * - - Fetches alerts from Pitt-Google using a `Consumer`
      - - Handles the database/stream connections and unpacks the returned data.

    * - - Base class: ``tom_alerts.alerts.GenericBroker``
      - - Python methods use Google's client APIs
          (`Pub/Sub <https://googleapis.dev/python/pubsub/latest/index.html>`__,
          `BigQuery <https://googleapis.dev/python/bigquery/latest/index.html>`__)
        - REST method uses a ``requests_oauthlib.OAuth2Session`` object for HTTP
          requests

Here we use `Broker` and `Consumer` generically to refer to any of the specific
implementations, which have names like ``BrokerStreamRest``.


.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: Overview

   Basics<self>
   workflow
   integrate_tom
   auth

.. toctree::
   :hidden:
   :maxdepth: 1
   :caption: API

   stream_rest
   database_python
