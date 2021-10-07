.. tom_pittgoogle documentation master file, created by
   sphinx-quickstart on Wed Oct  6 01:30:01 2021.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to tom_pittgoogle's documentation!
==========================================

This branch demonstrates integration with the TOM Toolkit three ways:

.. toctree::
   :maxdepth: 1

   stream_rest
   database_python

How to use
----------------

1.  Run a TOM using the
    [TOM Toolkit](https://tom-toolkit.readthedocs.io/en/stable/)

2.  Add the following to your TOM's `settings.py`

.. code:: python

    # TODO: fill in

3.  Run `makemigrations`, etc. and authenticate yourself.

4.  Navigate to the "Alerts" page on your TOM and query Pitt-Google
    using one of the three broker options.

Authentication
----------------

Uses OAuth 2.0.

The user will need to visit a URL and authorize `PittGoogleConsumer` to make
API calls on their behalf.

The user must have a Google account that is authorized make API calls
through the project defined by the `GOOGLE_CLOUD_PROJECT` variable in the
Django `settings.py` file. Any project can be used.

Additional requirement because this is still in dev: The OAuth is restricted
to users registered with Pitt-Google, so contact us.

TODO: Integrate this with Django. For now, the user interacts via command line.
