How to integrate with TOM Toolkit
==================================

This assumes that you know how to run a TOM server/site using the
`TOM Toolkit <https://tom-toolkit.readthedocs.io/en/stable/>`__

#.  Clone this repo (``git clone https://github.com/mwvgroup/tom_pittgoogle.git``)
    and put the directory on your path.

#.  Add Pitt-Google to your TOM. Follow the TOM Toolkit instructions in the section
    `Using Our New Alert Broker <https://tom-toolkit.readthedocs.io/en/stable/brokers/create_broker.html#using-our-new-alert-broker>`__.
    Our modules were written following the instructions preceding that section.

    -   In your ``settings.py`` file:

        -   Add these to the ``TOM_ALERT_CLASSES`` list:

            .. code::

                'tom_pittgoogle.broker_stream_rest.BrokerStreamRest',
                'tom_pittgoogle.broker_stream_python.BrokerStreamPython',
                'tom_pittgoogle.broker_database_python.BrokerDatabasePython',

        -   Add these additional settings:

            .. code:: python

                # see the Authentication docs
                GOOGLE_CLOUD_PROJECT = "pitt-broker-user-project"  # user's project
                PITTGOOGLE_OAUTH_CLIENT_ID = os.getenv("PITTGOOGLE_OAUTH_CLIENT_ID")
                PITTGOOGLE_OAUTH_CLIENT_SECRET = os.getenv("PITTGOOGLE_OAUTH_CLIENT_SECRET")


#.  After running `makemigrations`, etc. and authenticating yourself,
    navigate to the "Alerts" page on your TOM site. You should see three
    new broker options corresponding to the three Pitt-Google classes
    you added to the ``TOM_ALERT_CLASSES`` list.
