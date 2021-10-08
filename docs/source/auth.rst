Authentication
================

Users authenticate themselves by following an OAuth 2.0 workflow.
Authentication is required to make API calls.

.. contents::
   :local:
   :depth: 1

Requirements
-------------

1.  The user must have a Google account (e.g., Gmail address) that is authorized make
    API calls through the project that is defined by the ``GOOGLE_CLOUD_PROJECT``
    variable in the Django ``settings.py`` file.
    Any project can be used, as long as the user is authorized.

    -   We have a test project setup that we are happy to add community members to,
        for as long as that remains feasible.
        Send Troy a request, and include your Google account info (Gmail address).

2.  Since this is still in dev: Contact Troy to be added to the OAuth's list of
    authorized test users, and to obtain the
    ``PITTGOOGLE_OAUTH_CLIENT_ID`` and ``PITTGOOGLE_OAUTH_CLIENT_SECRET``.
    Include your Google account info (Gmail address).

Authentication Workflow
------------------------

Note: Currently this is a bit painful because the user must:

-   re-authenticate every time a query is run.

-   interact via the command line. When running a query from the TOM site's
    "Query a Broker" page, **the process will hang until the user follows the prompts on
    the command line and completes the authentication**. The site may temporarily
    crash until this is completed.

(TODO: integrate the OAuth with Django, and automatically refresh tokens)

**Workflow** - The user will:

#.  Visit a URL, which will be displayed on the command line when the `Consumer`
    class is initialized (currently, when the `Broker`'s ``fetch_alerts`` is called).

#.  Log in to their Google account. This authenticates their access to make API calls
    through the project.

#.  Authorize this `PittGoogleConsumer` app/module to make API calls on their behalf.
    This only needs to be done once for each API access "scope"
    (Pub/Sub, BigQuery, and Logging).

#.  Respond to the prompt on the command line by entering the full URL of the webpage
    they are redirected to after completing the above.

**What happens next?** - The `Consumer`:

#.  Completes the instantiation of an ``OAuth2Session``,
    which is used to either make HTTP requests directly, or instantiate a credentials
    object for the Python client.

#.  Instantiates a ``Client`` object to make API calls with (Python methods only).

#.  Checks that it can successfully connect to the requested resource.
