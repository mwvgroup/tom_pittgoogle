Basic Code Workflow
====================

Each implementation does things a bit differently, but they share a basic workflow:

    The `Broker` instantiates a `Consumer` and uses it to fetch, unpack, and
    process alerts.

The `Consumer` can accept a user filter and return only alerts that pass.

Here is a compact but working example of a `Broker`'s ``fetch_alerts`` method for the
`StreamRest` implementation.

.. code:: python

    def fetch_alerts(self):
        from consumer_stream_rest import ConsumerStreamRest

        subscription_name = "ztf-loop"
        max_messages = 10
        lighten_alerts = True  # flatten the alert dict and drop extra fields. optional.
        # If you pass a callback function, the Consumer will run each alert through it.
        # Optional. Useful for user filters. Here's a basic demo.
        def user_filter(alert_dict):
            passes_filter = True
            if passes_filter:
                return alert_dict
            else:
                return None
        callback = user_filter

        consumer = ConsumerStreamRest(subscription_name)

        response = consumer.oauth2.post(
            f"{consumer.subscription_url}:pull", data={"maxMessages": max_messages},
        )

        alerts = consumer.unpack_and_ack_messages(
            response, lighten_alerts=lighten_alerts, callback=callback,
        )  # List[dict]

        return iter(alerts)
