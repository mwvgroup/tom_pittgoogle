Basic Code Workflow
====================

.. code:: python

    from consumer_stream_rest import ConsumerStreamRest

    consumer = ConsumerStreamRest(subscription_name)

    # TODO: make this better

    response = consumer.oauth2.post(
        f"{consumer.subscription_url}:pull", data={"maxMessages": max_messages},
    )

    alerts = consumer.unpack_and_ack_messages(
        response, lighten_alerts=True, callback=user_filter,
    )  # List[dict]
