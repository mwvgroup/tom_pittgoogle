Pub/Sub Message Service
=========================

The Pitt-Google broker uses Pub/Sub to produce live alert streams.
Pub/Sub is an asynchronous, publish–subscribe messaging service.
It serves the same function as Apache Kafka.
Here we outline the basics of Pub/Sub and highlight some differences with Kafka.
See Google's `What is Pub/Sub? <https://cloud.google.com/pubsub/docs/overview>`__ for more information.

Pub/Sub is a messaging *service*.
Users can simply publish and subscribe to message streams through the APIs, with no need to run servers or manage the data distribution.

.. Pub/Sub can be used just as easily within or outside of Google Cloud.
.. In addition, most Google Cloud services are directly integrated with Pub/Sub, meaning users can configure subscriptions that automatically trigger event-based processing and pipelines in the Cloud.

Message delivery in Pub/Sub vs. Kafka:

Pub/Sub subscriptions are processed on a per-message basis.
When a subscriber "pulls" a subscription, Pub/Sub leases messages to the subscriber client.
Once the subscriber processes a message, it sends an acknowledgement of success back to Pub/Sub.
If no such acknowledgement is received within the allotted time, Pub/Sub will redeliver the message at some point in the future.
Note that message ordering is not guaranteed\*.

By contrast, Kafka separates topics into partitions, delivers messages to a consumer from a given
partition, in order, and tracks offsets to record the last message sent to the consumer.

Results of these differences:

-   With Pub/Sub, messages only leave the subscription after they have been successfully processed.
    If the client experiences an error while processing a message, the message is not lost (assuming it has not already been acknowledged), and the client does not have to "rewind" the stream to the correct offset to get it back.
    The message will be automatically redelivered.

-   The parallelism of Kafka consumers is limited by the number of partitions in the topic.
    There is no such constraint with Pub/Sub.

.. /* This assumes that the subscription is configured as a "pull" subscription. By contrast, messages in "push" subscriptions will be automatically delivered to the preconfigured endpoint.

\* Pub/Sub does offer a message ordering option, but it is not the default and we have not enabled it on our streams.
