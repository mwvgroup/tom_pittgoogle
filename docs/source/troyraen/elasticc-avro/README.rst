
Create a Pub/Sub stream with ELAsTiCC's Avro schema
===================================================

Reference links:


* `pubsub-create-avro-schema <https://cloud.google.com/pubsub/docs/samples/pubsub-create-avro-schema>`_
* `validating schemas <https://cloud.google.com/pubsub/docs/schemas#validating>`_
* `pubsub-create-topic-with-schema <https://cloud.google.com/pubsub/docs/samples/pubsub-create-topic-with-schema>`_
* `pubsub-publish-avro-records <https://cloud.google.com/pubsub/docs/samples/pubsub-publish-avro-records>`_
* `pubsub-subscribe-avro-records <https://cloud.google.com/pubsub/docs/samples/pubsub-subscribe-avro-records>`_

Setup
-----

.. code-block:: python

   import os
   import fastavro
   from google.cloud.pubsub import SchemaServiceClient
   from google.pubsub_v1.types import Schema

   # broker_utils.gcp_utils uses pubsub v1.x but we need v2.x.
   # Import from troy/python_fncs
   from python_fncs import pubsub as gcp_utils  

   project_id = os.getenv('GOOGLE_CLOUD_PROJECT')
   schema_id = "elasticc-schema"
   topic_id = "elasticc"  # schema attached to topic, not in msg header
   topic_id_msghdr = "elasticc-header"  # schema in msg header, not attached to topic

   avsc_file = "/Users/troyraen/Documents/broker/desc-plastic/Examples/plasticc_schema/lsst.v4_1.brokerClassification.avsc"
   test_msg_file = "test_classification_alert.avro"

Create the schema in GCP
------------------------

.. code-block:: python

   project_path = f"projects/{project_id}"

   # Read a JSON-formatted Avro schema file as a string.
   with open(avsc_file, "rb") as f:
       avsc_source = f.read().decode("utf-8")

   # create the schema
   schema_client = SchemaServiceClient()
   schema_path = schema_client.schema_path(project_id, schema_id)
   schema = Schema(name=schema_path, type_=Schema.Type.AVRO, definition=avsc_source)
   result = schema_client.create_schema(
       request={"parent": project_path, "schema": schema, "schema_id": schema_id}
   )
   # schema_client.delete_schema(request={"name": schema_path})
   # schema_in = schema_client.get_schema(request={"name": schema_path})

Create topics (one with schema attached, one without); create subscriptions
---------------------------------------------------------------------------

.. code-block:: python

   from google.cloud.pubsub import PublisherClient
   from google.pubsub_v1.types import Encoding

   publisher_client = PublisherClient()

   # topic with schema attached
   topic_path = publisher_client.topic_path(project_id, topic_id)
   encoding = Encoding.BINARY
   publisher_client.create_topic(
       request={
           "name": topic_path,
           "schema_settings": {"schema": schema_path, "encoding": encoding},
       }
   )
   gcp_utils.create_subscription(topic_id)

   # topic without schema attached. schema should be in message header.
   topic_path_msghdr = publisher_client.topic_path(project_id, topic_id_msghdr)
   publisher_client.create_topic(request={"name": topic_path_msghdr,})
   gcp_utils.create_subscription(topic_id_msghdr)

Create a fake alert and publish it
----------------------------------

.. code-block:: python

   fastavro_schema = fastavro.schema.load_schema(avsc_file)

   # create the data packet
   alertId = 123456789
   bogus, real = 0.1, 0.9
   prob_SN, prob_Ia = 0.8, 0.6

   class_dict = {
       "alertId": alertId,
       "classifierNames": "RealBogus_v0.1, SuperNNova_v1.3",   # comma-separated string
       "classifications": {                                    # dict with a single item
           "classificationDict":                               # list of dicts
               [
                   {"10":      bogus},
                   {"20":      real},
                   {"11120":   prob_SN},
                   {"111120":  prob_Ia},
               ]
       }
   }

   # simplify the schema
   class_dict_alt = {
       "alertId": alertId,
       "classifierNames": "RealBogus_v0.1, supernnova_v1.3",   # comma-separated string
       "classifications": [                                    # list of dicts
           {"10":      bogus},
           {"20":      real},
           {"11120":   prob_SN},
           {"111120":  prob_Ia},
       ]
   }

   # simplify again
   class_dict_alt2 = {
       "alertId": alertId,
       "classifierNames": "RealBogus_v0.1, supernnova_v1.3",   # comma-separated string
       "classifications": {                                    # dict
           "10":       bogus,
           "20":       real,
           "11120":    prob_SN,
           "111120":   prob_Ia,
       }
   }

   # publish message to topic with schema attached
   fout = io.BytesIO()
   fastavro.schemaless_writer(fout, fastavro_schema, class_dict)
   fout.seek(0)
   data = fout.getvalue()
   future = publisher_client.publish(topic_path, data)

   # publish message to topic without schema attached
   fout = io.BytesIO()
   fastavro.writer(fout, fastavro_schema, [class_dict])
   fout.seek(0)
   data = fout.getvalue()
   future = publisher_client.publish(topic_path_msghdr, data)

   # write to file
   with open(test_msg_file, 'wb') as out:
       fastavro.writer(out, fastavro_schema, records)

Pull the messages
-----------------

.. code-block:: python

   # topic with schema attached
   msgs = gcp_utils.pull_pubsub(topic_id)
   msg = msgs[0]
   byin = io.BytesIO(msg)
   class_dict_in = fastavro.schemaless_reader(byin, fastavro_schema)
   # pubsub_schema = schema_client.get_schema(request={"name": schema_path})
   # pubsub_schema_dict = json.loads(pubsub_schema.definition)
   # class_dict_in = fastavro.schemaless_reader(byin, pubsub_schema_dict)

   # topic without schema attached
   msgs = gcp_utils.pull_pubsub(topic_id_msghdr)
   msg = msgs[0]
   byin = io.BytesIO(msg)
   for record in fastavro.reader(byin):
       class_dict_alt_in = record
       break
