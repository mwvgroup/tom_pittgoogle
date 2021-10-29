Map a `stream.models.Alert` to DESC's schema for ELAsTiCC
=================================================================

.. list-table:: Schema Fields
    :class: tight-table
    :widths: 35 35 20
    :header-rows: 1

    * - `ELAsTiCC <https://docs.google.com/presentation/d/1FwOdELG-XgdNtySeIjF62bDRVU5EsCToi2Svo_kXA50/edit#slide=id.ge52201f94a_0_6>`__
      - `Alert <https://github.com/LSSTDESC/tom_desc/blob/u/tjr/alert_model/stream/models.py>`__
      - `ZTF <https://zwickytransientfacility.github.io/ztf-avro-alert/schema.html>`__

    * -
      - topic (broker topic)
      -

    * -
      - events (?)
      -

    * - alertId (long int. Unique per broker?)
      - identifier? (str)
      -

    * -
      - coordinates
      -

    * -
      - parsed_message (alert as a dict)
      -

    * -
      - raw_message (alert as bytes? full message packet? don't care?)
      -

    * -
      - parsed (bool)
      -

    * -
      - timestamp
      - ZTF's publish timestamp in message attribute

    * -
      - broker_ingest_timestamp
      -

    * -
      - broker_publish_timestamp
      -

    * -
      - created (DESC ingestion timestamp, auto-added)
      -

    * - classifierNames (string)
      - classifierNames (string. Extra)
      -

    * - classifications
      - (none?)
      -

    * - 10 (float)
      - Bogus (float. Extra)
      -

    * - 20 (float)
      - Real (float. Extra)
      -

    * - 11120 (float)
      - SN-like (float. Extra)
      -

    * - 111120 (float)
      - Ia (float. Extra)
      -

    * - (... etc, all classes)
      - (...)
      - (...)

    * - (?)
      - (?)
      - candid (long int. DIA source ID, unique per alert.)

    * - (?)
      - (?)
      - objectId (string. DIA object ID)
