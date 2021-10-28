Map a `tom_targets.models.Target` to DESC's schema for ELAsTiCC
=================================================================

.. list-table:: Schema Fields
    :class: tight-table
    :widths: 35 35 20
    :header-rows: 1

    * - `ELAsTiCC <https://docs.google.com/presentation/d/1FwOdELG-XgdNtySeIjF62bDRVU5EsCToi2Svo_kXA50/edit#slide=id.ge52201f94a_0_6>`__
      - `Target <https://tom-toolkit.readthedocs.io/en/stable/api/tom_targets/models.html>`__
        (`source <https://github.com/TOMToolkit/tom_base/blob/main/tom_targets/models.py>`__)
      - `ZTF <https://zwickytransientfacility.github.io/ztf-avro-alert/schema.html>`__

    * - alertId (long int. Unique per broker?)
      - name (str?)
      - (none)

    * - (?)
      - (?)
      - candid (long int. DIA source ID, unique per alert.)

    * - (?)
      - (?)
      - objectId (string. DIA object ID)

    * - (timestamps?)
      - (?)
      - (?)

    * - classifierNames (string)
      - classifierNames (string. Extra)
      - (none)

    * - classifications
      - (none?)
      - (none)

    * - 10 (float)
      - Bogus (float. Extra)
      - (none)

    * - 20 (float)
      - Real (float. Extra)
      - (none)

    * - 11120 (float)
      - SN-like (float. Extra)
      - (none)

    * - 111120 (float)
      - Ia (float. Extra)
      - (none)

    * - (... etc, all classes)
      - (...)
      - (...)
