
blockchain.atomicals.get_status
=================================

Return the status of an atomical such as location, owner, block, recent transfer history, content-type, content-length, etc :ref:`atomical id
<atomical ids>`. Can be queried by mint outpoint or the atomical inscription number.

**Signature**

  .. function:: blockchain.atomicals.get_status(mint_outpoint | atomical_number)
  .. versionadded:: 1.6

  *mint_outpoint*

    The mint outpoint is the location where the atomical was created (txid + index: 32 bytes followed by 'i' + index number) .

  *atomical_number*

    The ordinal position of the atomical since inception of the atomicals protocol.

**Result**

  The complete information about the current status of an atomical including recent transfer history.

**Result Example**

::
  [
    {
      "atomical_id": "8431dfd074d52eba0e8d0ae93d544306ee53547ecb91b0c72adbc51592bd603ai0",
      "atomical_number": 1,
      "location_info": {
        "location": "8431dfd074d52eba0e8d0ae93d544306ee53547ecb91b0c72adbc51592bd603ai0",
        "txid": "8431dfd074d52eba0e8d0ae93d544306ee53547ecb91b0c72adbc51592bd603a",
        "index": 0,
        "scripthash": "ad5e6bc8af0c709ad29dea38a3d5f0e96d33bb470fb5cce328b4134ea3b5812a",
        "value": 3000,
        "script": "512009034ea14147937a1fa23c8afe754170ce9ea34571aadd27e29e982d94f06b12"
      },
      "mint_info": {
        "txid": "8431dfd074d52eba0e8d0ae93d544306ee53547ecb91b0c72adbc51592bd603a",
        "input_index": 0,
        "index": 0,
        // The following blockhash, blockheader, and height will be empty if the transaction is still in the mempool
        "blockheader": "000000204caa2a2fd7f9290b1f47dcb2b4f62b5e93fae2a71ca902000000000000000000332389bce7d72d5cbaab250fee9af75ef01c7fb54a80b64aeb9af776a93962522ca823643e020617f696a170",
        "blockhash": "d56fdeceb71e5a4c030d208b7946dc5ba1889152816a02000000000000000000",
        "height": 782964,
        "scripthash": "ad5e6bc8af0c709ad29dea38a3d5f0e96d33bb470fb5cce328b4134ea3b5812a",
        "script": "512009034ea14147937a1fa23c8afe754170ce9ea34571aadd27e29e982d94f06b12",
        "value": 3000,
        "data": {
          "_": {
            "body": "776f726c64",
            "content_length": 5,
            "content_type": "text/plain; charset=utf-8"
          }
        }
      },
      "state_info": {},
      "history": [
        {
          "tx_hash": "8431dfd074d52eba0e8d0ae93d544306ee53547ecb91b0c72adbc51592bd603a",
          "height": 782964
        }
      ]
    }
  ]

blockchain.atomicals.listall
=================================

Return the confirmed atomicals in reverse chronological order.

**Signature**

  .. function:: blockchain.atomicals.listall(offset, limit)
  .. versionadded:: 1.6

  *offset*

    The offset position 

  *limit*

    Return number of atomicals. Max 100.

**Result**

  A feed of atomicals - list of confirmed transactions in blockchain order, with array of 
  outputs of :func:`blockchain.atomicals.get_status`.  

  See :func:`blockchain.atomicals.get_status` for the format of each record.

**Result Examples**

::
{
  offset: 0,
  limit: 20,
  items: [
    {
      // Format of `blockchain.atomicals.get_status`
    },
  ]
}

blockchain.atomicals.listunspent
=================================

Return an ordered list of atomicals UTXOs sent to a script hash.

**Signature**

  .. function:: blockchain.atomicals.listunspent(scripthash)
  .. versionadded:: 1.6

  *scripthash*

    The script hash as a hexadecimal string.

**Result**

  A list of atomicals unspent outputs in blockchain order.  This function takes
  the mempool into account.  Mempool transactions paying to the
  address are included at the end of the list in an undefined order.
  Any output that is spent in the mempool does not appear.  Each
  output is a dictionary with the following keys:

  * *height*

    The integer height of the block the transaction was confirmed in.
    ``0`` if the transaction is in the mempool.

  * *tx_pos*

    The zero-based index of the output in the transaction's list of
    outputs.

  * *tx_hash*

    The output's transaction hash as a hexadecimal string.

  * *value*

    The output's value in minimum coin units (satoshis).

**Result Example**

::

  [
    {
      "tx_pos": 0,
      "value": 45318048,
      "tx_hash": "9f2c45a12db0144909b5db269415f7319179105982ac70ed80d76ea79d923ebf",
      "height": 437146,
      "atomical_id": "9f2c45a12db0144909b5db269415f7319179105982ac70ed80d76ea79d923ebfi0",
      "atomical_id_hex": "9f2c45a12db0144909b5db269415f7319179105982ac70ed80d76ea79d923ebf01000000"
    },
    {
      "tx_pos": 0,
      "value": 919195,
      "tx_hash": "3d2290c93436a3e964cfc2f0950174d8847b1fbe3946432c4784e168da0f019f",
      "height": 441696,
      "atomical_id": "9f2c45a12db0144909b5db269415f7319179105982ac70ed80d76ea79d923ebfi0",
      "atomical_id_hex": "9f2c45a12db0144909b5db269415f7319179105982ac70ed80d76ea79d923ebf01000000"
    }
  ]
 
blockchain.atomicals.get_data
=================================

Return the data of an atomical of all files :ref:`atomical id
<atomical ids>`. Can be queried by mint outpoint or the atomical inscription number.

**Signature**

  .. function:: blockchain.atomicals.get_data(mint_outpoint | atomical_number)
  .. versionadded:: 1.6

  *mint_outpoint*

    The mint outpoint is the location where the atomical was created (txid + index: 32 bytes followed by 'i' + index number) .

  *atomical_number*

    The ordinal position of the atomical since inception of the atomicals protocol.

**Result**

  The complete data of the minted files of an atomical

**Result Example**

::  
  {
    "_": {
      "ct": "text/plain",
      "d": "bytes..."
    },
    "myimage": {
      "ct": "image/jpeg",
      "d": "bytes..."
    }
  }
