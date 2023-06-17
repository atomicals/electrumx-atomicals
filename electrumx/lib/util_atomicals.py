# Copyright (c) 2023, The Atomicals Developers - atomicals.xyz
# Copyright (c) 2016-2017, Neil Booth
#
# All rights reserved.
#
# The MIT License (MIT)
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# and warranty status of this software.

'''Miscellaneous atomicals utility classes and functions.'''

from array import array
from electrumx.lib.script import OpCodes, ScriptError, Script
from electrumx.lib.util import unpack_le_uint16_from, unpack_le_uint64, unpack_le_uint32, unpack_le_uint32_from, pack_le_uint16, pack_le_uint32
from electrumx.lib.hash import hash_to_hex_str, hex_str_to_hash
import re
import sys
import pickle
from cbor2 import dumps, loads, CBORDecodeError
from collections.abc import Mapping

# Atomical NFT/FT mint information is stored in the b'mi' index and is pickle encoded dictionary
def unpack_mint_info(mint_info_value):
    if not mint_info_value:
        raise IndexError(f'unpack_mint_info mint_info_value is null. Index error.')
    return pickle.loads(mint_info_value)
 
# Get the expected output index of an Atomical NFT
# The 'x' extract operation allows a UTXO, which has multiple Atomicals imprinted on it, to be split (or extracted) apart
def get_expected_output_index_of_atomical_nft(mint_info, tx, atomical_id, atomicals_operations_found):
    assert(mint_info['type'] == 'NFT')  # Sanity check
    if len(mint_info['input_indexes'] > 1):
        raise IndexError(f'get_expected_output_index_of_atomical_nft len is greater than 1. Critical developer or index error. AtomicalId={atomical_id.hex()}')
    # The expected output index is equal to the input index...
    expected_output_index = mint_info['input_indexes'][0]
    # ... unless the 'x' extract operation is used to reassign the Atomical from the 1'st output to the 0'th output.
    # Allow the extract operation only from the 1'st input because it will place the atomical to the 0'th output
    # There should be a key in the dictionary with the key value being the Atomical id to move
    extract_atomical = atomicals_operations_found['op'] == 'x' and atomicals_operations_found['input_index'] == 1 and atomicals_operations_found['payload'].get(atomical_id)
    # Never allow an NFT atomical to be burned accidentally by having insufficient number of outputs either
    # The expected output index will become the 0'th index if the 'x' extract operation was specified or there are insufficient outputs
    if expected_output_index >= len(tx.outputs) or extract_atomical:
        expected_output_index = 0
    return expected_output_index

# Get the expected output indexes of an Atomical FT
def get_expected_output_indexes_of_atomical_ft(mint_info, tx, atomical_id, atomicals_operations_found):
    assert(mint_info['type'] == 'FT') # Sanity check
    expected_output_indexes = []
    remaining_value = mint_info['value']
    # The FT type has the 'skip' (y) method to skip the first output in the assignment of the value of the token
    # Essentially this makes it possible to "split" out multiple FT's located at the same input
    # If any of the inputs has the skip operation, then it will apply for the atomical token generally across all inputs and the first output will be skipped
    skip_first_output = False
    if atomicals_operations_found.get('op') == 'y' and atomicals_operations_found.get('input_index') == 0 and atomicals_operations_found.get('payload') and atomicals_operations_found.get('payload').get(atomical_id):
        skip_first_output = True 

    is_skipped = False  # Used to track if we skipped the first output
    for out_idx, txout in enumerate(tx.outputs): 
        # If the first output should be skipped and we have not yet done so, then skip/ignore it
        if skip_first_output and not is_skipped:
            is_skipped = True
            continue 
        # For all remaining outputs attach colors as long as there is adequate remaining_value left to cover the entire output value
        if txout.value <= remaining_value:
            expected_output_indexes.append(out_idx)
            remaining_value -= txout.value
        else: 
            # Since one of the inputs was not less than or equal to the remaining value, then stop assigning outputs. The remaining coins are burned. RIP.
            break
    return expected_output_indexes

# Check whether the value is a 36 byte hex string
def is_atomical_id_long_form_string(value):
    try:
        int(value, 16) # Throws ValueError if it cannot be validated as hex string
        return True
    except (ValueError, TypeError):
        pass
    return False

# Check whether the value is a 36 byte sequence
def is_atomical_id_long_form_bytes(value):
    try:
        raw_hash = hex_str_to_hash(value)
        if len(raw_hash) == 36:
            return True
    except (ValueError, TypeError):
        pass
    return False

# Check whether the value is a compact form location/atomical id 
def is_compact_atomical_id(value):
    '''Whether this is a compact atomical id or not
    '''
    if isinstance(value, int):
        return False
    if value == None or value == "":
        return False
    index_of_i = value.find("i")
    if index_of_i != 64: 
        return False
    raw_hash = hex_str_to_hash(value[ : 64])
    if len(raw_hash) == 32:
        return True
    return False

# Convert the compact string form to the expanded 36 byte sequence
def compact_to_location_id_bytes(value):
    '''Convert the 36 byte atomical_id to the compact form with the "i" at the end
    '''

    index_of_i = value.index("i")
    if index_of_i != 64: 
        raise TypeError(f'{value} should be 32 bytes hex followed by i<number>')
    
    raw_hash = hex_str_to_hash(value[ : 64])
    
    if len(raw_hash) != 32:
        raise TypeError(f'{value} should be 32 bytes hex followed by i<number>')

    num = int(value[ 65: ])

    if num < 0 or num > 100000:
        raise TypeError(f'{value} index output number was parsed to be less than 0 or greater than 100000')

    return raw_hash + pack_le_uint32(num)
 
# Convert 36 byte sequence to compact form string
def location_id_bytes_to_compact(atomical_id):
    digit, = unpack_le_uint32_from(atomical_id[32:])
    return f'{hash_to_hex_str(atomical_id[:32])}i{digit}'
 
# Get the tx hash from the location/atomical id
def get_tx_hash_index_from_location_id(atomical_id): 
    output_index, = unpack_le_uint32_from(atomical_id[ 32 : 36])
    return atomical_id[ : 32], output_index 

# Check if the operation is a valid distributed mint (dmint) type
def is_valid_dmt_op_format(tx_hash, dmt_op):
    if not dmt_op or dmt_op['op'] != 'dmt' or dmt_op['input_index'] != 0:
        return False, {}
    payload_data = dmt_op['payload']
    metadata = payload_data.get('meta', {})
    if not isinstance(metadata, dict):
        return False, {}
    args = payload_data.get('args', {})
    if not isinstance(params, dict):
        return False, {}
    ticker = params.get('tick', None)
    if is_valid_ticker_string(ticker):
        return True, {
            'payload': payload_data,
            'meta': metadata,
            'args': args,
            '$ticker': ticker
        }
    return False, {}

# Get the mint information structure if it's a valid mint event type
def get_mint_info_op_factory(tx_hash, tx, op_found_struct):
    # Builds the base mint information that's common to all minted Atomicals
    def build_base_mint_info(tx_hash, tx):
        # The first output is always imprinted
        expected_output_index = 0
        txout = tx.outputs[expected_output_index]
        scripthash = double_sha256(txout.pk_script)
        output_idx_le = pack_le_uint32(expected_output_index) 
        location = tx_hash + output_idx_le
        value_sats = pack_le_uint64(txout.value)
        # Create the general mint information
        mint_info = {
            # Establish the atomical_id from the initial location
            'id': location,
            'txid': hash_to_hex_str(tx_hash),
            'index': expected_output_index,
            'scripthash': scripthash,
            'value': txout.value,
            'script': txout.pk_script,
            # The following fields will be added at a different level of processing
            # 'number': atomical_num,  
            # 'header': header, 
            # 'height': height, 
            # 'tx_num': tx_num
        }
    
    # Get the 'meta' and 'args' fields in the payload, or return empty dictionary if not set
    # Enforces that both of these must be empty or a valid dictionary
    # This prevents a user from minting a big data blob into one of the fields
    def populate_args_meta(mint_info, op_found_payload):
        metadata = op_found_payload.get('meta', {})
        if not isinstance(metadata, dict):
            return False
        args = op_found_payload.get('args', {})
        if not isinstance(args, dict):
            return False
        mint_info['args'] = args 
        mint_info['meta'] = meta 
        return True

    # Create the base mint information structure
    mint_info = build_base_mint_info(tx_hash, tx)
    if not populate_args_meta(mint_info, op_found_struct['payload']):
        return None, None
    ############################################
    #
    # Non-Fungible Token (NFT) Mint Operations
    #
    ############################################
    if op_found_struct['op'] == 'nft' and op_found_struct['input_index'] == 0:
        mint_info['type'] = 'NFT'
        mint_info['subtype'] = 'base'
    elif op_found_struct['op'] == 'co' and op_found_struct['input_index'] == 0:
        mint_info['type'] = 'NFT'
        mint_info['subtype'] = 'container'
        container = mint_info['args'].get('container', None)
        if not isinstance(container, str):
            return None, None
        if not is_valid_container_string_name(container):
            return None, None
        mint_info['$container'] = container
    elif op_found_struct['op'] == 'rlm' and op_found_struct['input_index'] == 0:
        mint_info['type'] = 'NFT'
        mint_info['subtype'] = 'realm'
        realm = mint_info['args'].get('realm', None)
        if not isinstance(realm, str):
            return None, None
        if not is_valid_realm_string(realm):
            return None, None
        mint_info['$realm'] = realm
    elif op_found_struct['op'] == 'sub' and op_found_struct['input_index'] == 0:
        mint_info['type'] = 'NFT'
        mint_info['subtype'] = 'subrealm'
        subrealm = mint_info['args'].get('subrealm', None)
        if not isinstance(subrealm, str):
            return None, None
        if not is_valid_realm_string(subrealm):
            return None, None

        # The parent realm id is in a compact form string to make it easier for users and developers
        parent_realm_id = mint_info['args'].get('pid')
        if not isinstance(parent_realm_id, str):
            return None, None
        if not is_compact_atomical_id(parent_realm_id):
            return None, None

        mint_info['$subrealm'] = subrealm
        # Save in the compact form to make it easier to understand for developers and users
        # It requires an extra step to convert, but it makes it easier to understand the format
        mint_info['$parent_realm_id_compact'] = parent_realm_id
        # Decode the compact form and make it available in the mint info
        mint_info['$parent_realm_id'] = compact_to_location_id_bytes(parent_realm_id)
    ############################################
    #
    # Fungible Token (FT) Mint Operations
    #
    ############################################
    elif op_found_struct['op'] == 'ft' and op_found_struct['input_index'] == 0:
        mint_info['type'] = 'FT'
        mint_info['subtype'] = 'base'
        ticker = mint_info['args'].get('tick', None)
        if not is_valid_ticker_string(ticker):
            return None, None
        mint_info['$ticker'] = ticker
    elif op_found_struct['op'] == 'dft' and op_found_struct['input_index'] == 0:
        mint_info['type'] = 'FT'
        mint_info['subtype'] = 'distributed'
        ticker = mint_info['args'].get('tick', None)
        if not is_valid_ticker_string(ticker):
            return None, None
        mint_info['$ticker'] = ticker
        mint_height = mint_info['args'].get('h', None)
        if not isinstance(mint_height, int) or mint_height < 0 or mint_height > 10000000:
            print(f'DFT mint has invalid mint_height h {tx_hash}, {mint_height}. Skipping...')
            return None, None
        mint_amount = mint_info['args'].get('amt', None)
        if not isinstance(mint_amount, int) or mint_amount <= 0 or mint_amount > 10000000000:
            print(f'DFT mint has invalid mint_amount amt {tx_hash}, {mint_amount}. Skipping...')
            return None, None
        max_mints = mint_info['args'].get('cnt', None)
        if not isinstance(max_mints, int) or max_mints <= 0 or max_mints > 1000000:
            print(f'DFT mint has invalid max_mints cnt {tx_hash}, {max_mints}. Skipping...')
            return None, None
        # Do not mint because at least one is a zero
        if mint_amount <= 0 or max_mints <= 0:
            self.logger.info(f'FT mint has zero quantities {tx_hash}, {mint_amount}. Skipping...')
            return None, None
        mint_info['$mint_height'] = mint_height
        mint_info['$mint_amount'] = mint_amount
        mint_info['$max_mints'] = max_mints
    
    if not mint_info:
        return None, None
 
    return mint_info['type'], mint_info
    
# A valid ticker string must be at least 3 characters and max 10 with a-z0-9
def is_valid_ticker_string(ticker):
    if not ticker:
        return None 
    tolower = ticker.lower()
    m = re.compile(r'^[a-z0-9]{3,10}$')
    if m.match(tolower):
        return True
    return False 

# A valid realm string must begin with a-z and have up to 63 characters after it 
# Including a-z0-9 and hypohen's "-"
def is_valid_realm_string_name(realm_name):
    if not realm_name:
        return None 
    tolower = realm_name.lower()
    # Realm names must start with an alphabetical character
    m = re.compile(r'^[a-z][a-z0-9\-]{0,63}$')
    if m.match(tolower):
        return True
    return False 

# A valid subrealm string must begin with a-z and have up to 63 characters after it 
# Including a-z0-9 and hypohen's "-"
def is_valid_subrealm_string_name(subrealm_name):
    if not subrealm_name:
        return None 
    tolower = subrealm_name.lower()
    # SubRealm names can start with a number also, unlike top-level-realms 
    m = re.compile(r'^[a-z0-9]|[a-z0-9\-]{0,63}$')
    if m.match(tolower):
        return True
    return False 

# Collections must be at least 1 letter and max 64 with a-z0-9 and hypohen's "-"
def is_valid_container_string_name(container_name):
    if not container_name:
        return None 
    tolower = container_name.lower()
    # Collection names can start with any type of character
    m = re.compile(r'^[a-z0-9\-]{1,64}$')
    if m.match(tolower):
        return True
    return False 

# Parses the push datas from a bitcoin script byte sequence
def parse_push_data(op, n, script):
    data = b''
    if op <= OpCodes.OP_PUSHDATA4:
        # Raw bytes follow
        if op < OpCodes.OP_PUSHDATA1:
            dlen = op
        elif op == OpCodes.OP_PUSHDATA1:
            dlen = script[n]
            n += 1
        elif op == OpCodes.OP_PUSHDATA2:
            dlen, = unpack_le_uint16_from(script[n: n + 2])
            n += 2
        elif op == OpCodes.OP_PUSHDATA4:
            dlen, = unpack_le_uint32_from(script[n: n + 4])
            n += 4
        if n + dlen > len(script):
            raise IndexError
        data = script[n : n + dlen]
    return data, n + dlen, dlen

# Parses all of the push datas in a script and then concats/accumulates the bytes together
# It allows the encoding of a multi-push binary data across many pushes
def parse_atomicals_data_definition_operation(script, n):
    '''Extract the payload definitions'''
    accumulated_encoded_bytes = b''
    try:
        script_entry_len = len(script)
        while n < script_entry_len:
            op = script[n]
            n += 1
            # define the next instruction type
            if op == OpCodes.OP_ENDIF:
                break
            elif op <= OpCodes.OP_PUSHDATA4:
                data, n, dlen = parse_push_data(op, n, script)
                accumulated_encoded_bytes = accumulated_encoded_bytes + data
        return accumulated_encoded_bytes
    except Exception as e:
        raise ScriptError(f'parse_atomicals_data_definition_operation script error {e}') from None

# Parses the valid operations in an Atomicals script
def parse_operation_from_script(script, n):
    '''Parse an operation'''
    # Check for each protocol operation
    script_len = len(script)
    atom_op_decoded = None
    one_letter_op_len = 2
    two_letter_op_len = 3
    three_letter_op_len = 4

    # check the 3 letter protocol operations
    if n + three_letter_op_len < script_len:
        atom_op = script[n : n + three_letter_op_len].hex()
        print('atom op')
        print(atom_op)
        if atom_op == "036e6674":
            atom_op_decoded = 'nft'  # nft - CreMintate non-fungible token
        elif atom_op == "03646674":  
            atom_op_decoded = 'dft'  # dft - Deploy distributed mint fungible token starting point
        elif atom_op == "03637274":  
            atom_op_decoded = 'crt'  # crt - Define contract state
        elif atom_op == "036d6f64":  
            atom_op_decoded = 'mod'  # mod - Modify general state
        elif atom_op == "03737562":  
            atom_op_decoded = 'sub'  # sub - Create Sub-Realm
        elif atom_op == "03657674": 
            atom_op_decoded = 'evt'  # evt - Message response/reply
        elif atom_op == "03726c6d": 
            atom_op_decoded = 'rlm'  # rlm - Create Realm (top-level-realm TLR)
        elif atom_op == "03646d74": 
            atom_op_decoded = 'dmt'  # dmt - Mint tokens of distributed mint type (dft)
        
        if atom_op_decoded:
            return atom_op_decoded, parse_atomicals_data_definition_operation(script, n + three_letter_op_len)
    
    # check the 2 letter protocol operations
    if n + two_letter_op_len < script_len:
        atom_op = script[n : n + two_letter_op_len].hex()
        if atom_op == "026674":
            atom_op_decoded = 'ft'  # ft - fungible token mint
        elif atom_op == "02636f":  
            atom_op_decoded = 'co'  # co - Container or collection type mint
        elif atom_op == "02736c":  
            atom_op_decoded = 'sl'  # sl - Seal an NFT and lock it from further changes forever
        
        if atom_op_decoded:
            return atom_op_decoded, parse_atomicals_data_definition_operation(script, n + two_letter_op_len)
    
    # check the 1 letter
    if n + one_letter_op_len < script_len:
        atom_op = script[n : n + one_letter_op_len].hex()
        # Extract operation (for NFTs only)
        if atom_op == "0178":
            atom_op_decoded = 'x'  # extract - move atomical to 0'th output
        # Skip operation (for FTs only)
        elif atom_op == "0179":
            atom_op_decoded = 'y'  # skip - skip first output for fungible token transfer
        
        if atom_op_decoded:
            return atom_op_decoded, parse_atomicals_data_definition_operation(script, n + one_letter_op_len)
    
    print(f'Invalid Atomicals Operation Code. Skipping... "{script[n : n + 4].hex()}"')
    return None, None

# Parses and detects valid Atomicals protocol operations in a witness script
# Stops when it finds the first operation in the first input
def parse_protocols_operations_from_witness_for_input(txinwitness):
    '''Detect and parse all operations across the witness input arrays from a tx'''
    atomical_operation_type_map = {}
    for script in txinwitness:
        n = 0
        script_entry_len = len(script)
        if script_entry_len < 39 or script[0] != 0x20:
            continue
        found_operation_definition = False
        while n < script_entry_len - 5:
            op = script[n]
            n += 1
            # Match the pubkeyhash
            if op == 0x20 and n + 32 <= script_entry_len:
                n = n + 32
                while n < script_entry_len - 5:
                    op = script[n]
                    n += 1 
                    # Get the next if statement    
                    if op == OpCodes.OP_IF:
                        # spr3 / atom
                        if "0473707233" == script[n : n + 5].hex():
                            found_operation_definition = True
                            # Parse to ensure it is in the right format
                            operation_type, payload = parse_operation_from_script(script, n + 5)
                            if operation_type != None:
                                print(f'Potential Atomicals Operation Code Found: {operation_type}')
                                return operation_type, payload
                            break
                if found_operation_definition:
                    break
            else:
                break
    return None, None

# Parses and detects the witness script array and detects the Atomicals operations
def parse_protocols_operations_from_witness_array(tx):
    '''Detect and parse all operations of atomicals across the witness input arrays (inputs 0 and 1) from a tx'''
    if not hasattr(tx, 'witness'):
        return {}
    txin_idx = 0
    for txinwitness in tx.witness:
        # All inputs are parsed but further upstream most operations will only function if placed in the 0'th input
        # The exception is the 'x' extract operation which will only function correctly if placed in the 1'st input
        op_name, payload = parse_protocols_operations_from_witness_for_input(txinwitness)
        if not op_name:
            continue 
        decoded_object = {}
        if payload: 
            # Ensure that the payload is cbor encoded dictionary or empty
            try:
                decoded_object = loads(payload)
                if not isinstance(decoded_object, dict):
                    print(f'parse_protocols_operations_from_witness_array found {op_name} but decoded CBOR payload is not a dict for {tx}. Skipping tx input...')
                    continue
            except: 
                print(f'parse_protocols_operations_from_witness_array found {op_name} but CBOR payload parsing failed for {tx}. Skipping tx input...')
                continue
            # Return immediately at the first successful parse of the payload
            # It doesn't mean that it will be valid when processed, because most operations require the txin_idx=0 
            # Nonetheless we return it here and it can be checked uptstream

            # Special care must be taken that someone does not maliciously create an invalid CBOR/payload and then allows it to 'fall through'
            # This is the reason that most mint operations require input_index=0
            return {
                'op': op_name,
                'payload': decoded_object,
                'payload_bytes': payload,
                'input_index': txin_idx
            }
        txin_idx = txin_idx + 1
    return None

# Check and unpack field data to see if it should be JSON/object encoded or there is a $ct and $d sub elements
# which provide the encoding and content type hint to successfully decode the field
def check_unpack_field_data(db_mint_value):
    try:
        fieldset = {}
        loaded_data = loads(db_mint_value)
        if type(loaded_data) is dict: 
            for key, value in loaded_data.items():
                fieldset[key] = {}
                if value:
                    if value.get('$ct', None) != None:
                        if len(value['$ct']) < 256:
                            fieldset[key]['content-type'] = value['$ct']
                        else:
                            fieldset[key]['content-type'] = 'invalid-content-type-too-long'
                    else: 
                        fieldset[key]['content-type'] = 'application/json'
                        serialized_object_size = sys.getsizeof(dumps(value))
                        fieldset[key]['content-length'] = serialized_object_size
                    if value.get('$d', None) != None:
                        fieldset[key]['content-length'] = len(value['$d'])
                else: 
                    # Empty value unparsed
                    fieldset[key] = {}
            return fieldset
    except Exception as e:
        print(f'check_unpack_field_data exception encountered, ignoring and continuing... {e}')
        pass
    return None