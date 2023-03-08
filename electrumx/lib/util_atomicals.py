# Copyright (c) 2023, The Atomicals Developers
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
to_le_uint32 = pack_le_uint32

def get_decode_cbor_payload(payload):
    try:
        loaded_data = loads(payload)
        metadata = loaded_data.get('metadata', None)
        return params
    except Exception as e:
        pass
    return None

def unpack_mint_info(mint_info_value):
    if not mint_info_value:
        raise IndexError(f'unpack_mint_info mint_info_value is null. Index error.')
    return pickle.loads(mint_info_value)
 
def get_expected_mint_output_index_of_atomical(input_idx, tx):
    expected_output_index = input_idx
    if input_idx >= len(tx.outputs):
        expected_output_index = 0
    return expected_output_index

def get_expected_output_index_of_atomical_nft(input_idx, tx, atomical_id, atomicals_operations_found, mint_info):
    assert(mint_info['type'] == 'NFT') # Sanity check
    expected_output_index = input_idx
    extract_atomical = atomicals_operations_found.get('x') != None and atomicals_operations_found['x'].get(idx) != None and atomicals_operations_found['x'][idx].get(atomical_id) != None
    if input_idx >= len(tx.outputs) or extract_atomical != None:
        expected_output_index = 0
    return expected_output_index

def get_expected_output_indexes_of_atomical_ft(input_idxs, total_value, tx, atomical_id, atomicals_operations_found, mint_info):
    assert(mint_info['type'] == 'FT') # Sanity check
    expected_output_indexes = []
    remaining_value = total_value
    # The FT type has the 'skip' (s) method to skip the first output in the assignment of the value of the token
    # Essentially this makes it possible to "split" out multiple FT's located at the same input
    # If any of the inputs has the skip operation, then it will apply for the atomical token generally and the first output will be skipped
    skip_first_output = False
    for idx in input_idxs: 
        if atomicals_operations_found.get('s') != None and atomicals_operations_found['s'].get(idx) != None and atomicals_operations_found['s'][idx].get(atomical_id) != None:
            skip_first_output = True 

    is_skipped = False
    for out_idx, txout in enumerate(tx.outputs): 
        if skip_first_output and not is_skipped:
            is_skipped = True
            continue 
        # For all remaining outputs attach colors as long as there is a whole remaining_value 
        if txout.value <= remaining_value:
            expected_output_indexes.append(out_idx)
            remaining_value -= txout.value
        else: 
            # Since one of the inputs was not less than or equal to the remaining value, then stop assigning outputs. The remaining coins are burned. RIP.
            break
    return expected_output_indexes

def get_expected_output_indexes_of_atomical_realm(input_idxs, total_value, tx, atomical_id, atomicals_operations_found, mint_info):
    assert(mint_info['type'] == 'REALM') # Sanity check
    # Only allow the progression of the realm if the realm was not merged and only if the first input was the realm
    if len(input_idxs) != 1 or input_idxs[0] != 0:
        return 
    expected_output_indexes = []
    

    return expected_output_indexes

def get_update_payload_from_inputs(input_idxs, atomicals_operations_found):
    update_payload = None 
    max_idx = -1 # Use the max index to ensure only the latest input index will be applied
    for idx in input_idxs: 
        if atomicals_operations_found.get('u') != None and atomicals_operations_found['u'].get(idx) != None and idx > max_idx:
            update_payload = atomicals_operations_found['u'][idx]
            max_idx = idx 
    return update_payload 

def compact_to_location_id_bytes(value):
    '''Convert the 36 byte atomical_id to the compact form with the "i" at the end
    '''

    index_of_i = value.index("i")
    if index_of_i != 64: 
        raise TypeError(f'{atomical_id} should be 32 bytes hex followed by i<number>')
    
    raw_hash = hex_str_to_hash(value[ : 64])
    
    if len(raw_hash) != 32:
        raise TypeError(f'{atomical_id} should be 32 bytes hex followed by i<number>')

    num = int(value[ 65: ])

    if num < 0 or num > 100000:
        raise TypeError(f'{atomical_id} index output number was parsed to be less than 0 or greater than 100000')

    return raw_hash + pack_le_uint32(num)
 
def location_id_bytes_to_compact(atomical_id):
    digit, = unpack_le_uint32_from(atomical_id[32:])
    return f'{hash_to_hex_str(atomical_id[:32])}i{digit}'
 
def get_tx_hash_index_from_location_id(atomical_id): 
    output_index, = unpack_le_uint32_from(atomical_id[ 32 : 36])
    return atomical_id[ : 32], output_index 
    
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

def parse_atomicals_data_definition_operation(script, n):
    '''Extract the atomical payload definitions'''
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
        raise ScriptError(f'parse_atomicals_mint_operation script error {e}') from None
 
def parse_operation_from_script(script, n):
    '''Parse an operation'''
    end = n + 2
    atom_op = script[n : end].hex()
    print(f'Potential Atomicals Operation Code Found: {atom_op}')
    atom_op_decoded = None
    if atom_op == "016e":
        # Mint NFT
        atom_op_decoded = 'n'
    if atom_op == "0166":
        # Mint FT
        atom_op_decoded = 'f'
    elif atom_op == "0175":
        # Update operation
        atom_op_decoded = 'u'
    elif atom_op == "0178":
        # Extract operation (for NFTs only)
        atom_op_decoded = 'x'
    # elif atom_op == "0177":
    #   # Mint Realm operation
    #    atom_op_decoded = 'r'
    elif atom_op == "0178":
        # Skip operation (for FTs only)
        atom_op_decoded = 's'

    if atom_op_decoded != None:
        return atom_op_decoded, parse_atomicals_data_definition_operation(script, end)
    
    print(f'Invalid Atomicals Operation Code. Skipping... "{script[n : end].hex()}"')
    return None, None

def parse_protocols_operations_from_witness_for_input(txinwitness):
    '''Detect and parse all operations across the witness input arrays from a tx'''
    atomical_operation_type_map = {}
    realm_operation_type_map = {}
    for script in txinwitness:
        n = 0
        script_entry_len = len(script)
        if script_entry_len < 39 or script[0] != 0x20:
            continue
        found_operation_definition = False
        while n < script_entry_len - 5:
            op = script[n]
            n += 1
            # Match 04"atom" pushdata in hex
            if op == 0x20 and n + 32 <= script_entry_len:
                n = n + 32
                while n < script_entry_len - 5:
                    op = script[n]
                    n += 1               
                    if op == OpCodes.OP_IF:
                        # spr3 / atom
                        if "0473707233" == script[n : n + 5].hex():
                            found_operation_definition = True
                            # Parse to ensure it is in the right format
                            operation_type, parsed_data = parse_operation_from_script(script, n + 5)
                            if operation_type != None and parsed_data != None:
                                atomical_operation_type_map[operation_type] = parsed_data
                            else: 
                                atomical_operation_type_map[operation_type] = {} 
                            break
                        # rollo / realm
                        # elif "05726f6c6c6f" == script[n : n + 6].hex():
                        #     found_operation_definition = True
                        #     # Parse to ensure it is in the right format
                        #     operation_type, parsed_data = parse_operation_from_script(script, n + 6)
                        #     if operation_type != None and parsed_data != None:
                        #         realm_operation_type_map[operation_type] = parsed_data
                        #     else: 
                        #         realm_operation_type_map[operation_type] = {} 
                        #     break
                if found_operation_definition:
                    break
            else:
                break
    return atomical_operation_type_map, realm_operation_type_map

def parse_protocols_operations_from_witness_array(tx):
    '''Detect and parse all operations of atomicals across the witness input arrays from a tx'''
    if not hasattr(tx, 'witness'):
        return {}, {}
    atomicals_operation_datas_by_input = {}
    realms_operation_datas_by_input = {}
    txin_idx = 0
    for txinwitness in tx.witness:
        atomicals_operation_data, realms_operation_data = parse_protocols_operations_from_witness_for_input(txinwitness)
        if atomicals_operation_data != None and len(atomicals_operation_data.items()) > 0: 
            atomicals_operation_datas_by_input[txin_idx] = {}
            # Group by operation type, 'n' for NFT atomical mint and 'f' for FT atomical mint
            if atomicals_operation_data.get("n") != None:
                atomicals_operation_datas_by_input["n"] = {}
                atomicals_operation_datas_by_input["n"][txin_idx] = atomicals_operation_data["n"]
            elif atomicals_operation_data.get("f") != None:
                atomicals_operation_datas_by_input["f"] = {}
                atomicals_operation_datas_by_input["f"][txin_idx] = atomicals_operation_data["f"]
            elif atomicals_operation_data.get("u") != None:
                atomicals_operation_datas_by_input["u"] = {}
                atomicals_operation_datas_by_input["u"][txin_idx] = atomicals_operation_data["u"]
            elif atomicals_operation_data.get("x") != None:
                atomicals_operation_datas_by_input["x"] = {}
                atomicals_operation_datas_by_input["x"][txin_idx] = atomicals_operation_data["x"]
            elif atomicals_operation_data.get("s") != None:
                atomicals_operation_datas_by_input["s"] = {}
                atomicals_operation_datas_by_input["s"][txin_idx] = atomicals_operation_data["s"]
        
        if realms_operation_data != None and len(realms_operation_data.items()) > 0: 
            realms_operation_datas_by_input[txin_idx] = {}
            # Group by operation type, 'r' for realm mint
            if realms_operation_data.get("r") != None:
                realms_operation_datas_by_input["r"] = {}
                realms_operation_datas_by_input["r"][txin_idx] = realms_operation_data["r"]
        txin_idx = txin_idx + 1

    return atomicals_operation_datas_by_input, realms_operation_datas_by_input

def check_unpack_mint_data(db_mint_value):
    try:
        fieldset = {}
        loaded_data = loads(db_mint_value)
        if type(loaded_data) is dict: 
            for key, value in loaded_data.items():
                fieldset[key] = {}
                if value:
                    if value.get('$ct', None) != None:
                        if len(value['$ct']) < 50:
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
        print('exception')
        print(e)
        pass
    return None