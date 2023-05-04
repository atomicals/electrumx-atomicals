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
from electrumx.lib.util import unpack_le_uint16_from, unpack_le_uint32_from, \
    pack_le_uint16, pack_le_uint32

from electrumx.lib.hash import hash_to_hex_str, hex_str_to_hash
import re
import sys
from cbor2 import dumps, loads, CBORDecodeError
from collections.abc import Mapping
to_le_uint32 = pack_le_uint32

def get_expected_output_index_of_atomical_in_tx(input_idx, tx):
    expected_output_index = input_idx
    if input_idx >= len(tx.outputs):
        expected_output_index = 0
    return expected_output_index

def decode_op_byte(byteop):
    if byteop == b'n':
        return 'nft'
    elif byteop == b'f':
        return 'ft',
    elif byteop == b'x':
        return 'ex',
    elif byteop == b'u':
        return 'up'

    raise TypeError(f'Invalid byteop {byteop}')

def compact_to_atomical_id_bytes(value):
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
 
def atomical_id_bytes_to_compact(atomical_id):
    digit, = unpack_le_uint32_from(atomical_id[32:])
    return f'{hash_to_hex_str(atomical_id[:32])}i{digit}'
 
def get_tx_hash_index_from_atomical_id(atomical_id): 
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
 
def parse_atomicals_extract_operation_push_datas(script, n):
    '''Get all of the push datas'''
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

def parse_atomicals_extract_operation(script, n):
    '''Parse an Atomicals extract operation'''
    payload = parse_atomicals_data_definition_operation(script, n)
    try:
        decoded_cbor = loads(payload)
        extract_keys = {}
        if decoded_cbor == None:
            print("Atomicals Extract: Payload CBOR parsing returned None. Doing nothing for the operation.")
            return {}
        try:
            iterator = iter(decoded_cbor)
            for item in decoded_cbor:
                extract_keys[item] = True     
        except TypeError:
            # Not iterable
            print('Atomicals Extract: Payload is not an array. Doing nothing for the operation.')
        return extract_keys, payload
    except CBORDecodeError:
        print("Atomicals Extract: CBORDecodeError. Doing nothing for the operation.")
    return None, payload
    
def parse_atomicals_mint_or_update_operation(script, n):
    return parse_atomicals_data_definition_operation(script, n) 
 
def parse_atomicals_operation_from_script(script, n):
    '''Parse an operation'''
    end = n + 2
    atom_op = script[n : end].hex()
    print(f'Atomicals operation found. {atom_op}')
    if atom_op == "016e":
        # Mint NFT
        return 'n', parse_atomicals_mint_or_update_operation(script, end), None
    if atom_op == "0166":
        # Mint FT
        return 'f', parse_atomicals_mint_or_update_operation(script, end), None
    elif atom_op == "0175":
        # Update operation
        return 'u', parse_atomicals_mint_or_update_operation(script, end), None
    elif atom_op == "0178":
        # Extract operation
        return 'x', parse_atomicals_mint_or_update_operation(script, end), None
    
    print(f'Undefined Atomicals operation found. {script[n : end].hex()}')
    return None, None

def parse_atomicals_operations_from_witness_for_input(txinwitness):
    '''Detect and parse all operations of atomicals across the witness input arrays from a tx'''
    operation_type_map = {}
    for script in txinwitness:
        n = 0
        script_entry_len = len(script)
        if script_entry_len < 39 or script[0] != 0x20:
            continue
        found_atomical = False
        while n < script_entry_len - 5:
            op = script[n]
            n += 1
            # op = script[n]
            # Match 04"atom" pushdata in hex
            if op == 0x20 and n + 32 <= script_entry_len:
                n = n + 32
                while n < script_entry_len - 5:
                    op = script[n]
                    n += 1               
                    if op == OpCodes.OP_IF:
                        # spr1
                        if "0473707232" == script[n : n + 5].hex():
                            found_atomical = True
                            # Parse to ensure it is in the right format
                            operation_type, parsed_data, rawpayload= parse_atomicals_operation_from_script(script, n + 5)
                            if parsed_data != None:
                                operation_type_map[operation_type] = parsed_data
                            else: 
                                operation_type_map[operation_type] = {} 
                            break
                if found_atomical:
                    break
            else:
                break
    return operation_type_map

def parse_atomicals_operations_from_witness_array(tx):
    '''Detect and parse all operations of atomicals across the witness input arrays from a tx'''
    if not hasattr(tx, 'witness'):
        return {}
    operation_datas_by_input = {}
    txin_idx = 0
    for txinwitness in tx.witness:
        operation_data = parse_atomicals_operations_from_witness_for_input(txinwitness)
        if operation_data != None and len(operation_data.items()) > 0: 
            operation_datas_by_input[txin_idx] = {}
            # Group by operation type
            if operation_data.get("m") != None:
                operation_datas_by_input["m"] = {}
                operation_datas_by_input["m"][txin_idx] = operation_data["m"]
            if operation_data.get("u") != None:
                operation_datas_by_input["u"] = {}
                operation_datas_by_input["u"][txin_idx] = operation_data["u"]
            if operation_data.get("x") != None:
                operation_datas_by_input["x"] = {}
                operation_datas_by_input["x"][txin_idx] = operation_data["x"]
        txin_idx = txin_idx + 1
    return operation_datas_by_input

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