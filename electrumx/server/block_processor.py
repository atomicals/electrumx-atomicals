# Copyright (c) 2016-2017, Neil Booth
# Copyright (c) 2017, the ElectrumX authors
#
# All rights reserved.
#
# See the file "LICENCE" for information about the copyright
# and warranty status of this software.

'''Block prefetcher and chain processor.'''


import asyncio
import time
from typing import Sequence, Tuple, List, Callable, Optional, TYPE_CHECKING, Type

from aiorpcx import run_in_thread, CancelledError

import electrumx
from electrumx.server.daemon import DaemonError, Daemon
from electrumx.lib.hash import hash_to_hex_str, HASHX_LEN, double_sha256
from electrumx.lib.script import is_unspendable_legacy, is_unspendable_genesis
from electrumx.lib.util import (
    chunks, class_logger, pack_le_uint32, pack_le_uint64, unpack_le_uint64, pack_be_uint64, unpack_be_uint64, OldTaskGroup
)
from electrumx.lib.tx import Tx
from electrumx.server.db import FlushData, COMP_TXID_LEN, DB
from electrumx.server.history import TXNUM_LEN
from electrumx.lib.util_atomicals import parse_atomicals_operations_from_witness_array, get_expected_output_index_of_atomical_in_tx, atomical_id_bytes_to_compact

if TYPE_CHECKING:
    from electrumx.lib.coins import Coin
    from electrumx.server.env import Env
    from electrumx.server.controller import Notifications

from cbor2 import dumps, loads, CBORDecodeError
import pickle

TX_HASH_LEN = 32
SCRIPTHASH_LEN = 32
ATOMICAL_ID_LEN = 36
TX_OUPUT_IDX_LEN = 4

class Prefetcher:
    '''Prefetches blocks (in the forward direction only).'''

    def __init__(self, daemon: 'Daemon', coin: Type['Coin'], blocks_event: asyncio.Event):
        self.logger = class_logger(__name__, self.__class__.__name__)
        self.daemon = daemon
        self.coin = coin
        self.blocks_event = blocks_event
        self.blocks = []
        self.caught_up = False
        # Access to fetched_height should be protected by the semaphore
        self.fetched_height = None
        self.semaphore = asyncio.Semaphore()
        self.refill_event = asyncio.Event()
        # The prefetched block cache size.  The min cache size has
        # little effect on sync time.
        self.cache_size = 0
        self.min_cache_size = 10 * 1024 * 1024
        # This makes the first fetch be 10 blocks
        self.ave_size = self.min_cache_size // 10
        self.polling_delay = 5

    async def main_loop(self, bp_height):
        '''Loop forever polling for more blocks.'''
        await self.reset_height(bp_height)
        while True:
            try:
                # Sleep a while if there is nothing to prefetch
                await self.refill_event.wait()
                if not await self._prefetch_blocks():
                    await asyncio.sleep(self.polling_delay)
            except DaemonError as e:
                self.logger.info(f'ignoring daemon error: {e}')
            except asyncio.CancelledError as e:
                self.logger.info(f'cancelled; prefetcher stopping {e}')
                raise
            except Exception:
                self.logger.exception(f'ignoring unexpected exception')

    def get_prefetched_blocks(self):
        '''Called by block processor when it is processing queued blocks.'''
        blocks = self.blocks
        self.blocks = []
        self.cache_size = 0
        self.refill_event.set()
        return blocks

    async def reset_height(self, height):
        '''Reset to prefetch blocks from the block processor's height.

        Used in blockchain reorganisations.  This coroutine can be
        called asynchronously to the _prefetch_blocks coroutine so we
        must synchronize with a semaphore.
        '''
        async with self.semaphore:
            self.blocks.clear()
            self.cache_size = 0
            self.fetched_height = height
            self.refill_event.set()

        daemon_height = await self.daemon.height()
        behind = daemon_height - height
        if behind > 0:
            self.logger.info(
                f'catching up to daemon height {daemon_height:,d} ({behind:,d} '
                f'blocks behind)'
            )
        else:
            self.logger.info(f'caught up to daemon height {daemon_height:,d}')

    async def _prefetch_blocks(self):
        '''Prefetch some blocks and put them on the queue.

        Repeats until the queue is full or caught up.
        '''
        daemon = self.daemon
        daemon_height = await daemon.height()
        async with self.semaphore:
            while self.cache_size < self.min_cache_size:
                first = self.fetched_height + 1
                # Try and catch up all blocks but limit to room in cache.
                cache_room = max(self.min_cache_size // self.ave_size, 1)
                count = min(daemon_height - self.fetched_height, cache_room)
                # Don't make too large a request
                count = min(self.coin.max_fetch_blocks(first), max(count, 0))
                if not count:
                    self.caught_up = True
                    return False

                hex_hashes = await daemon.block_hex_hashes(first, count)
                if self.caught_up:
                    self.logger.info(f'new block height {first + count-1:,d} '
                                     f'hash {hex_hashes[-1]}')
                blocks = await daemon.raw_blocks(hex_hashes)

                assert count == len(blocks)

                # Special handling for genesis block
                if first == 0:
                    blocks[0] = self.coin.genesis_block(blocks[0])
                    self.logger.info(f'verified genesis block with hash '
                                     f'{hex_hashes[0]}')

                # Update our recent average block size estimate
                size = sum(len(block) for block in blocks)
                if count >= 10:
                    self.ave_size = size // count
                else:
                    self.ave_size = (size + (10 - count) * self.ave_size) // 10

                self.blocks.extend(blocks)
                self.cache_size += size
                self.fetched_height += count
                self.blocks_event.set()

        self.refill_event.clear()
        return True


class ChainError(Exception):
    '''Raised on error processing blocks.'''


class BlockProcessor:
    '''Process blocks and update the DB state to match.

    Employ a prefetcher to prefetch blocks in batches for processing.
    Coordinate backing up in case of chain reorganisations.
    '''

    def __init__(self, env: 'Env', db: DB, daemon: Daemon, notifications: 'Notifications'):
        self.env = env
        self.db = db
        self.daemon = daemon
        self.notifications = notifications

        self.coin = env.coin
        # blocks_event: set when new blocks are put on the queue by the Prefetcher, to be processed
        self.blocks_event = asyncio.Event()
        self.prefetcher = Prefetcher(daemon, env.coin, self.blocks_event)
        self.logger = class_logger(__name__, self.__class__.__name__)

        # Meta
        self.next_cache_check = 0
        self.touched = set()
        self.reorg_count = 0
        self.height = -1
        self.tip = None  # type: Optional[bytes]
        self.tip_advanced_event = asyncio.Event()
        self.tx_count = 0
        self.atomical_count = 0
        self._caught_up_event = None

        # Caches of unflushed items.
        self.headers = []
        self.tx_hashes = []
        self.undo_infos = []  # type: List[Tuple[Sequence[bytes], int]]
        self.atomicals_undo_infos = []  # type: List[Tuple[Sequence[bytes], int]]

        # UTXO cache
        self.utxo_cache = {}
        self.atomicals_utxo_cache = {}
        self.atomicals_idempotent_data = {}
        self.db_deletes = []

        # If the lock is successfully acquired, in-memory chain state
        # is consistent with self.height
        self.state_lock = asyncio.Lock()

        # Signalled after backing up during a reorg
        self.backed_up_event = asyncio.Event()

    async def run_in_thread_with_lock(self, func, *args):
        # Run in a thread to prevent blocking.  Shielded so that
        # cancellations from shutdown don't lose work - when the task
        # completes the data will be flushed and then we shut down.
        # Take the state lock to be certain in-memory state is
        # consistent and not being updated elsewhere.
        async def run_in_thread_locked():
            async with self.state_lock:
                return await run_in_thread(func, *args)
        return await asyncio.shield(run_in_thread_locked())

    async def check_and_advance_blocks(self, raw_blocks):
        '''Process the list of raw blocks passed.  Detects and handles
        reorgs.
        '''
        if not raw_blocks:
            return
        first = self.height + 1
        blocks = [self.coin.block(raw_block, first + n)
                  for n, raw_block in enumerate(raw_blocks)]
        headers = [block.header for block in blocks]
        hprevs = [self.coin.header_prevhash(h) for h in headers]
        chain = [self.tip] + [self.coin.header_hash(h) for h in headers[:-1]]

        if hprevs == chain:
            start = time.monotonic()
            await self.run_in_thread_with_lock(self.advance_blocks, blocks)
            await self._maybe_flush()
            if not self.db.first_sync:
                s = '' if len(blocks) == 1 else 's'
                blocks_size = sum(len(block) for block in raw_blocks) / 1_000_000
                self.logger.info(f'processed {len(blocks):,d} block{s} size {blocks_size:.2f} MB '
                                 f'in {time.monotonic() - start:.1f}s')
            if self._caught_up_event.is_set():
                await self.notifications.on_block(self.touched, self.height)
            self.touched = set()
        elif hprevs[0] != chain[0]:
            await self.reorg_chain()
        else:
            # It is probably possible but extremely rare that what
            # bitcoind returns doesn't form a chain because it
            # reorg-ed the chain as it was processing the batched
            # block hash requests.  Should this happen it's simplest
            # just to reset the prefetcher and try again.
            self.logger.warning('daemon blocks do not form a chain; '
                                'resetting the prefetcher')
            await self.prefetcher.reset_height(self.height)

    async def reorg_chain(self, count=None):
        '''Handle a chain reorganisation.

        Count is the number of blocks to simulate a reorg, or None for
        a real reorg.'''
        if count is None:
            self.logger.info('chain reorg detected')
        else:
            self.logger.info(f'faking a reorg of {count:,d} blocks')
        await self.flush(True)

        async def get_raw_blocks(last_height, hex_hashes) -> Sequence[bytes]:
            heights = range(last_height, last_height - len(hex_hashes), -1)
            try:
                blocks = [self.db.read_raw_block(height) for height in heights]
                self.logger.info(f'read {len(blocks)} blocks from disk')
                return blocks
            except FileNotFoundError:
                return await self.daemon.raw_blocks(hex_hashes)

        def flush_backup():
            # self.touched can include other addresses which is
            # harmless, but remove None.
            self.touched.discard(None)
            self.db.flush_backup(self.flush_data(), self.touched)

        _start, last, hashes = await self.reorg_hashes(count)
        # Reverse and convert to hex strings.
        hashes = [hash_to_hex_str(hash) for hash in reversed(hashes)]
        for hex_hashes in chunks(hashes, 50):
            raw_blocks = await get_raw_blocks(last, hex_hashes)
            await self.run_in_thread_with_lock(self.backup_blocks, raw_blocks)
            await self.run_in_thread_with_lock(flush_backup)
            last -= len(raw_blocks)
        await self.prefetcher.reset_height(self.height)
        self.backed_up_event.set()
        self.backed_up_event.clear()

    async def reorg_hashes(self, count):
        '''Return a pair (start, last, hashes) of blocks to back up during a
        reorg.

        The hashes are returned in order of increasing height.  Start
        is the height of the first hash, last of the last.
        '''
        start, count = await self.calc_reorg_range(count)
        last = start + count - 1
        s = '' if count == 1 else 's'
        self.logger.info(f'chain was reorganised replacing {count:,d} '
                         f'block{s} at heights {start:,d}-{last:,d}')

        return start, last, await self.db.fs_block_hashes(start, count)

    async def calc_reorg_range(self, count):
        '''Calculate the reorg range'''

        def diff_pos(hashes1, hashes2):
            '''Returns the index of the first difference in the hash lists.
            If both lists match returns their length.'''
            for n, (hash1, hash2) in enumerate(zip(hashes1, hashes2)):
                if hash1 != hash2:
                    return n
            return len(hashes)

        if count is None:
            # A real reorg
            start = self.height - 1
            count = 1
            while start > 0:
                hashes = await self.db.fs_block_hashes(start, count)
                hex_hashes = [hash_to_hex_str(hash) for hash in hashes]
                d_hex_hashes = await self.daemon.block_hex_hashes(start, count)
                n = diff_pos(hex_hashes, d_hex_hashes)
                if n > 0:
                    start += n
                    break
                count = min(count * 2, start)
                start -= count

            count = (self.height - start) + 1
        else:
            start = (self.height - count) + 1

        return start, count

    def estimate_txs_remaining(self):
        # Try to estimate how many txs there are to go
        daemon_height = self.daemon.cached_height()
        coin = self.coin
        tail_count = daemon_height - max(self.height, coin.TX_COUNT_HEIGHT)
        # Damp the initial enthusiasm
        realism = max(2.0 - 0.9 * self.height / coin.TX_COUNT_HEIGHT, 1.0)
        return (tail_count * coin.TX_PER_BLOCK +
                max(coin.TX_COUNT - self.tx_count, 0)) * realism

    # - Flushing
    def flush_data(self):
        '''The data for a flush.  The lock must be taken.'''
        assert self.state_lock.locked()
        return FlushData(self.height, self.tx_count, self.atomical_count, self.headers,
                         self.tx_hashes, self.undo_infos, self.atomicals_undo_infos, self.utxo_cache, self.atomicals_utxo_cache, self.atomicals_idempotent_data,
                         self.db_deletes, self.tip)

    async def flush(self, flush_utxos):
        def flush():
            self.db.flush_dbs(self.flush_data(), flush_utxos,
                              self.estimate_txs_remaining)
        await self.run_in_thread_with_lock(flush)

    async def _maybe_flush(self):
        # If caught up, flush everything as client queries are
        # performed on the DB.
        if self._caught_up_event.is_set():
            await self.flush(True)
        elif time.monotonic() > self.next_cache_check:
            flush_arg = self.check_cache_size()
            if flush_arg is not None:
                await self.flush(flush_arg)
            self.next_cache_check = time.monotonic() + 30

    def check_cache_size(self):
        '''Flush a cache if it gets too big.'''
        # Good average estimates based on traversal of subobjects and
        # requesting size from Python (see deep_getsizeof).
        one_MB = 1000*1000
        utxo_cache_size = len(self.utxo_cache) * 205
        db_deletes_size = len(self.db_deletes) * 57
        hist_cache_size = self.db.history.unflushed_memsize()
        # Roughly ntxs * 32 + nblocks * 42
        tx_hash_size = ((self.tx_count - self.db.fs_tx_count) * 32
                        + (self.height - self.db.fs_height) * 42)
        utxo_MB = (db_deletes_size + utxo_cache_size) // one_MB
        hist_MB = (hist_cache_size + tx_hash_size) // one_MB

        self.logger.info(f'our height: {self.height:,d} daemon: '
                         f'{self.daemon.cached_height():,d} '
                         f'UTXOs {utxo_MB:,d}MB hist {hist_MB:,d}MB')

        # Flush history if it takes up over 20% of cache memory.
        # Flush UTXOs once they take up 80% of cache memory.
        cache_MB = self.env.cache_MB
        if utxo_MB + hist_MB >= cache_MB or hist_MB >= cache_MB // 5:
            return utxo_MB >= cache_MB * 4 // 5
        return None

    def put_atomicals_utxo(self, location_id, atomical_id, value): 
        if self.atomicals_utxo_cache.get(location_id) == None: 
            self.atomicals_utxo_cache[location_id] = {}

        self.atomicals_utxo_cache[location_id][atomical_id] = value

    def advance_blocks(self, blocks):
        '''Synchronously advance the blocks.

        It is already verified they correctly connect onto our tip.
        '''
        min_height = self.db.min_undo_height(self.daemon.cached_height())
        height = self.height
        genesis_activation = self.coin.GENESIS_ACTIVATION

        for block in blocks:
            height += 1
            is_unspendable = (is_unspendable_genesis if height >= genesis_activation
                              else is_unspendable_legacy)
            undo_info, atomicals_undo_info = self.advance_txs(block.transactions, is_unspendable, block.header, height)
            if height >= min_height:
                self.undo_infos.append((undo_info, height))
                self.atomicals_undo_infos.append((atomicals_undo_info, height))
                self.db.write_raw_block(block.raw, height)

        headers = [block.header for block in blocks]
        self.height = height
        self.headers += headers
        self.tip = self.coin.header_hash(headers[-1])
        self.tip_advanced_event.set()
        self.tip_advanced_event.clear()

    def advance_txs(
            self,
            txs: Sequence[Tuple[Tx, bytes]],
            is_unspendable: Callable[[bytes], bool],
            header,
            height
    ) -> Sequence[bytes]:
        self.tx_hashes.append(b''.join(tx_hash for tx, tx_hash in txs))

        # Use local vars for speed in the loops
        undo_info = []
        atomicals_undo_info = []
        tx_num = self.tx_count
        atomical_num = self.atomical_count
        script_hashX = self.coin.hashX_from_script
        put_utxo = self.utxo_cache.__setitem__
        put_atomicals_idempotent_data = self.atomicals_idempotent_data.__setitem__
        spend_utxo = self.spend_utxo
        spend_atomicals_utxo = self.spend_atomicals_utxo
        undo_info_append = undo_info.append
        atomicals_undo_info_extend = atomicals_undo_info.extend
        update_touched = self.touched.update
        hashXs_by_tx = []
        append_hashXs = hashXs_by_tx.append
        to_le_uint32 = pack_le_uint32
        to_le_uint64 = pack_le_uint64
        to_be_uint64 = pack_be_uint64

        for tx, tx_hash in txs:
            hashXs = []
            append_hashX = hashXs.append
            tx_numb = to_le_uint64(tx_num)[:TXNUM_LEN]
            atomicals_transfers_found_at_inputs = {}
            
            # Spend the inputs
            txin_index = 0
            for txin in tx.inputs:
                if txin.is_generation():
                    continue
                cache_value = spend_utxo(txin.prev_hash, txin.prev_idx)
                undo_info_append(cache_value)
                append_hashX(cache_value[:HASHX_LEN])

                # Find all the existing transferred atomicals and spend the Atomicals utxos
                atomicals_transferred_list = spend_atomicals_utxo(txin.prev_hash, txin.prev_idx)
                if len(atomicals_transferred_list):
                    atomicals_transfers_found_at_inputs[txin_index] = atomicals_transferred_list
                atomicals_undo_info_extend(atomicals_transferred_list)
                txin_index = txin_index + 1
            
            # Detect all Atomicals operations in the transaction witness inputs
            atomicals_operations_found = parse_atomicals_operations_from_witness_array(tx)
            atomicals_operations_or_transfer_found = (atomicals_operations_found != None and len(atomicals_operations_found.items()) > 0) or ( atomicals_transfers_found_at_inputs != None and len(atomicals_transfers_found_at_inputs.items()) > 0)
            if atomicals_operations_or_transfer_found:
                # How to get the original tx rawtx??? or should we just pass in the deserialized?
                put_atomicals_idempotent_data(b't' + tx_hash, pickle.dumps(tx))

            # Add the new UTXOs
            for idx, txout in enumerate(tx.outputs):
                # Ignore unspendable outputs
                if is_unspendable(txout.pk_script):
                    continue

                # Get the hashX
                hashX = script_hashX(txout.pk_script)
                append_hashX(hashX)
                put_utxo(tx_hash + to_le_uint32(idx),
                         hashX + tx_numb + to_le_uint64(txout.value))

            # Bulk of the transfer, extract, update and mint logic for Atomicals 
            # We already have the spent Atomicals in atomicals_transfers_found_at_inputs and the atomicals_operations_found from the witness scripts of inputs
            #
            # Format of atomicals_transfers_found_at_inputs is:
            # map(input_idx => Sequence(location + atomical_id + hashX + scripthash + value_sats))
            # 
            # Format of atomicals_operations_found is:
            # map("m|u" => 
            #    map(input_idx" => CBOR encoded payload
            #
            # map("x" => 
            #    map(input_idx" =>
            #       map("fieldname" => True
            #
            
            # Process Mint operations
            if atomicals_operations_found.get('n') != None:
                input_idx_map = atomicals_operations_found['n']
                atomical_num += 1
                for input_idx, payload_data in input_idx_map.items():
                    # The atomical cannot be created if there is not a corresponding output to put the atomical onto
                    # This is done so that if an atomical mint is in the n'th input, and there are insufficient outputs
                    # ... then it is not that easy to determine where the atomical mint was located without doing a scan
                    # ... of all the inputs
                    # Therefore it is invalid to mint at the n'th input and have less than n outputs
                    if input_idx >= len(tx.outputs):
                        continue
                    # Lookup the txout will be imprinted with the atomical
                    expected_output_index = get_expected_output_index_of_atomical_in_tx(input_idx, tx) 
                    txout = tx.outputs[expected_output_index]
                    scripthash = double_sha256(txout.pk_script)
                    hashX = script_hashX(txout.pk_script)
                    output_idx_le = to_le_uint32(expected_output_index) 
                    input_idx_le = to_le_uint32(input_idx) 
                    location = tx_hash + output_idx_le
                    value_sats = to_le_uint64(txout.value)
                    # Establish the atomical_id from the initial location
                    atomical_id = location
                    self.logger.info(f'Atomicals mint in Transaction {hash_to_hex_str(tx_hash)} @ Input Index: {input_idx:,d}. Minting at Output Index: {input_idx:,d}, atomical_id={atomical_id_bytes_to_compact(atomical_id)}, atomical_number={atomical_num}') 
                    # Leverage existing history table by double hashing the atomical_id
                    append_hashX(double_sha256(atomical_id))
                    atomical_count_numb = to_be_uint64(atomical_num)
                    # Save mint data
                    put_atomicals_idempotent_data(b'md' + atomical_id, payload_data)
                    # Save mint block info
                    put_atomicals_idempotent_data(b'mb' + atomical_id, atomical_count_numb + header + to_le_uint32(height))
                    # Save mint info
                    put_atomicals_idempotent_data(b'mi' + atomical_id, input_idx_le + scripthash + value_sats + b'n' + txout.pk_script)
                    # Track the atomical number for the newly minted atomical
                    put_atomicals_idempotent_data(b'n' + atomical_count_numb, atomical_id)
                    # Track active atomical location
                    put_atomicals_idempotent_data(b'a' + atomical_id + location, location + scripthash + value_sats)
                    # Save the output script of the atomical to lookup at a future point
                    put_atomicals_idempotent_data(b'z' + tx_hash + output_idx_le, txout.pk_script)
                    # Save the location to have the atomical located there
                    self.put_atomicals_utxo(location, atomical_id, hashX + scripthash + value_sats + tx_numb)

            # Process the updates data
            for idx, atomicals_list in atomicals_transfers_found_at_inputs.items():
                self.logger.info(f'Atomicals at index')
                self.logger.info(idx)
                self.logger.info('Atomicals list')
                self.logger.info(atomicals_list)
               
                # Process the generic transfer and the extract operation (if there is one for the specific Atomicals)
                for atomical_item in atomicals_list:
                    self.logger.info('atomical_item being processed for a transfer found at the inputs')
                    self.logger.info(atomical_item.hex())
                    
                    atomical_id = atomical_item[ATOMICAL_ID_LEN : ATOMICAL_ID_LEN + ATOMICAL_ID_LEN]
                    # input_idx_packed = pack_le_uint32(idx)
                    expected_output_index = get_expected_output_index_of_atomical_in_tx(idx, tx) 
                    expected_output_index_packed = pack_le_uint32(expected_output_index)
                    # There is an update operation at the Atomical outpoint being spent
                    if atomicals_operations_found.get('u') != None and atomicals_operations_found['u'].get(idx) != None:
                        update_payload = atomicals_operations_found['u'][idx]
                        for atomical_item in atomicals_list:    
                            # Save the latest updated fields (beta)
                            put_atomicals_idempotent_data(b's' + atomical_id + tx_numb + expected_output_index_packed, update_payload)
                    
                    # If the extract operation refers to the current atomical, then force it to the 0'th output
                    if atomicals_operations_found.get('x') != None and atomicals_operations_found['x'].get(idx) != None and atomicals_operations_found['x'][idx].get(atomical_id) != None:
                        expected_output_index = 0 
                    output_idx_le = to_le_uint32(expected_output_index) 
                    location = tx_hash + output_idx_le
                    txout = tx.outputs[expected_output_index]
                    scripthash = double_sha256(txout.pk_script)
                    hashX = script_hashX(txout.pk_script)
                    value_sats = to_le_uint64(txout.value)
                    put_atomicals_idempotent_data(b'a' + atomical_id + location, location + scripthash + value_sats)
                     # Save the output script of the atomical to lookup at a future point
                    put_atomicals_idempotent_data(b'z' + tx_hash + output_idx_le, txout.pk_script)
                    self.logger.info('new atomical locations for atomical_id')
                    self.logger.info(atomical_id.hex())
                    self.logger.info('location')
                    self.logger.info(location.hex())
                    self.logger.info('scripthash')
                    self.logger.info(scripthash)
                    self.logger.info('value_sats')
                    self.logger.info(value_sats)
                    self.logger.info('atomical_item')
                    self.logger.info(atomical_item.hex())
                    self.put_atomicals_utxo(location, atomical_id, hashX + scripthash + value_sats + tx_numb)
                    # Leverage existing history table by hashing the atomical_id
                    append_hashX(double_sha256(atomical_id))

            append_hashXs(hashXs)
            update_touched(hashXs)
            tx_num += 1

        self.db.history.add_unflushed(hashXs_by_tx, self.tx_count)
        self.tx_count = tx_num
        self.db.tx_counts.append(tx_num)

        self.atomical_count = atomical_num
        self.db.atomical_counts.append(atomical_num)

        return undo_info, atomicals_undo_info

    def backup_blocks(self, raw_blocks: Sequence[bytes]):
        '''Backup the raw blocks and flush.

        The blocks should be in order of decreasing height, starting at.
        self.height.  A flush is performed once the blocks are backed up.
        '''
        self.db.assert_flushed(self.flush_data())
        assert self.height >= len(raw_blocks)
        genesis_activation = self.coin.GENESIS_ACTIVATION

        coin = self.coin
        for raw_block in raw_blocks:
            # Check and update self.tip
            block = coin.block(raw_block, self.height)
            header_hash = coin.header_hash(block.header)
            if header_hash != self.tip:
                raise ChainError(
                    f'backup block {hash_to_hex_str(header_hash)} not tip '
                    f'{hash_to_hex_str(self.tip)} at height {self.height:,d}'
                )
            self.tip = coin.header_prevhash(block.header)
            is_unspendable = (is_unspendable_genesis if self.height >= genesis_activation
                              else is_unspendable_legacy)
            self.backup_txs(block.transactions, is_unspendable)
            self.height -= 1
            self.db.tx_counts.pop()
            self.db.atomicals_counts.pop()

        self.logger.info(f'backed up to height {self.height:,d}')

    def backup_txs(
            self,
            txs: Sequence[Tuple[Tx, bytes]],
            is_unspendable: Callable[[bytes], bool],
    ):
        # Prevout values, in order down the block (coinbase first if present)
        # undo_info is in reverse block order
        undo_info = self.db.read_undo_info(self.height)
        if undo_info is None:
            raise ChainError(f'no undo information found for height '
                             f'{self.height:,d}')
        n = len(undo_info)

        atomicals_undo_info = self.db.read_atomicals_undo_info(self.height)
        if atomicals_undo_info is None:
            raise ChainError(f'no atomicals undo information found for height '
                             f'{self.height:,d}')
        m = len(atomicals_undo_info)
        atomicals_undo_entry_len = ATOMICAL_ID_LEN + ATOMICAL_ID_LEN + HASHX_LEN + SCRIPTHASH_LEN + 8 + 4
        atomicals_count = m / atomicals_undo_entry_len
        # Build a map of location to atomical
        has_undo_info_for_atomicals = False
        if m > 0:
            has_undo_info_for_atomicals = True
        c = m
        atomicals_undo_info_map = {}
        counted_atomicals_count = 0
        while c > 0:
            c -= atomicals_undo_entry_len
            assert(c >= 0)
            atomicals_undo_item = atomicals_undo_info[c : c + atomicals_undo_entry_len]
            atomicals_location = atomicals_undo_item[: ATOMICAL_ID_LEN]
            atomicals_atomical_id = atomicals_undo_item[ ATOMICAL_ID_LEN : ATOMICAL_ID_LEN + ATOMICAL_ID_LEN]
            # Remainder is: hashX + scripthash + value_sats
            atomicals_value = atomicals_undo_item[ATOMICAL_ID_LEN + ATOMICAL_ID_LEN :]
            
            # There can be many atomicals at the same location
            if atomicals_undo_info_map.get(atomicals_location) != None:
                atomicals_undo_info_map[atomicals_location] = []

            atomicals_undo_info_map[atomicals_location].append({ 
                location: atomicals_location,
                atomical_id: atomicals_atomical_id,
                value: atomicals_value
            })
            counted_atomicals_count += 1
        
        assert(counted_atomicals_count == atomicals_count)

        # Use local vars for speed in the loops
        put_utxo = self.utxo_cache.__setitem__
        spend_utxo = self.spend_utxo
        
        put_atomicals_idempotent_data = self.atomicals_idempotent_data.__setitem__
        spend_atomicals_utxo = self.spend_atomicals_utxo
        
        touched = self.touched
        undo_entry_len = HASHX_LEN + TXNUM_LEN + 8

        tx_num = self.tx_count
        atomical_num = self.atomical_count
        atomicals_minted = 0
        for tx, tx_hash in reversed(txs):

            for idx, txout in enumerate(tx.outputs):
                # Spend the TX outputs.  Be careful with unspendable
                # outputs - we didn't save those in the first place.
                if is_unspendable(txout.pk_script):
                    continue

                # Get the hashX
                cache_value = spend_utxo(tx_hash, idx)
                hashX = cache_value[:HASHX_LEN]
                touched.add(hashX)
                output_index_packed = pack_le_uint32(idx)
                current_location = tx_hash + output_index_packed
                if has_undo_info_for_atomicals:
                    # Spend the atomicals
                    spent_atomicals = spend_atomicals_utxo(tx_hash, idx)
                    for spent_atomical in spent_atomicals:
                        hashX = spent_atomical[ ATOMICAL_ID_LEN + ATOMICAL_ID_LEN : ATOMICAL_ID_LEN + ATOMICAL_ID_LEN + HASHX_LEN]
                        touched.add(hashX)
                        atomical_id = spent_atomical[ ATOMICAL_ID_LEN : ATOMICAL_ID_LEN + ATOMICAL_ID_LEN]
                        location = spent_atomical[ : ATOMICAL_ID_LEN]
                        # Remove the stored output
                        self.db_deletes.append(b'z' + location)
                        # Just delete any potential state updates indiscriminately
                        # what that means is that we do not actually correlate which input event/field corresponds to the spent atomical
                        # We merely wipe out every possible state and field update even if it was for another atomical
                        # Since we are deleting for the tx_numb, this is safe, albeit inefficient
                        updates_map = atomicals_operations_found.get('u', None)
                        if updates_map != None:
                            for input_idx, fieldsmap in updates_map.items():
                                for field, value_not_used in fieldsmap.items():
                                    self.db_deletes.append(b's' + atomical_id + tx_numb + output_index_packed)

            atomicals_operations_found = {}
            # This will need to apply per input
            if has_undo_info_for_atomicals:
                atomicals_operations_found = parse_atomicals_operations_from_witness_array(tx)
                # remove the mint data if this was a mint
                if atomicals_operations_found.get('n') != None: 
                    input_idx_map = atomicals_operations_found['n']
                    for input_idx, fields_map in input_idx_map.items():
                        # Lookup the txout that would be imprinted with the atomical
                        expected_output_index = get_expected_output_index_of_atomical_in_tx(input_idx, tx) 
                        output_idx_le = to_le_uint32(expected_output_index) 
                        location = tx_hash + output_idx_le
                        # Establish the atomical_id from the initial location
                        atomical_id = location
                        self.db_deletes.append(b'md' + atomical_id)
                        self.db_deletes.append(b'mb' + atomical_id)
                        self.db_deletes.append(b'mi' + atomical_id)
                        # Make sure to remove the atomical number
                        atomical_numb = pack_be_uint64(atomical_num) 
                        self.db_deletes.append(b'n' + atomical_numb)
                        # remove the b'a' state since we know it was a mint and it's no longer needed
                        self.db_deletes.append(b'a' + atomical_id + location)
                        atomical_num -= 1
                        atomicals_minted += 1

            # Restore the inputs
            for txin in reversed(tx.inputs):
                if txin.is_generation():
                    continue
                n -= undo_entry_len
                undo_item = undo_info[n:n + undo_entry_len]
                put_utxo(txin.prev_hash + pack_le_uint32(txin.prev_idx), undo_item)
                hashX = undo_item[:HASHX_LEN]
                touched.add(hashX)
                # Restore the atomicals utxos in the undo information
                # Note that we do not need to restart any of the b's' states because they are already recorded from before
                # We only had to make sure the appropriate b's' states were deleted above
                # And therefore the only thing left for us to do here is to restore the correct spend status of the atomicals here
                # so that we can correctly lookup the atomicals by address/scripthash and also by the b'a' location.
                potential_atomicals_list_to_restore = atomicals_undo_info_map.get(txin.prev_hash + pack_le_uint32(txin.prev_idx), None)
                if potential_atomicals_list_to_restore != None:
                    for atomical_to_restore in potential_atomicals_list_to_restore:
                        self.put_atomicals_utxo(atomical_to_restore['location'], atomical_to_restore['atomical_id'], atomical_to_restore['value'])
                        # Make sure not to take the hashX in the value since it's not needed in the b'a' index
                        put_atomicals_idempotent_data(b'a' + atomical_to_restore['atomical_id'] + atomical_to_restore['location'], atomical_to_restore['location'] + atomical_to_restore['value'][ HASHX_LEN : ])
            
            # Delete the atomical number index for the current atomical number
            atomical_count_numb = pack_be_uint64(atomical_num)
            self.db_deletes.append(b'n' + atomical_count_numb)
            tx_num -= 1

        assert n == 0
        assert m == 0

        self.tx_count -= len(txs)
        self.atomical_count -= len(atomicals_minted)

    '''An in-memory UTXO cache, representing all changes to UTXO state
    since the last DB flush.

    We want to store millions of these in memory for optimal
    performance during initial sync, because then it is possible to
    spend UTXOs without ever going to the database (other than as an
    entry in the address history, and there is only one such entry per
    TX not per UTXO).  So store them in a Python dictionary with
    binary keys and values.

      Key:    TX_HASH + TX_IDX           (32 + 4 = 36 bytes)
      Value:  HASHX + TX_NUM + VALUE     (11 + 5 + 8 = 24 bytes)

    That's 60 bytes of raw data in-memory.  Python dictionary overhead
    means each entry actually uses about 205 bytes of memory.  So
    almost 5 million UTXOs can fit in 1GB of RAM.  There are
    approximately 42 million UTXOs on bitcoin mainnet at height
    433,000.

    Semantics:

      add:   Add it to the cache dictionary.

      spend: Remove it if in the cache dictionary.  Otherwise it's
             been flushed to the DB.  Each UTXO is responsible for two
             entries in the DB.  Mark them for deletion in the next
             cache flush.

    The UTXO database format has to be able to do two things efficiently:

      1.  Given an address be able to list its UTXOs and their values
          so its balance can be efficiently computed.

      2.  When processing transactions, for each prevout spent - a (tx_hash,
          idx) pair - we have to be able to remove it from the DB.  To send
          notifications to clients we also need to know any address it paid
          to.

    To this end we maintain two "tables", one for each point above:

      1.  Key: b'u' + address_hashX + tx_idx + tx_num
          Value: the UTXO value as a 64-bit unsigned integer

      2.  Key: b'h' + compressed_tx_hash + tx_idx + tx_num
          Value: hashX

    The compressed tx hash is just the first few bytes of the hash of
    the tx in which the UTXO was created.  As this is not unique there
    will be potential collisions so tx_num is also in the key.  When
    looking up a UTXO the prefix space of the compressed hash needs to
    be searched and resolved if necessary with the tx_num.  The
    collision rate is low (<0.1%).
    '''

    def spend_utxo(self, tx_hash: bytes, tx_idx: int) -> bytes:
        '''Spend a UTXO and return (hashX + tx_num + value_sats).

        If the UTXO is not in the cache it must be on disk.  We store
        all UTXOs so not finding one indicates a logic error or DB
        corruption.
        '''
        # Fast track is it being in the cache
        idx_packed = pack_le_uint32(tx_idx)
        cache_value = self.utxo_cache.pop(tx_hash + idx_packed, None)
        if cache_value:
            return cache_value

        # Spend it from the DB.
        txnum_padding = bytes(8-TXNUM_LEN)

        # Key: b'h' + compressed_tx_hash + tx_idx + tx_num
        # Value: hashX
        prefix = b'h' + tx_hash[:COMP_TXID_LEN] + idx_packed
        candidates = {db_key: hashX for db_key, hashX
                      in self.db.utxo_db.iterator(prefix=prefix)}

        for hdb_key, hashX in candidates.items():
            tx_num_packed = hdb_key[-TXNUM_LEN:]

            if len(candidates) > 1:
                tx_num, = unpack_le_uint64(tx_num_packed + txnum_padding)
                hash, _height = self.db.fs_tx_hash(tx_num)
                if hash != tx_hash:
                    assert hash is not None  # Should always be found
                    continue

            # Key: b'u' + address_hashX + tx_idx + tx_num
            # Value: the UTXO value as a 64-bit unsigned integer
            udb_key = b'u' + hashX + hdb_key[-4-TXNUM_LEN:]
            utxo_value_packed = self.db.utxo_db.get(udb_key)
            if utxo_value_packed:
                # Remove both entries for this UTXO
                self.db_deletes.append(hdb_key)
                self.db_deletes.append(udb_key)
                return hashX + tx_num_packed + utxo_value_packed

        raise ChainError(f'UTXO {hash_to_hex_str(tx_hash)} / {tx_idx:,d} not '
                         f'found in "h" table')

    def spend_atomicals_utxo(self, tx_hash: bytes, tx_idx: int) -> bytes:
        '''Spend the atomicals entry for UTXO and return atomicals[].'''
        # Todo: For now we don't use a cache and we always use the disk.
        # Later we will find a way to store and query the entries efficiently for cache
        # Fast track is it being in the cache
        # idx_packed = pack_le_uint32(tx_idx)
        # cache_value = self.atomicals_utxo_cache.pop(tx_hash + idx_packed, None)
        # if cache_value:
        #     return cache_value
        idx_packed = pack_le_uint32(tx_idx)
        location_id = tx_hash + idx_packed
        cache_map = self.atomicals_utxo_cache.pop(location_id, None)
        
        if cache_map:
            spent_cache_values = []
            for key in cache_map.keys(): 
                value = cache_map[key]
                spent_cache_values.append(location_id + key + value)
                self.logger.info("cache_map")
                self.logger.info(location_id.hex())
                self.logger.info(key.hex())
                self.logger.info(value.hex())
            return spent_cache_values
            
        # Search the locations of existing atomicals
        # Key:  b'i' + tx_hash + txout_idx + mint_tx_hash + mint_txout_idx
        # Value: hashX + scripthash + value
        prefix = b'i' + location_id
        found_at_least_one = False
        atomicals_data_list = []
        for atomical_db_key, atomical_db_value in self.db.utxo_db.iterator(prefix=prefix):
            # Remove the 2 entries for atomicals utxo
            # Note: we do not delete the b'a' key for the atomical id mapping because it gets updated elsewhere on rollback and advance
            # Get all of the atomicals for an address to be deleted
            hashX = atomical_db_value[ : HASHX_LEN ]
            kdb_key_prefix = b'k' + hashX + location_id
            k_list = []
            for atomical_kdb_key, not_used_value in self.db.utxo_db.iterator(prefix=kdb_key_prefix):
                k_list.append(atomical_kdb_key)

            for del_elem in k_list:
                self.db_deletes.append(del_elem)
                found_at_least_one = True

            if found_at_least_one == False: 
                raise IndexError(f'Did not find expected at least one entry for atomicals: {hash_to_hex_str(tx_hash)} / {tx_idx:,d} not '
                            f'found in "k" table')

            self.db_deletes.append(atomical_db_key)
            self.logger.info("adding atomical to atomicals data list to be spent")
            self.logger.info(atomical_db_key.hex())
            self.logger.info(atomical_db_value.hex())
            atomicals_data_list.append(atomical_db_key[ 1 : ] + atomical_db_value)
            self.logger.info("atomicals_data_list.append")
            self.logger.info(atomical_db_key.hex())
            self.logger.info(atomical_db_value.hex())
 
        # Return the full data spent
        return atomicals_data_list

    async def _process_prefetched_blocks(self):
        '''Loop forever processing blocks as they arrive.'''
        while True:
            if self.height == self.daemon.cached_height():
                if not self._caught_up_event.is_set():
                    await self._first_caught_up()
                    self._caught_up_event.set()
            await self.blocks_event.wait()
            self.blocks_event.clear()
            if self.reorg_count:
                await self.reorg_chain(self.reorg_count)
                self.reorg_count = 0
            else:
                blocks = self.prefetcher.get_prefetched_blocks()
                await self.check_and_advance_blocks(blocks)

    async def _first_caught_up(self):
        self.logger.info(f'caught up to height {self.height}')
        # Flush everything but with first_sync->False state.
        first_sync = self.db.first_sync
        self.db.first_sync = False
        await self.flush(True)
        if first_sync:
            self.logger.info(f'{electrumx.version} synced to '
                             f'height {self.height:,d}')
        # Reopen for serving
        await self.db.open_for_serving()

    async def _first_open_dbs(self):
        await self.db.open_for_sync()
        self.height = self.db.db_height
        self.tip = self.db.db_tip
        self.tx_count = self.db.db_tx_count
        self.atomical_count = self.db.db_atomical_count

    # --- External API

    async def fetch_and_process_blocks(self, caught_up_event):
        '''Fetch, process and index blocks from the daemon.

        Sets caught_up_event when first caught up.  Flushes to disk
        and shuts down cleanly if cancelled.

        This is mainly because if, during initial sync ElectrumX is
        asked to shut down when a large number of blocks have been
        processed but not written to disk, it should write those to
        disk before exiting, as otherwise a significant amount of work
        could be lost.
        '''
        self._caught_up_event = caught_up_event
        await self._first_open_dbs()
        try:
            async with OldTaskGroup() as group:
                await group.spawn(self.prefetcher.main_loop(self.height))
                await group.spawn(self._process_prefetched_blocks())
        # Don't flush for arbitrary exceptions as they might be a cause or consequence of
        # corrupted data
        except CancelledError:
            self.logger.info('flushing to DB for a clean shutdown...')
            await self.flush(True)

    def force_chain_reorg(self, count):
        '''Force a reorg of the given number of blocks.

        Returns True if a reorg is queued, false if not caught up.
        '''
        if self._caught_up_event.is_set():
            self.reorg_count = count
            self.blocks_event.set()
            return True
        return False


class DecredBlockProcessor(BlockProcessor):
    async def calc_reorg_range(self, count):
        start, count = await super().calc_reorg_range(count)
        if start > 0:
            # A reorg in Decred can invalidate the previous block
            start -= 1
            count += 1
        return start, count


class NameIndexBlockProcessor(BlockProcessor):

    def advance_txs(self, txs, is_unspendable):
        result = super().advance_txs(txs, is_unspendable)

        tx_num = self.tx_count - len(txs)
        script_name_hashX = self.coin.name_hashX_from_script
        update_touched = self.touched.update
        hashXs_by_tx = []
        append_hashXs = hashXs_by_tx.append

        for tx, _tx_hash in txs:
            hashXs = []
            append_hashX = hashXs.append

            # Add the new UTXOs and associate them with the name script
            for txout in tx.outputs:
                # Get the hashX of the name script.  Ignore non-name scripts.
                hashX = script_name_hashX(txout.pk_script)
                if hashX:
                    append_hashX(hashX)

            append_hashXs(hashXs)
            update_touched(hashXs)
            tx_num += 1

        self.db.history.add_unflushed(hashXs_by_tx, self.tx_count - len(txs))

        return result


class LTORBlockProcessor(BlockProcessor):

    def advance_txs(self, txs, is_unspendable):
        self.tx_hashes.append(b''.join(tx_hash for tx, tx_hash in txs))

        # Use local vars for speed in the loops
        undo_info = []
        tx_num = self.tx_count
        script_hashX = self.coin.hashX_from_script
        put_utxo = self.utxo_cache.__setitem__
        spend_utxo = self.spend_utxo
        undo_info_append = undo_info.append
        update_touched = self.touched.update
        to_le_uint32 = pack_le_uint32
        to_le_uint64 = pack_le_uint64

        hashXs_by_tx = [set() for _ in txs]

        # Add the new UTXOs
        for (tx, tx_hash), hashXs in zip(txs, hashXs_by_tx):
            add_hashXs = hashXs.add
            tx_numb = to_le_uint64(tx_num)[:TXNUM_LEN]

            for idx, txout in enumerate(tx.outputs):
                # Ignore unspendable outputs
                if is_unspendable(txout.pk_script):
                    continue

                # Get the hashX
                hashX = script_hashX(txout.pk_script)
                add_hashXs(hashX)
                put_utxo(tx_hash + to_le_uint32(idx),
                         hashX + tx_numb + to_le_uint64(txout.value))
            tx_num += 1

        # Spend the inputs
        # A separate for-loop here allows any tx ordering in block.
        for (tx, tx_hash), hashXs in zip(txs, hashXs_by_tx):
            add_hashXs = hashXs.add
            for txin in tx.inputs:
                if txin.is_generation():
                    continue
                cache_value = spend_utxo(txin.prev_hash, txin.prev_idx)
                undo_info_append(cache_value)
                add_hashXs(cache_value[:HASHX_LEN])

        # Update touched set for notifications
        for hashXs in hashXs_by_tx:
            update_touched(hashXs)

        self.db.history.add_unflushed(hashXs_by_tx, self.tx_count)

        self.tx_count = tx_num
        self.db.tx_counts.append(tx_num)

        return undo_info

    def backup_txs(self, txs, is_unspendable):
        undo_info = self.db.read_undo_info(self.height)
        if undo_info is None:
            raise ChainError(
                f'no undo information found for height {self.height:,d}'
            )

        # Use local vars for speed in the loops
        put_utxo = self.utxo_cache.__setitem__
        spend_utxo = self.spend_utxo
        add_touched = self.touched.add
        undo_entry_len = HASHX_LEN + TXNUM_LEN + 8

        # Restore coins that had been spent
        # (may include coins made then spent in this block)
        n = 0
        for tx, tx_hash in txs:
            for txin in tx.inputs:
                if txin.is_generation():
                    continue
                undo_item = undo_info[n:n + undo_entry_len]
                put_utxo(txin.prev_hash + pack_le_uint32(txin.prev_idx), undo_item)
                add_touched(undo_item[:HASHX_LEN])
                n += undo_entry_len

        assert n == len(undo_info)

        # Remove tx outputs made in this block, by spending them.
        for tx, tx_hash in txs:
            for idx, txout in enumerate(tx.outputs):
                # Spend the TX outputs.  Be careful with unspendable
                # outputs - we didn't save those in the first place.
                if is_unspendable(txout.pk_script):
                    continue

                # Get the hashX
                cache_value = spend_utxo(tx_hash, idx)
                hashX = cache_value[:HASHX_LEN]
                add_touched(hashX)

        self.tx_count -= len(txs)
