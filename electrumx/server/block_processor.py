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
from electrumx.lib.script import SCRIPTHASH_LEN, is_unspendable_legacy, is_unspendable_genesis
from electrumx.lib.util import (
    chunks, class_logger, pack_le_uint32, unpack_le_uint32, pack_le_uint64, unpack_le_uint64, pack_be_uint64, unpack_be_uint64, OldTaskGroup, pack_byte
)
from electrumx.lib.tx import Tx
from electrumx.server.db import FlushData, COMP_TXID_LEN, DB
from electrumx.server.history import TXNUM_LEN
from electrumx.lib.util_atomicals import is_valid_container_string_name, is_unspendable_payment_marker_atomical_id, pad_bytes64, MINT_SUBREALM_RULES_EFFECTIVE_BLOCKS, MINT_REALM_CONTAINER_TICKER_COMMIT_REVEAL_DELAY_BLOCKS, MINT_SUBREALM_REVEAL_PAYMENT_DELAY_BLOCKS, is_valid_dmt_op_format, is_compact_atomical_id, is_atomical_id_long_form_string, unpack_mint_info, parse_protocols_operations_from_witness_array, get_expected_output_index_of_atomical_nft, get_expected_output_indexes_of_atomical_ft, location_id_bytes_to_compact, is_valid_subrealm_string_name, is_valid_realm_string_name, is_valid_ticker_string, get_mint_info_op_factory

if TYPE_CHECKING:
    from electrumx.lib.coins import Coin
    from electrumx.server.env import Env
    from electrumx.server.controller import Notifications

from cbor2 import dumps, loads, CBORDecodeError
import pickle
import pylru
import regex 
import sys 

TX_HASH_LEN = 32
ATOMICAL_ID_LEN = 36
LOCATION_ID_LEN = 36
TX_OUTPUT_IDX_LEN = 4

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
        self.atomical_count = 0     # Total number of Atomicals minted (Includes all NFT/FT types)
        self._caught_up_event = None

        # Caches of unflushed items.
        self.headers = []
        self.tx_hashes = []
        self.undo_infos = []  # type: List[Tuple[Sequence[bytes], int]]
        self.atomicals_undo_infos = []  # type: List[Tuple[Sequence[bytes], int]]

        # UTXO cache
        self.utxo_cache = {}
        self.atomicals_utxo_cache = {}  # The cache of atomicals UTXOs
        self.general_data_cache = {}    # General data cache for atomicals related actions
        self.ticker_data_cache = {}     # Caches the tickers created
        self.realm_data_cache = {}      # Caches the realms created
        self.subrealm_data_cache = {}   # Caches the subrealms created
        self.container_data_cache = {}  # Caches the containers created
        self.distmint_data_cache = {}   # Caches the distributed mints created
        self.db_deletes = []

        # If the lock is successfully acquired, in-memory chain state
        # is consistent with self.height
        self.state_lock = asyncio.Lock()

        # Signalled after backing up during a reorg
        self.backed_up_event = asyncio.Event()

        self.atomicals_id_cache = pylru.lrucache(100000)
  
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
        return FlushData(self.height, self.tx_count, self.headers,
                         self.tx_hashes, self.undo_infos, self.utxo_cache,
                         self.db_deletes, self.tip,
                         self.atomical_count,
                         self.atomicals_undo_infos, self.atomicals_utxo_cache, self.general_data_cache, self.ticker_data_cache, 
                         self.realm_data_cache, self.subrealm_data_cache, self.container_data_cache, self.distmint_data_cache)

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


    # Get the mint information and LRU cache it for fast retrieval
    # Used for quickly getting the mint information for an atomical
    def get_atomicals_id_mint_info(self, atomical_id):
        result = None
        try:
            result = self.atomicals_id_cache[atomical_id]
        except KeyError:
            # Now check the mint cache (before flush to db)
            try:
                result = unpack_mint_info(self.general_data_cache[b'mi' + atomical_id])
            except KeyError:
                result = unpack_mint_info(self.db.get_atomical_mint_info_dump(atomical_id))
            self.atomicals_id_cache[atomical_id] = result
        return result 
    
    # Get basic atomical information in a format that can be attached to utxos in an RPC call
    # Must be called for known existing atomicals or will throw an exception
    def get_atomicals_id_mint_info_basic_struct(self, atomical_id):
        result = self.get_atomicals_id_mint_info(atomical_id)
        return {
            'atomical_id': location_id_bytes_to_compact(result['id']),
            'atomical_number': result['number'],
            'type': result['type']
        }

    # Get the expected payment amount and destination for an atomical subrealm
    def get_expected_subrealm_payment_info(self, found_atomical_id, current_height):
        # Lookup the subrealm atomical to obtain the details of which subrealm parent it is for
        found_atomical_mint_info = self.get_atomicals_id_mint_info(found_atomical_id)
        if found_atomical_mint_info:
            # Found the mint information. Use the mint details to determine the parent realm id and name requested
            # Along with the price that was expected according to the mint reveal height
            args_subrealm = found_atomical_mint_info['args'].get('request_subrealm')
            request_subrealm = found_atomical_mint_info['$request_subrealm']
            # Check that $request_subrealm was set because it will only be set if the basic validation succeeded
            # If it's not set, then the atomical subrealm mint was not valid on a basic level and must be rejected
            if not request_subrealm:
                return None
            # Sanity check
            assert(args_subrealm == request_subrealm)
            # More sanity checks on the formats and validity
            if isinstance(request_subrealm, str) and is_valid_subrealm_string_name(request_subrealm):
                # Validate that the current payment came in before MINT_SUBREALM_REVEAL_PAYMENT_DELAY_BLOCKS after the mint reveal of the atomical
                # This is done to ensure that payments must be made in a timely fashion or else someone else can claim the subrealm
                if not self.is_within_acceptable_blocks_for_subrealm_payment(mint_info):
                    # The first_location_height (mint/reveal height) is too old and this payment came in far too late
                    # Ignore the payment therefore.
                    return None
                # The parent realm id is in a compact form string to make it easier for users and developers
                # Only store the details if the pid is also set correctly
                request_parent_realm_id_compact = mint_info['args'].get('pid')
                parent_realm_id_compact = mint_info.get('$pid')
                parent_realm_id = mint_info['$pid_bytes']
                assert(request_parent_realm_id_compact == parent_realm_id_compact)
                if isinstance(parent_realm_id_compact, str) and is_compact_atomical_id(parent_realm_id_compact):
                    # We have a validated potential parent id, now look it up to see if the parent is a valid atomical
                    found_parent_mint_info = self.get_atomicals_id_mint_info(parent_realm_id)
                    if found_parent_mint_info:
                        # We have found the parent atomical, which may or may not be a valid realm
                        # Do the basic check for $request_realm which indicates it succeeded the basic validity checks
                        args_realm = found_parent_mint_info['args'].get('request_realm')
                        request_realm = found_parent_mint_info['$request_realm']
                        # One or both was empty and therefore didn't pass the basic checks
                        # Someone apparently made a payment marker for an invalid parent realm id. They made a mistake, ignoring it..
                        if not args_realm or not request_subrealm: 
                            return None
                        # Make sure it's the right type and format checks pass again just in case
                        if not isinstance(request_realm, str) or not is_valid_realm_string_name(request_realm):
                            return None 
                        # At this point we know we have a valid parent, but because realm allocation is delayed by MINT_REALM_CONTAINER_TICKER_COMMIT_REVEAL_DELAY_BLOCKS
                        # ... we do not actually know if the parent was awarded the realm or not until the required heights are met
                        # Nonetheless, someone DID make a payment and referenced the parent by the specific atomical id and therefore we will try to apply to payment
                        # It does not mean in the end that they actually get the subrealm if they paid the wrong parent. But that's their mistake and was easily avoided
                        # Here we go and check for the required payment amount and details now...
                        matched_price_point = self.get_matched_price_point_for_subrealm_name_by_height(parent_atomical_id, request_subrealm, current_height)
                        if matched_price_point:
                            return matched_price_point, parent_atomical_id, request_subrealm
        return None

    # Save the subrealm payment
    def put_subrealm_payment(self, parent_atomical_id, atomical_id, subrealm_name, tx_hash_idx_of_payment): 
        # Retrieve the subrealm index record first
        # subject_enc = subject.encode() 
        # Check if it's located in the cache first
        # name_map = name_data_cache.get(prefix_key + subject_enc)
        # cached_atomical_id = None
        # if name_map:
        #     atomical_id = name_map.get(commit_tx_num)
        #     if atomical_id:
        #          if atomical_id != expected_entry_value:
        #             raise IndexerError(f'IndexerError: delete_name_element_template {db_delete_prefix} cache name data does not match expected atomical id {commit_tx_num} {expected_atomical_id} {subject} {atomical_id}')
        #         # remove from the cache
        #         name_map.pop(commit_tx_num)
            # Intentionally fall through to catch it in the db as well just in case
        # Check the db whether or not it was in the cache as a safety measure (todo: Can be removed later as codebase proves robust)
        # db_delete_key = db_delete_prefix + prefix_key + subject_enc + pack_le_uint64(commit_tx_num)
        # atomical_data_from_db = self.db.utxo_db.get(db_delete_key)
        # if atomical_data_from_db:
        #     if atomical_data_from_db != expected_entry_value: 
        #         raise IndexerError(f'IndexerError: delete_name_element_template {db_delete_prefix} db data does not match expected atomical id {commit_tx_num} {expected_atomical_id} {subject} {name_data_from_db}')
        #     self.db_deletes.append(db_delete_key)
        #     self.logger.info(f'IndexerError: delete_name_element_template {db_delete_prefix} deletes subject={subject}, expected_entry_value={expected_entry_value.hex()}')
        # if cached_atomical_id or atomical_data_from_db:
        #     return True

        # Ensure that the subrealm record had no payment associated with it; do not allow overwriting

        # Save the payment record
        i = 'todo'
    
    # Delete the subrealm payment
    def delete_subrealm_payment(self, parent_atomical_id, atomical_id, subrealm_name, tx_hash_idx_of_payment): 
        i = 'todo'

    # Save distributed mint infromation for the atomical
    # Mints are only stored if they are less than the max_mints amount
    def put_distmint_data(self, atomical_id, location_id, value): 
        self.logger.info(f'put_distmint_data: atomical_id={atomical_id.hex()}, location_id={location_id.hex()}, value={value}')
        if self.distmint_data_cache.get(atomical_id) == None: 
            self.distmint_data_cache[atomical_id] = {}
        self.distmint_data_cache[atomical_id][location_id] = value

    # Save atomicals UTXO to cache that will be flushed to db
    def put_atomicals_utxo(self, location_id, atomical_id, value): 
        self.logger.info(f'put_atomicals_utxo: atomical_id={atomical_id.hex()}, location_id={location_id.hex()}, value={value}')
        if self.atomicals_utxo_cache.get(location_id) == None: 
            self.atomicals_utxo_cache[location_id] = {}
        # Use a tombstone to mark deleted because even if it's removed we must
        # store the b'i' value
        self.atomicals_utxo_cache[location_id][atomical_id] = {
            'deleted': False,
            'value': value
        }

    # Get the total number of distributed mints for an atomical id and check the cache and db
    # This can be a heavy operation with many 10's of thousands in the db
    def get_distmints_count_by_atomical_id(self, atomical_id):
        # Count the number of mints in the cache and add it to the number of mints in the db below
        cache_count = 0
        location_map_for_atomical = self.distmint_data_cache.get(atomical_id, None)
        if location_map_for_atomical != None:
            cache_count = len(location_map_for_atomical.keys())
        # Query all the gi key in the db for the atomical
        prefix = b'gi' + atomical_id
        db_count = 0
        for atomical_gi_db_key, atomical_gi_db_value in self.db.utxo_db.iterator(prefix=prefix):
            db_count += 1
        # The number minted is equal to the cache and db
        total_mints = cache_count + db_count
        # Some sanity checks to make sure no developer error 
        assert(cache_count >= 0)
        assert(db_count >= 0)
        assert(total_mints >= 0)
        assert(isinstance(total_mints, int))
        return total_mints

    # Spend all of the atomicals at a location
    def spend_atomicals_utxo(self, tx_hash: bytes, tx_idx: int) -> bytes:
        '''Spend the atomicals entry for UTXO and return atomicals[].'''
        idx_packed = pack_le_uint32(tx_idx)
        location_id = tx_hash + idx_packed
        cache_map = self.atomicals_utxo_cache.get(location_id)
        if cache_map:
            self.logger.info(f'spend_atomicals_utxo.cache_map: location_id={location_id.hex()} has Atomicals...')
            atomicals_data_list_cached = []
            for key in cache_map.keys(): 
                value_with_tombstone = cache_map[key]
                value = value_with_tombstone['value']
                is_sealed = value[HASHX_LEN + SCRIPTHASH_LEN + 8:]
                if is_sealed == b'00':
                    # Only allow it to be spent if not sealed  
                    atomicals_data_list_cached.append({
                        'atomical_id': key,
                        'location_id': location_id,
                        'data': value
                    })
                    value_with_tombstone['deleted'] = True  # Flag it as deleted so the b'a' active location will not be written on flushed
                self.logger.info(f'spend_atomicals_utxo.cache_map: location_id={location_id.hex()} atomical_id={key.hex()}, is_sealed={is_sealed}, value={value}')
            if len(atomicals_data_list_cached) > 0:
                return atomicals_data_list_cached
        # Search the locations of existing atomicals
        # Key:  b'i' + location(tx_hash + txout_idx) + atomical_id(mint_tx_hash + mint_txout_idx)
        # Value: hashX + scripthash + value
        prefix = b'i' + location_id
        found_at_least_one = False
        atomicals_data_list = []
        for atomical_i_db_key, atomical_i_db_value in self.db.utxo_db.iterator(prefix=prefix):
            # Get all of the atomicals for an address to be deleted
            atomical_id = atomical_i_db_key[1 + ATOMICAL_ID_LEN:]
            is_sealed = atomical_i_db_value[HASHX_LEN + SCRIPTHASH_LEN + 8:]
            prefix = b'a' + atomical_id + location_id
            found_at_least_one = False
            for atomical_a_db_key, atomical_a_db_value in self.db.utxo_db.iterator(prefix=prefix):
                found_at_least_one = True
            if found_at_least_one == False: 
                raise IndexError(f'Did not find expected at least one entry for atomicals table for atomical: {atomical_id.hex()} at location {location.hex()}')
            # Only allow the deletion/spending if it wasn't sealed
            if is_sealed == b'00':
                self.db_deletes.append(b'a' + atomical_id + location_id)
                atomicals_data_list.append({
                    'atomical_id': atomical_id,
                    'location_id': location_id,
                    'data': atomical_i_db_value
                })
            self.logger.info(f'spend_atomicals_utxo.utxo_db: location_id={location_id.hex()} atomical_id={atomical_id.hex()}, is_sealed={is_sealed}, value={atomical_i_db_value}')
            # Return all of the atomicals spent at the address
        return atomicals_data_list

    # Put a name element template to the cache
    def put_name_element_template(self, atomical_id_value, subject, commit_tx_num, name_data_cache, is_valid_name_string_func, db_prefix_key, subject_prefix_key=b''): 
        self.logger.info(f'put_name_element_template: atomical_id_value={atomical_id_value.hex()}, subject={subject}, commit_tx_num={commit_tx_num}')
        if not is_valid_name_string_func(subject):
            raise IndexError(f'put_name_element_template subject invalid {subject}')
        subject_enc = subject.encode()
        record_key = db_prefix_key + subject_prefix_key + subject_enc
        if not name_data_cache.get(record_key):
            name_data_cache[record_key] = {}
        # subject_prefix_key is the potential parent realm id for subrealms only, empty for everything else
        name_data_cache[record_key][commit_tx_num] = atomical_id_value

    def delete_name_element_template(self, expected_entry_value, subject, commit_tx_num, name_data_cache, db_get_name_func, db_delete_prefix, subject_prefix_key=b''): 
        subject_enc = subject.encode() 
        record_key = db_prefix_key + subject_prefix_key + subject_enc
        # Check if it's located in the cache first
        name_map = name_data_cache.get(record_key)
        cached_atomical_id = None
        if name_map:
            atomical_id = name_map.get(commit_tx_num)
            if atomical_id:
                if atomical_id != expected_entry_value:
                    raise IndexerError(f'IndexerError: delete_name_element_template {db_delete_prefix} cache name data does not match expected atomical id {commit_tx_num} {expected_atomical_id} {subject} {atomical_id}')
                # remove from the cache
                name_map.pop(commit_tx_num)
            # Intentionally fall through to catch it in the db as well just in case
        # Check the db whether or not it was in the cache as a safety measure (todo: Can be removed later as codebase proves robust)
        db_delete_key = record_key + pack_le_uint64(commit_tx_num)
        atomical_data_from_db = self.db.utxo_db.get(db_delete_key)
        if atomical_data_from_db:
            if atomical_data_from_db != expected_entry_value: 
                raise IndexerError(f'IndexerError: delete_name_element_template {db_delete_prefix} db data does not match expected atomical id {commit_tx_num} {expected_atomical_id} {subject} {name_data_from_db}')
            self.db_deletes.append(db_delete_key)
            self.logger.info(f'IndexerError: delete_name_element_template {db_delete_prefix} deletes subject={subject}, expected_entry_value={expected_entry_value.hex()}')
        if cached_atomical_id or atomical_data_from_db:
            return True
  
    # Delete the distributed mint data that is used to track how many mints were made
    def delete_distmint_data(self, atomical_id, location_id) -> bytes:
        cache_map = self.distmint_data_cache.get(atomical_id, None)
        if cache_map != None:
            cache_map.pop(location_id, None)
            self.logger.info(f'delete_distmint_data.distmint_data_cache: location_id={location_id.hex()}, atomical_id={atomical_id.hex()}')
        gi_key = b'gi' + atomical_id + location_id
        gi_value = self.db.utxo_db.get(gi_key)
        if gi_value:
            # not do the i entry beuse it's deleted elsewhere 
            self.db_deletes.append(b'gi' + atomical_id + location_id)
            self.logger.info(f'delete_distmint_data.db_deletes: location_id={location_id.hex()}, atomical_id={atomical_id.hex()}')

    def log_subrealm_request(self, method, msg, status, subrealm, parent_realm_atomical_id, height):
        self.logger.info(f'{method} - {msg}, status={status} subrealm={subrealm}, parent_realm_atomical_id={parent_realm_atomical_id.hex()}, height={height}')
     
    def log_can_be_created(self, method, msg, subject, validity, val):
        self.logger.info(f'{method} - {msg}: {subject} value {val} is acceptable to be created: {validity}')
   
    # Validate the parameters for an NFT and validate realm/subrealm/container data
    def validate_and_create_nft_mint_utxo(self, mint_info, txout, height, tx_hash):
        if not mint_info or not isinstance(mint_info, dict):
            return False
        #tx_numb = pack_le_uint64(mint_info['tx_num'])[:TXNUM_LEN]
        value_sats = pack_le_uint64(mint_info['first_location_value'])
        # Save the initial location to have the atomical located there
        is_sealed = b'00'
        self.put_atomicals_utxo(mint_info['first_location'], mint_info['id'], mint_info['first_location_hashX'] + mint_info['first_location_scripthash'] + value_sats + is_sealed)
        atomical_id = mint_info['id']
        self.logger.info(f'Atomicals Create NFT in reveal tx {hash_to_hex_str(tx_hash)}, atomical_id={location_id_bytes_to_compact(atomical_id)}, tx_hash={hash_to_hex_str(tx_hash)}, mint_info={mint_info}')
        return True
    
    # Validate the parameters for a FT
    def validate_and_create_ft_mint_utxo(self, mint_info, tx_hash):
        self.logger.info(f'validate_and_create_ft_mint_utxo: tx_hash={hash_to_hex_str(tx_hash)}')
        #tx_numb = pack_le_uint64(mint_info['tx_num'])[:TXNUM_LEN]
        value_sats = pack_le_uint64(mint_info['first_location_value'])
        # Save the initial location to have the atomical located there
        if mint_info['subtype'] != 'distributed':
            is_sealed = b'00'
            self.put_atomicals_utxo(mint_info['first_location_location'], mint_info['id'], mint_info['first_location_hashX'] + mint_info['first_location_scripthash'] + value_sats + is_sealed)
        subtype = mint_info['subtype']
        self.logger.info(f'Atomicals Create FT in reveal tx {hash_to_hex_str(tx_hash)}, subtype={subtype}, atomical_id={location_id_bytes_to_compact(atomical_id)}, tx_hash={hash_to_hex_str(tx_hash)}')
        return True

    def get_tx_num_height_from_tx_hash(self, tx_hash):
        tx_hash_value = self.general_data_cache.get(b'tx' + tx_hash)
        if tx_hash_value:
            unpacked_tx_num, = unpack_le_uint64(tx_hash_value[:8])
            unpacked_height, = unpack_le_uint32(tx_hash_value[-4:])
            return unpacked_tx_num, unpacked_height
        return self.db.get_tx_num_height_from_tx_hash(tx_hash)

    def create_realm_entry_if_requested(self, mint_info):
        if is_valid_realm_string_name(mint_info.get('$request_realm')):
            self.put_name_element_template(mint_info['id'], mint_info.get('$request_realm'), mint_info['commit_tx_num'], self.realm_data_cache, is_valid_realm_string_name, b'rlm')

    def delete_realm_entry_if_requested(self, mint_info):
        if is_valid_realm_string_name(mint_info.get('$request_realm')):
            self.delete_name_element_template(mint_info['id'], mint_info.get('$request_realm'), mint_info['commit_tx_num'], self.realm_data_cache, b'rlm')
    
    def create_container_entry_if_requested(self, mint_info):
        if is_valid_container_string_name(mint_info.get('$request_container')):
            self.put_name_element_template(mint_info['id'], mint_info.get('$request_container'), mint_info['commit_tx_num'], self.container_data_cache, is_valid_container_string_name, b'co')
    
    def delete_container_entry_if_requested(self, mint_info):
        if is_valid_container_string_name(mint_info.get('$request_container')):
            self.delete_name_element_template(mint_info['id'], mint_info.get('$request_container'), mint_info['commit_tx_num'], self.container_data_cache, b'co')

    def create_ticker_entry_if_requested(self, mint_info):
        if is_valid_ticker_string(mint_info.get('$request_ticker')):
            self.put_name_element_template(mint_info['id'], mint_info.get('$request_ticker'), mint_info['commit_tx_num'], self.ticker_data_cache, is_valid_ticker_string_name, b'tick')

    def delete_ticker_entry_if_requested(self, mint_info):
        if is_valid_ticker_string_name(mint_info.get('$request_ticker')):
            self.delete_name_element_template(mint_info['id'], mint_info.get('$request_ticker'), mint_info['commit_tx_num'], self.ticker_data_cache, b'tick')

    # Check for the payment and parent information for a subrealm mint request
    # This information is used to determine how to put and delete the record in the index
    def get_subrealm_parent_realm_payment_info(self, mint_info, atomicals_spent_at_inputs): 
        if not is_valid_subrealm_string_name(mint_info.get('$request_subrealm')):
            return None, None
        # Check to see if the parent realm was spent as part of the inputs to authorize the direct creation of the subrealm
        # If the parent realm was spent as one of the inputs, then there does not need to be a payment made, we consider the current transaction
        # as the payment then
        parent_realm_id = mint_info['$pid_bytes']
        initiated_by_parent = False
        for idx, atomical_entry_list in atomicals_spent_at_inputs.items():
            for atomical_entry in atomical_entry_list:
                atomical_id = atomical_entry['atomical_id']
                if atomical_id == parent_realm_id:
                    initiated_by_parent = True
                    break
            # parent atomical matches being spent
            if initiated_by_parent:
                break
        # By default the payment hash is all zeroes, awaiting a payment to be made later
        payment_tx_outpoint = b'000000000000000000000000000000000000000000000000000000000000000000000000'
        if initiated_by_parent:
            # However if it was initiated by the parent, therefore simply assign the payment tx hash as the reveal (first_location_txid) of the subrealm nft mint
            payment_tx_outpoint = mint_info['first_location_txid'] + pack_le_uint32(0)
        return parent_realm_id, payment_tx_outpoint

    # Create the subrealm entry if requested correctly
    def create_subrealm_entry_if_requested(self, mint_info, atomicals_spent_at_inputs): 
        parent_realm_id, payment_tx_outpoint = self.get_subrealm_parent_realm_payment_info(mint_info, atomicals_spent_at_inputs)
        if parent_realm_id:
            self.put_name_element_template(mint_info['id'] + payment_tx_outpoint, mint_info.get('$request_subrealm'), mint_info['commit_tx_num'], self.subrealm_data_cache, is_valid_subrealm_string_name, b'srlm', parent_realm_id)

    # Delete the subrealm created entry
    # Be careful to determine if it was created with the parent realm or not
    # Because if it was creatd with the parent realm then there will be no future payment and the tx itself is considered the payment
    def delete_subrealm_entry_if_requested(self, mint_info, atomicals_spent_at_inputs):
        parent_realm_id, payment_tx_outpoint = self.get_subrealm_parent_realm_payment_info(mint_info, atomicals_spent_at_inputs)
        if parent_realm_id:
            self.delete_name_element_template(mint_info['id'] + payment_tx_outpoint, mint_info.get('$request_subrealm'), mint_info['commit_tx_num'], self.subrealm_data_cache, b'srlm', parent_realm_id)

    def is_within_acceptable_blocks_for_name_reveal(self, mint_info):
        return mint_info['commit_height'] >= mint_info['first_location_height'] - MINT_REALM_CONTAINER_TICKER_COMMIT_REVEAL_DELAY_BLOCKS

    def is_within_acceptable_blocks_for_subrealm_payment(self, mint_info):
        return mint_info['commit_height'] >= mint_info['first_location_height'] - MINT_SUBREALM_REVEAL_PAYMENT_DELAY_BLOCKS

    # Check whether to create an atomical NFT/FT 
    # Validates the format of the detected input operation and then checks the correct extra data is valid
    # such as realm, container, ticker, etc. Only succeeds if the appropriate names can be assigned
    def create_atomical(self, operations_found_at_inputs, atomicals_spent_at_inputs, header, height, tx_num, atomical_num, tx, tx_hash):
        if not operations_found_at_inputs:
            return None
        # Catch the strange case where there are no outputs
        if len(tx.outputs) == 0:
            return None

        # All mint types always look at only input 0 to determine if the operation was found
        # This is done to preclude complex scenarios of valid/invalid different mint types across inputs 
        valid_create_op_type, mint_info = get_mint_info_op_factory(self.coin.hashX_from_script, tx, tx_hash, operations_found_at_inputs)
        if not valid_create_op_type or (valid_create_op_type != 'NFT' and valid_create_op_type != 'FT'):
            self.logger.info(f'create_atomical not valid type: {tx_hash}')
            return None

        # The atomical would always be created at the first output
        txout = tx.outputs[0]

        # The prev tx number is the prev input being spent that creates the atomical
        commit_tx_num, commit_tx_height = self.get_tx_num_height_from_tx_hash(mint_info['commit_txid'])
        if not commit_tx_num:
            raise IndexError(f'Indexer error retrieved null commit_tx_num')

        atomical_id = mint_info['id']
        mint_info['number'] = atomical_num 
        # The mint tx num is used to determine precedence for names like tickers, realms, containers
        mint_info['commit_tx_num'] = commit_tx_num 
        mint_info['commit_height'] = commit_tx_height 
        mint_info['first_location_header'] = header 
        mint_info['first_location_height'] = height 
        mint_info['first_location_tx_num'] = tx_num 
        
        if valid_create_op_type == 'NFT':
            if not self.validate_and_create_nft_mint_utxo(mint_info, txout, height, tx_hash):
                self.logger.info(f'Atomicals Create NFT validate_and_create_nft_mint_utxo returned FALSE in Transaction {hash_to_hex_str(tx_hash)}') 
                return None
            if self.is_within_acceptable_blocks_for_name_reveal(mint_info):
                self.create_realm_entry_if_requested(mint_info)
                self.create_subrealm_entry_if_requested(mint_info, atomicals_spent_at_inputs)
                self.create_container_entry_if_requested(mint_info)
        elif valid_create_op_type == 'FT':
            if not self.validate_and_create_ft_mint_utxo(mint_info, tx_hash):
                self.logger.info(f'Atomicals Create FT validate_and_create_ft_mint_utxo returned FALSE in Transaction {hash_to_hex_str(tx_hash)}') 
                return None
            # Add $max_supply informative property
            if mint_info['subtype'] == 'distributed':
                mint_info['$max_supply'] = mint_info['$mint_amount'] * mint_info['$max_mints'] 
            else: 
                mint_info['$max_supply'] = txout.value
            if self.is_within_acceptable_blocks_for_name_reveal(mint_info):
                self.create_ticker_entry_if_requested(mint_info)
        else: 
            raise IndexError(f'Fatal index error Create Invalid')
        
        # Save mint data fields
        put_general_data = self.general_data_cache.__setitem__
        put_general_data(b'md' + atomical_id, operations_found_at_inputs['payload_bytes'])
        # Save mint info fields and metadata
        put_general_data(b'mi' + atomical_id, pickle.dumps(mint_info))
        # Track the atomical number for the newly minted atomical
        atomical_count_numb = pack_be_uint64(atomical_num)
        put_general_data(b'n' + atomical_count_numb, atomical_id)
        # Save the output script of the atomical commit and reveal mint outputs to lookup at a future point for resolving address script
        put_general_data(b'po' + atomical_id, txout.pk_script)
        put_general_data(b'po' + mint_info['first_location'], txout.pk_script)
        return atomical_id

    # Build a map of atomical id to the type, value, and input indexes
    # This information is used below to assess which inputs are of which type and therefore which outputs to color
    def build_atomical_id_info_map(self, map_atomical_ids_to_info, atomicals_entry_list, txin_index):
        for atomicals_entry in atomicals_entry_list:
            atomical_id = atomicals_entry['atomical_id']
            value, = unpack_le_uint64(atomicals_entry['data'][-8:])
            atomical_mint_info = self.get_atomicals_id_mint_info(atomical_id)
            if not atomical_mint_info: 
                raise IndexError(f'color_atomicals_outputs {atomical_id.hex()} not found in mint info. IndexError.')
            if map_atomical_ids_to_info.get(atomical_id, None) == None:
                map_atomical_ids_to_info[atomical_id] = {
                    'atomical_id': atomical_id,
                    'type': atomical_mint_info['type'],
                    'value': 0,
                    'input_indexes': []
                }
            map_atomical_ids_to_info[atomical_id]['value'] += value
            map_atomical_ids_to_info[atomical_id]['input_indexes'].append(txin_index)
        return map_atomical_ids_to_info
    
    # Get the expected output indexes to color for the atomicals based on the atomical types and input conditions
    def get_expected_output_indexes_to_color(self, operations_found_at_inputs, mint_info, tx, atomical_id):
        if mint_info['type'] == 'NFT':
            assert(len(mint_info['input_indexes']) == 1)
            self.logger.info(f'get_expected_output_indexes_to_color {atomical_id} {operations_found_at_inputs}')
            expected_output_indexes = [get_expected_output_index_of_atomical_nft(mint_info, tx, atomical_id, operations_found_at_inputs)]
        elif mint_info['type'] == 'FT':
            assert(len(mint_info['input_indexes']) >= 1)
            expected_output_indexes = get_expected_output_indexes_of_atomical_ft(mint_info, tx, atomical_id, operations_found_at_inputs) 
        else:
            raise IndexError('color_atomicals_outputs: Unknown type. Index Error.')

    # Detect and apply updates-related like operations for an atomical such as mod/evt/crt/sl
    def apply_state_like_updates(operations_found_at_inputs, mint_info, atomical_id, tx_numb, output_idx_le, height):
        if not apply_state_like_updates:
            return 

        put_general_data = self.general_data_cache.__setitem__
        if operations_found_at_inputs and operations_found_at_inputs.get('op') == 'mod' and operations_found_at_inputs.get('input_index') == 0:
            self.logger.info(f'apply_state_like_updates op=mod, height={height}, atomical_id={atomical_id.hex()}, tx_numb={tx_numb}')
            put_general_data(b'mod' + atomical_id + tx_numb + output_idx_le, operations_found_at_inputs['payload_bytes'])
            # If the mod(ify) operation path was set to '/subrealm-mint' with prices field
            # Then validate they are in the correct format
            mod_path = operations_found_at_inputs['payload'].get('path')
            if mod_path and len(mod_path.encode()) <= 64:
                mod_path_padded = pad_bytes64(mod_path.encode())
                height_packed = pack_le_uint32(height)
                put_general_data(b'modpath' + atomical_id + mod_path_padded + tx_numb + output_idx_le + height_packed, pickle.dumps(operations_found_at_inputs['payload']))
        elif operations_found_at_inputs and operations_found_at_inputs.get('op') == 'evt' and operations_found_at_inputs.get('input_index') == 0:
            self.logger.info(f'apply_state_like_updates op=evt, height={height}, atomical_id={atomical_id.hex()}, tx_numb={tx_numb}')
            put_general_data(b'evt' + atomical_id + tx_numb + output_idx_le, operations_found_at_inputs['payload_bytes'])

    # Apply the rules to color the outputs of the atomicals
    def color_atomicals_outputs(self, operations_found_at_inputs, atomicals_spent_at_inputs, tx, tx_hash, tx_numb, height):
        '''color_atomicals_outputs Tags or colors the outputs in a given transaction according to the NFT/FT atomicals rules
        :param operations_found_at_inputs The operations for at the inputs to the transaction (under the atom envelope)
        :param atomicals_spent_at_inputs The atomicals that were found to be transferred at the given inputs
        :param tx Transaction context
        :param tx_hash Transaction hash of the context
        :param tx_numb Global transaction number
        '''
        put_general_data = self.general_data_cache.__setitem__
        map_atomical_ids_to_info = {}
        atomical_ids_touched = []
        for txin_index, atomicals_entry_list in atomicals_spent_at_inputs.items():
            # Accumulate the total input value by atomical_id
            # The value will be used below to determine the amount of input we can allocate for FT's
            self.build_atomical_id_info_map(map_atomical_ids_to_info, atomicals_entry_list, txin_index)
        
        # For each atomical, get the expected output indexes to color
        for atomical_id, mint_info in map_atomical_ids_to_info.items():
            expected_output_indexes = self.get_expected_output_indexes_to_color(operations_found_at_inputs, mint_info, tx, atomical_id)

            # For each expected output to be colored, check for state-like updates
            for expected_output_index in expected_output_indexes:
                output_idx_le = pack_le_uint32(expected_output_index)
                location = tx_hash + output_idx_le
                txout = tx.outputs[expected_output_index]
                scripthash = double_sha256(txout.pk_script)
                hashX = self.coin.hashX_from_script(txout.pk_script)
                value_sats = pack_le_uint64(txout.value)
                put_general_data(b'po' + location, txout.pk_script)
                self.apply_state_like_updates(operations_found_at_inputs, mint_info, atomical_id, tx_numb, output_idx_le, height)
                is_sealed = b'00'
                # Only allow the NFT collection subtype to be sealed
                if operations_found_at_inputs and operations_found_at_inputs.get('op') == 'sl' and mint_info['type'] == 'NFT' and operations_found_at_inputs.get('input_index') == 0:
                    is_sealed = b'01'
                self.put_atomicals_utxo(location, atomical_id, hashX + scripthash + value_sats + is_sealed)
            
            atomical_ids_touched.append(atomical_id)
        return atomical_ids_touched
    
        name_data_cache, self.db.get_effective_ticker

    # Get the effectivee name after the required number of blocks has past
    def get_effective_name_template(self, subject, current_height, name_data_cache, db_get_effective_func):
        # Get the effective name entries
        all_entries = []
        found_atomical_id, entries = db_get_effective_func(subject)
        for key, v in self.name_data_cache.items():
            for commit_tx_num, atomical_id in v.items():
                all_entries.append({
                    'atomical_id': atomical_id,
                    'commit_tx_num': commit_tx_num
                })
        all_entries.extend(entries)
        entries.sort(key=lambda x: x.commit_tx_num)
        if len(entries) > 0:
            # Get the commit_tx_num and use that to get the height 
            candidate_entry = entries[0]
            # Convert to using the db lookup
            commit_tx_hash, commit_height = self.db.fs_tx_hash(candidate_entry['commit_tx_num'])
            if commit_height <= current_height - MINT_REALM_CONTAINER_TICKER_COMMIT_REVEAL_DELAY_BLOCKS:
                return candidate_entry['atomical_id']
        return None 
 
    # Create a distributed mint output as long as the rules are satisfied
    def create_distmint_output(self, atomicals_operations_found_at_inputs, tx_hash, tx, height):
        if not atomicals_operations_found_at_inputs:
            return
        dmt_valid, dmt_return_struct = is_valid_dmt_op_format(tx_hash, atomicals_operations_found_at_inputs)
        if not dmt_valid:
            return None

        # get the potential dmt (distributed mint) atomical_id from the ticker given
        potential_dmt_atomical_id = self.get_effective_name_template(dmt_return_struct['$mint_ticker'], height, self.ticker_data_cache, self.db.get_effective_ticker)
        if not potential_dmt_atomical_id:
            self.logger.info(f'potential_dmt_atomical_id not found for dmt operation in {tx_hash}. Attempt was made for a non-existant ticker mint info. Ignoring...')
            return None 

        mint_info_for_ticker = self.get_atomicals_id_mint_info(potential_dmt_atomical_id)
        if not mint_info_for_ticker:
            raise IndexError(f'create_distmint_outputs mint_info_for_ticker not found for expected atomical {atomical_id}')
        
        if mint_info_for_ticker['subtype'] != 'distributed':
            self.logger.info(f'create_distmint_outputs Detected invalid mint attempt in {tx_hash} for ticker {ticker} which is not a distributed mint type. Ignoring...')
            return None 

        max_mints = mint_info_for_ticker['$max_mints']
        mint_amount = mint_info_for_ticker['$mint_amount']
        mint_height = mint_info_for_ticker['$mint_height']
        mint_ticker = mint_info_for_ticker['$ticker']

        if mint_ticker != dmt_return_struct['$mint_ticker']:
            dmt_ticker = dmt_return_struct['$mint_ticker']
            raise IndexError(f'create_distmint_outputs Fatal developer error with incorrect storage and retrieval of mint ticker for {tx_hash} {dmt_ticker}')
        
        if height < mint_height:
            self.logger.info(f'create_distmint_outputs found premature mint operation in {tx_hash} for {ticker} in {height} before {mint_height}. Ignoring...')
            return None

        expected_output_index = 0
        output_idx_le = pack_le_uint32(expected_output_index) 
        location = tx_hash + output_idx_le
        txout = tx.outputs[expected_output_index]
        scripthash = double_sha256(txout.pk_script)
        hashX = self.coin.hashX_from_script(txout.pk_script)
        value_sats = pack_le_uint64(txout.value)
        # Mint is valid and active if the value is what is expected
        if mint_amount == txout.value:
            # Count the number of existing b'gi' entries and ensure it is strictly less than max_mints
            distributed_mints = self.get_distmints_count_by_atomical_id(potential_dmt_atomical_id)
            if distributed_mints > max_mints:
                raise IndexError(f'create_distmint_outputs Fatal IndexError distributed_max_mints > max_mints for {atomical}. Too many mints detected in db')
                
            if distributed_max_mints < max_mints:
                self.logger.info(f'create_distmint_outputs found valid mint in {tx_hash} for {ticker}. Creating distributed mint record...')
                put_general_data(b'po' + location, txout.pk_script)
                is_sealed = b'00' # FT outputs can never be sealed
                self.put_atomicals_utxo(location, potential_dmt_atomical_id, hashX + scripthash + value_sats + is_sealed)
                self.put_distmint_data(potential_dmt_atomical_id, location, scripthash + value_sats)
            else: 
                self.logger.info(f'create_distmint_outputs found invalid mint operation because it is minted out completely. Ignoring...')
        else: 
            self.logger.info(f'create_distmint_outputs found invalid mint operation in {tx_hash} for {ticker} because incorrect txout.value {txout.value} when expected {mint_amount}')
        return potential_dmt_atomical_id

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
        put_general_data = self.general_data_cache.__setitem__
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
            atomicals_spent_at_inputs = {}
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
                    atomicals_spent_at_inputs[txin_index] = atomicals_transferred_list
                    for atomical_spent in atomicals_transferred_list:
                        atomical_id = atomical_spent['atomical_id']
                        self.logger.info(f'atomicals_transferred_list - tx_hash={hash_to_hex_str(tx_hash)}, txin_index={txin_index}, txin_hash={hash_to_hex_str(txin.prev_hash)}, txin_previdx={txin.prev_idx}, atomical_id_spent={atomical_id.hex()}')
                # Get the undo format for the spent atomicals
                reformatted_for_undo_entries = []
                for atomicals_entry in atomicals_transferred_list:
                    reformatted_for_undo_entries.append(atomicals_entry['location_id'] + atomicals_entry['atomical_id'] + atomicals_entry['data'])
                atomicals_undo_info_extend(reformatted_for_undo_entries)
                txin_index = txin_index + 1
            
            # Save the tx number for the current tx
            # This index is used to lookup the height of a commit tx when minting an atomical
            # For example if the reveal of a realm/container/ticker mint is greater than 
            # MINT_REALM_CONTAINER_TICKER_COMMIT_REVEAL_DELAY_BLOCKS then the realm request is invalid. 
            put_general_data(b'tx' + tx_hash, to_le_uint64(tx_num) + to_le_uint32(height))

            # Detect all protocol operations in the transaction witness inputs
            atomicals_operations_found_at_inputs = parse_protocols_operations_from_witness_array(tx, tx_hash)
            if atomicals_operations_found_at_inputs:
                # Log information to help troubleshoot
                size_payload = sys.getsizeof(atomicals_operations_found_at_inputs['payload_bytes'])
                operation_found = atomicals_operations_found_at_inputs['op']
                operation_input_index = atomicals_operations_found_at_inputs['input_index']
                commit_txid = atomicals_operations_found_at_inputs['commit_txid']
                commit_index = atomicals_operations_found_at_inputs['commit_index']
                first_location_txid = atomicals_operations_found_at_inputs['first_location_txid']
                first_location_index = atomicals_operations_found_at_inputs['first_location_index']
                self.logger.info(f'atomicals_operations_found_at_inputs - operation_found={operation_found}, operation_input_index={operation_input_index}, size_payload={size_payload}, tx_hash={hash_to_hex_str(tx_hash)}, commit_txid={hash_to_hex_str(commit_txid)}, commit_index={commit_index}, first_location_txid={hash_to_hex_str(first_location_txid)}, first_location_index={first_location_index}')

            # Add the new UTXOs
            for idx, txout in enumerate(tx.outputs):
                # Ignore unspendable outputs
                if is_unspendable(txout.pk_script):
                    continue
                # Get the hashX
                hashX = self.coin.hashX_from_script(txout.pk_script)
                append_hashX(hashX)
                put_utxo(tx_hash + to_le_uint32(idx), hashX + tx_numb + to_le_uint64(txout.value))
            
            # Create NFT/FT atomicals if it is defined in the tx
            created_atomical_id = self.create_atomical(atomicals_operations_found_at_inputs, atomicals_spent_at_inputs, header, height, tx_num, atomical_num, tx, tx_hash)
            if created_atomical_id:
                atomical_num += 1
                # Double hash the created_atomical_id to add it to the history to leverage the existing history db for all operations involving the atomical
                append_hashX(double_sha256(created_atomical_id))
                self.logger.info(f'create_atomical:created_atomical_id - atomical_id={created_atomical_id.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')

            # Color the outputs of any transferred NFT/FT atomicals according to the rules
            atomical_ids_transferred = self.color_atomicals_outputs(atomicals_operations_found_at_inputs, atomicals_spent_at_inputs, tx, tx_hash, tx_numb, height)
            for atomical_id in atomical_ids_transferred:
                self.logger.info(f'color_atomicals_outputs:atomical_ids_transferred - atomical_id={atomical_id.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')
                # Double hash the atomical_id to add it to the history to leverage the existing history db for all operations involving the atomical
                append_hashX(double_sha256(atomical_id))
            
            # Check if there were any payments for subrealms in thtx
            self.create_or_delete_subrealm_payment_output_if_valid(tx, height, atomicals_spent_at_inputs)

            # Distributed FT mints can be created as long as it is a valid $ticker and the $max_mints has not been reached
            # Check to create a distributed mint output from a valid tx
            atomical_id_of_distmint = self.create_distmint_output(atomicals_operations_found_at_inputs, tx_hash, tx, height)
            if atomical_id_of_distmint:
                # Double hash the atomical_id_of_distmint to add it to the history to leverage the existing history db for all operations involving the atomical
                append_hashX(double_sha256(atomical_id_of_distmint))
                self.logger.info(f'create_distmint_output:atomical_id_of_distmint - atomical_id={atomical_id_of_distmint.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')
          
            append_hashXs(hashXs)
            update_touched(hashXs)

            tx_num += 1

        self.db.history.add_unflushed(hashXs_by_tx, self.tx_count)

        self.tx_count = tx_num
        self.db.tx_counts.append(tx_num)
        self.atomical_count = atomical_num
        self.db.atomical_counts.append(atomical_num)
        
        return undo_info, atomicals_undo_info

    # Check for for output markers for a payment for a subrealm
    # Same function is used for creating and rollback. Set Delete=True for rollback operation
    def create_or_delete_subrealm_payment_output_if_valid(self, tx, height, atomicals_spent_at_inputs, Delete=False):
        # Add the new UTXOs
        found_atomical_id = None
        for idx, txout in enumerate(tx.outputs):
            found_atomical_id = is_unspendable_payment_marker_atomical_id(txout.pk_script)
            if found_atomical_id:
                break
        # Payment atomical id marker was found
        if found_atomical_id:
            # Get the details such as expected payment amount/output and which parent realm it belongs to
            matched_price_point, parent_realm_id, request_subrealm_name = self.get_expected_subrealm_payment_info(found_atomical_id, height)
            # An expected payment amount might not be set if there is no valid subrealm minting rules, or something invalid was found
            if not matched_price_point:
                return
            
            expected_payment_output = matched_price_point['output']
            expected_payment_amount = matched_price_point['value']
            expected_payment_regex = matched_price_point['regex']

            # Sanity check that request_subrealm_name matches the regex
            # intentionally assume regex pattern is valid and name matches because the logic path should have already been checked
            # Any exception here indicates a developer error and the service will intentionally crash
            # Compile the regular expression
            valid_pattern = re.compile(rf"{expected_payment_regex}")
            if not valid_pattern.match(request_subrealm_name):
                raise IndexError(f'valid pattern failed to match request_subrealm_name. DeveloperError {request_subrealm_name} {expected_payment_regex}')
           
            # Valid subrealm minting rules were found, scan all the outputs to check for a match
            for idx, txout in enumerate(tx.outputs):
                # Found the required payment amount and script
                if txout.pk_script == expected_payment_output and txout.value >= expected_payment_amount:
                    # Delete or create he record based on whether we are reorg rollback or creating new
                    # todo and ensure a payment cannot overwrite another payment and cannot overwrite the parent realm issuance record
                    if Delete:
                        self.put_subrealm_payment(parent_realm_id, found_atomical_id, request_subrealm_name, tx.hash + pack_le_uint32(idx))
                    else: 
                        self.delete_subrealm_payment(parent_realm_id, found_atomical_id, request_subrealm_name, tx.hash + pack_le_uint32(idx))
    
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
            self.db.atomical_counts.pop()
        self.logger.info(f'backed up to height {self.height:,d}')

    # Rollback the spending of an atomical
    def rollback_spend_atomicals(self, tx_hash, tx, idx, tx_numb, height, operations_found_at_inputs):
        output_index_packed = pack_le_uint32(idx)
        current_location = tx_hash + output_index_packed
        # Spend the atomicals if there were any
        spent_atomicals = self.spend_atomicals_utxo(tx_hash, tx, idx, tx_numb)
        if len(spent_atomicals) > 0:
            # Remove the stored output
            self.db_deletes.append(b'po' + current_location)
        hashXs = []
        for spent_atomical in spent_atomicals:
            atomical_id = spent_atomical['atomical_id']
            location_id = spent_atomical['location_id']
            self.logger.info(f'rollback_spend_atomicals, atomical_id={atomical_id.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')
            #  hashX + scripthash + value_sats + is_sealed
            hashX = spent_atomical['data'][:HASHX_LEN]
            hashXs.append(hashX)
            # Any mod operations are deleted
            if operations_found_at_inputs and operations_found_at_inputs.get('op') == 'mod' and operations_found_at_inputs.get('input_index') == 0:
                self.db_deletes.append(b'mod' + atomical_id + tx_numb + output_index_packed)
                # If the mod(ify) operation path was set to '/subrealm-mint' with prices field
                # Then validate they are in the correct format
                mod_path = operations_found_at_inputs['payload'].get('path')
                if mod_path and len(mod_path.encode()) <= 64:
                    mod_path_padded = pad_bytes64(mod_path.encode())
                    height_packed = pack_le_uint32(height)
                    self.db_deletes.append(b'modpath' + atomical_id + mod_path_padded + tx_numb + output_index_packed + height_packed)
            # Any evt operations are deleted
            elif operations_found_at_inputs and operations_found_at_inputs.get('op') == 'evt' and operations_found_at_inputs.get('input_index') == 0:
                self.db_deletes.append(b'evt' + atomical_id + tx_numb + output_index_packed)
        return hashXs, spent_atomicals

    # Rollback any distributed mints
    def rollback_distmint_data(self, tx_hash, operations_found_at_inputs):
        if not operations_found_at_inputs:
            return
        dmt_valid, dmt_return_struct = is_valid_dmt_op_format(tx_hash, operations_found_at_inputs)
        if dmt_valid: 
            ticker = dmt_return_struct['$mint_ticker']
            self.logger.info(f'rollback_distmint_data: dmt found in tx, tx_hash={hash_to_hex_str(tx_hash)}, ticker={ticker}')
            # get the potential dmt (distributed mint) atomical_id from the ticker given
            potential_dmt_atomical_id = self.get_effective_ticker(dmt_return_struct['$mint_ticker'])
            if potential_dmt_atomical_id:
                self.logger.info(f'rollback_distmint_data: potential_dmt_atomical_id is True, tx_hash={hash_to_hex_str(tx_hash)}, ticker={ticker}')
                output_index_packed = pack_le_uint32(idx)
                location = tx_hash + output_index_packed
                self.delete_distmint_data(potential_dmt_atomical_id, location)
                # remove the b'a' atomicals entry at the mint location
                self.db_deletes.append(b'a' + potential_dmt_atomical_id + location)
                # remove the b'i' atomicals entry at the mint location
                self.db_deletes.append(b'i' + location + potential_dmt_atomical_id)
    
    # Rollback atomical mint data
    def delete_atomical_mint_data_info(self, atomical_id, location, atomical_num):
        self.logger.info(f'delete_atomical_mint_data_info: atomical_id={atomical_id.hex()}, atomical_num={atomical_num}')
        self.db_deletes.append(b'md' + atomical_id)
        self.db_deletes.append(b'mi' + atomical_id)
        # Make sure to remove the atomical number
        atomical_numb = pack_be_uint64(atomical_num) 
        self.db_deletes.append(b'n' + atomical_numb)
        # remove the b'a' atomicals entry at the mint location
        self.db_deletes.append(b'a' + atomical_id + atomical_id)
        # remove the b'i' atomicals entry at the mint location
        self.db_deletes.append(b'i' + atomical_id + atomical_id)
        # remove the script output for the commit and reveal mint
        self.db_deletes.append(b'po' + atomical_id)
        self.db_deletes.append(b'po' + location)

    # Delete atomical mint data and any associated realms, subrealms, containers and tickers
    def delete_atomical_mint(self, tx_hash, tx, atomical_num, atomicals_spent_at_inputs, operations_found_at_inputs):
        was_mint_found = False
        # All mint types always look at only input 0 to determine if the operation was found
        # This is done to preclude complex scenarios of valid/invalid different mint types across inputs 
        valid_create_op_type, mint_info = get_mint_info_op_factory(self.coin.hashX_from_script, tx, tx_hash, operations_found_at_inputs)
        if not valid_create_op_type:
            self.logger.info(f'delete_atomical_mint not valid_create_op_type')
            return False
        if not valid_create_op_type == 'NFT' or not valid_create_op_type == 'FT':
            raise IndexError(f'Could not delete ticker symbol, should never happen. Developer Error, IndexError {tx_hash}')
        
        atomical_id = mint_info['id']
        
        # If it was an NFT
        if valid_create_op_type == 'NFT':
            self.logger.info(f'delete_atomical_mint - NFT: atomical_id={atomical_id.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')
            was_mint_found = True
            if self.is_within_acceptable_blocks_for_name_reveal(mint_info):
                self.delete_realm_entry_if_requested(mint_info)
                self.delete_subrealm_entry_if_requested(mint_info, atomicals_spent_at_inputs)
                self.delete_container_entry_if_requested(mint_info)
        # If it was an FT
        elif valid_create_op_type == 'FT': 
            self.logger.info(f'delete_atomical_mint FT: atomical_id={atomical_id.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')
            if self.is_within_acceptable_blocks_for_name_reveal(mint_info):
                self.delete_ticker_entry_if_requested(mint_info)
        else: 
            assert('Invalid mint developer error fatal')
        
        if was_mint_found:
            self.delete_atomical_mint_data_info(atomical_id, mint_info['first_location'], atomical_num)

        self.logger.info(f'delete_atomical_mint return values atomical_id={atomical_id.hex()}, atomical_num={atomical_num}, was_mint_found={was_mint_found}')
        return was_mint_found  

    # Get the price regex list for a subrealm atomical
    # Returns the most recent value sorted by height descending
    def get_subrealm_regex_price_list_from_height(self, atomical_id, height, subrealm_mint_modpath_history):
        # The modpath history is when the value was set at height for the given path
        # The convention used for subrealm minting that the data in b'modpath' only becomes valid exactly MINT_SUBREALM_RULES_EFFECTIVE_BLOCKS blocks after the height
        # The reason for this is that a price list cannot be changed with active transactions.
        # This prevents the owner of the atomical from rapidly changing prices and defrauding users 
        # For example, if the owner of a realm saw someone paid the fee for an atomical, they could front run the block
        # And update their price list before the block is mined, and then cheat out the person from getting their subrealm
        # This is sufficient notice (about 1 hour) for apps to notice that the price list changed, and act accordingly.
        for modpath_item in subrealm_mint_modpath_history:
            valid_from_height = modpath_item['height'] + MINT_SUBREALM_RULES_EFFECTIVE_BLOCKS
            if height < valid_from_height:
                continue
            # Found a valid subrealm_mint_path entry
            # Get the subrealm_mint_path for the array of the form: Array<{r: regex, p: satoshis, o: output script}>
            if not modpath_item['payload'] or not isinstance(modpath_item['payload'], dict):
                self.logger.info(f'get_subrealm_regex_price_list_from_height payload is not valid atomical_id={atomical_id.hex()}')
                continue

            mod_path = modpath_item['payload'].get('path')
            if mod_path != subrealm_mint_path:
                self.logger.info(f'get_subrealm_regex_price_list_from_height subrealm-mint path not found atomical_id={atomical_id.hex()}')
                continue

            # It is at least a dictionary
            regexes = modpath_item['payload'].get('prices', None)
            if not regexes:
                self.logger.info(f'get_subrealm_regex_price_list_from_height prices value not found atomical_id={atomical_id.hex()}')
                continue
            # There is a path realm/r that exists
            if not isinstance(regexes, list):
                self.logger.info(f'get_subrealm_regex_price_list_from_height prices value not a list atomical_id={atomical_id.hex()}')
                continue

            # The subrealms field is an array/list type
            # Now populate the regex price list
            regex_price_list = []
            for regex_price in regexes:
                # regex is the first pattern that will be checked to match for minting a subrealm
                regex_pattern = regex_price.get('r', None)
                # satoshi value is the price that must be paid to mint a subrealm
                satoshis = regex_price.get('v', None)
                # Output is the output script that must be paid to mint the subrealm
                output = regex_price.get('o', None)
                # If all three are set, th
                if regex_pattern != None and satoshis != None and output != None:
                    # Check that value is greater than 0
                    if satoshis <= 0:
                        self.logger.info(f'get_subrealm_regex_price_list_from_height invalid satoshis atomical_id={atomical_id.hex()}')
                        continue
                    # Check that regex is a valid regex pattern
                    try:
                        valid_pattern = re.compile(rf"{regex_pattern}")
                        if not isinstance(output, (bytes, bytearray)):
                            self.logger.info(f'get_subrealm_regex_price_list_from_height output o value is not a byte sequence atomical_id={atomical_id.hex()}')
                            continue
                        # After all we have finally validated this is a valid price point for minting subrealm...
                        price_point = {
                            'regex': regex_pattern,
                            'value': satoshis,
                            'output': output
                        }
                        regex_price_list.append(price_point)
                    except Exception as e: 
                        self.logger.info(f'Regex or byte output error for {atomical_id}')
                        self.logger.info(f'Exception {e}')
                        continue
                else: 
                    self.logger.info(f'get_subrealm_regex_price_list_from_height realm.r list element does not contain r, v, or o fields atomical_id={atomical_id.hex()}')
            self.logger.info(f'get_subrealm_regex_price_list_from_height found valid entry for regex price list atomical_id={atomical_id.hex()}')
            return regex_price_list
        return []

    # Get a matched price point (if any) for a subrealm name for the parent atomical taking into account the height
    # Recall that the 'modpath' (contract) values will take effect only after 6 blocks have passed after the height in
    # which the update 'modpath' operation was mined.
    def get_matched_price_point_for_subrealm_name_by_height(self, parent_atomical_id, proposed_subrealm_name, height):
        subrealm_mint_path = '/subrealm-mint'
        subrealm_mint_modpath_history = self.db.get_modpath_history(parent_atomical_id, subrealm_mint_path)
        regex_price_point_list = self.get_subrealm_regex_price_list_from_height(parent_atomical_id, height, subrealm_mint_modpath_history)
        # Ensure there is a list of regex price list that is available for the atomical
        if not regex_price_point_list or len(regex_price_point_list) <= 0:
            self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
            return None 
        for regex_price_point in regex_price_point_list:
            self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height regex_price_point={regex_price_point} parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
            # Perform some sanity checks just in case
            regex_pattern = regex_price_point.get('regex', None)
            # satoshi value is the price that must be paid to mint a subrealm
            satoshis = regex_price_point.get('value', None)
            # Output is the output script that must be paid to mint the subrealm
            output = regex_price_point.get('output', None)
            # Make sure the variables are defined
            if not regex_pattern:
                self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height null regex_pattern parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                continue 
            if not satoshis:
                self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height invalid satoshis parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                continue 
            if not output or not isinstance(output, (bytes, bytearray)):
                self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height invalid output parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                continue 
            if not isinstance(satoshis, int) or satoshis <= 0:
                self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height invalid satoshis parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                continue 
            try:
                # Compile the regular expression
                valid_pattern = re.compile(rf"{regex_pattern}")
                # Match the pattern to the proposed subrealm_name, the payment of the expect value and the output script
                if not valid_pattern.match(proposed_subrealm_name):
                    self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height not match pattern valid_pattern={valid_pattern} parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                    continue
                # if txout.value < satoshis:
                #     self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height txout.value < satoshis txout.value={txout.value}, satoshis={satoshis} parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                #     continue
                # if txout.pk_script != output: 
                #     self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height txout.pk_script != output txout.pk_script={txout.pk_script}, txout.output={txout.output}, satoshis={satoshis} parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                #     continue

                self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height successfully matched regex price point, parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                return regex_price_point
            except Exception as e: 
                # If it failed, then try the next matches if any
                pass
        return None

    def backup_txs(
            self,
            txs: Sequence[Tuple[Tx, bytes]],
            is_unspendable: Callable[[bytes], bool],
    ):
        # Clear the cache just in case there are old values cached for a mint that are stale
        # In particular for $realm and $ticker values if something changed on reorg
        self.atomicals_id_cache.cache_clear()
        # Prevout values, in order down the block (coinbase first if present)
        # undo_info is in reverse block order
        undo_info = self.db.read_undo_info(self.height)
        if undo_info is None:
            raise ChainError(f'no undo information found for height '
                             f'{self.height:,d}')
        n = len(undo_info)

        ############################################
        #
        # Begin Atomicals Undo Procedure Setup
        #
        ############################################
        atomicals_undo_info = self.db.read_atomicals_undo_info(self.height)
        if atomicals_undo_info is None:
            raise ChainError(f'no atomicals undo information found for height '
                             f'{self.height:,d}')
        m = len(atomicals_undo_info)
        atomicals_undo_entry_len = ATOMICAL_ID_LEN + ATOMICAL_ID_LEN + HASHX_LEN + SCRIPTHASH_LEN + 8 + 1 # final byte is the is_sealed flag
        atomicals_count = m / atomicals_undo_entry_len
        has_undo_info_for_atomicals = False
        if m > 0:
            has_undo_info_for_atomicals = True
        c = m
        atomicals_undo_info_map = {} # Build a map of atomicals location to atomicals located there
        counted_atomicals_count = 0
        while c > 0:
            c -= atomicals_undo_entry_len
            assert(c >= 0)
            atomicals_undo_item = atomicals_undo_info[c : c + atomicals_undo_entry_len]
            atomicals_location = atomicals_undo_item[: ATOMICAL_ID_LEN]
            atomicals_atomical_id = atomicals_undo_item[ ATOMICAL_ID_LEN : ATOMICAL_ID_LEN + ATOMICAL_ID_LEN]
            atomicals_value = atomicals_undo_item[ATOMICAL_ID_LEN + ATOMICAL_ID_LEN :]
            # There can be many atomicals at the same location
            # Group them by the location
            if atomicals_undo_info_map.get(atomicals_location, None) == None:
                atomicals_undo_info_map[atomicals_location] = []
            atomicals_undo_info_map[atomicals_location].append({ 
                'location_id': atomicals_location,
                'atomical_id': atomicals_atomical_id,
                'data': atomicals_value
            })
            counted_atomicals_count += 1
        assert(counted_atomicals_count == atomicals_count)
        ############################################
        #
        # Finished Atomicals Undo Procedure Setup
        #
        # The atomicals_undo_info_map contains the mapping of the atomicals at each location and their value
        # It is a way to get the total input value grouped by atomical id
        #
        ############################################

        # Use local vars for speed in the loops
        put_utxo = self.utxo_cache.__setitem__
        spend_utxo = self.spend_utxo
        
        put_general_data = self.general_data_cache.__setitem__
  
        touched = self.touched
        undo_entry_len = HASHX_LEN + TXNUM_LEN + 8

        tx_num = self.tx_count
        tx_numb = to_le_uint64(tx_num)[:TXNUM_LEN]
        atomical_num = self.atomical_count
        atomicals_minted = 0
        # Track the atomicals being rolled back to be used primarily for determining subrealm rollback validity
        atomicals_spent_at_inputs = {}
        for tx, tx_hash in reversed(txs):
            for idx, txout in enumerate(tx.outputs):
                # Spend the TX outputs.  Be careful with unspendable
                # outputs - we didn't save those in the first place.
                if is_unspendable(txout.pk_script):
                    continue
                # Get the hashX
                cache_value = spend_utxo(tx_hash, idx)
                hashX = cache_value[:HASHX_LEN]
                txout_value = cache_value[:-8] 
                touched.add(hashX)
                # Rollback the atomicals that were created at the output
                hashXs_spent, spent_atomicals = self.rollback_spend_atomicals(tx_hash, idx, tx, tx_numb)
                for hashX_spent in hashXs_spent:
                    touched.add(hashX_spent)
                # The idx is not where it was spent, because this is the rollback operation
                # Nonetheless we use the output idx as the "spent at" just to keep a consistent format when 
                # the variable atomicals_spent_at_inputs is used in other places. There is no usage for the index, but informational purpose only
                atomicals_spent_at_inputs[idx] = spent_atomicals

            # Delete the tx hash number
            self.db_deletes.append(b'tx' + tx_hash)

            # Backup any Atomicals NFT, FT, or DFT mints
            operations_found_at_inputs = parse_protocols_operations_from_witness_array(tx, tx_hash)
            was_mint_found = self.delete_atomical_mint(tx_hash, tx, atomical_num, atomicals_spent_at_inputs, operations_found_at_inputs)
            if was_mint_found:
                atomical_num -= 1
                atomicals_minted += 1
            
            # Rollback any subrealm payments
            self.create_or_delete_subrealm_payment_output_if_valid(tx, height, atomicals_spent_at_inputs, True)

            # If there were any distributed mint creation, then delete
            self.rollback_distmint_data(tx_hash, operations_found_at_inputs)

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
                potential_atomicals_list_to_restore = atomicals_undo_info_map.get(txin.prev_hash + pack_le_uint32(txin.prev_idx))
                if potential_atomicals_list_to_restore != None:
                    for atomical_to_restore in potential_atomicals_list_to_restore:
                        self.put_atomicals_utxo(atomical_to_restore['location_id'], atomical_to_restore['atomical_id'], atomical_to_restore['data'])
                        touched.add(double_sha256(atomical_to_restore['atomical_id']))
            
            tx_num -= 1

        assert n == 0
        assert m == 0

        self.tx_count -= len(txs)
        self.atomical_count -= atomicals_minted
        # Sanity checks...
        assert(atomical_num == atomical_count)

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
