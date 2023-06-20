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
    chunks, class_logger, pack_le_uint32, pack_le_uint64, unpack_le_uint64, pack_be_uint64, unpack_be_uint64, OldTaskGroup, pack_byte
)
from electrumx.lib.tx import Tx
from electrumx.server.db import FlushData, COMP_TXID_LEN, DB
from electrumx.server.history import TXNUM_LEN
from electrumx.lib.util_atomicals import is_valid_dmt_op_format, is_compact_atomical_id, is_atomical_id_long_form_string, unpack_mint_info, parse_protocols_operations_from_witness_array, get_expected_output_index_of_atomical_nft, get_expected_output_indexes_of_atomical_ft, location_id_bytes_to_compact, is_valid_subrealm_string_name, is_valid_realm_string_name, is_valid_ticker_string, get_mint_info_op_factory

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
        self.ticker_count = 0       # Number of tickers (for Atomical FTs) created. Effectively this counts the total unique FTs
        self.realm_count = 0        # Number of realms (Atomical NFTs) created. Also called Top-Level-Realms (TLR)
        self.subrealm_count = 0     # Number of subrealms (Atomical NFTs) created. Associated with a TLR or another sub-realm as the parent
        self.container_count = 0    # Number of containers (Atomical NFTs) created. Can be used as a collection-type data structure.
        self.distmint_count = 0     # Number of distributed mints (Atomical FTs) created. Counts the individual mints against the distributed FT (DFT) deploy.
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
                         self.atomical_count, self.ticker_count, self.realm_count, self.subrealm_count, self.container_count, self.distmint_count,
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
    def get_atomicals_id_mint_info_basic_struct_for_evt(self, atomical_id):
        result = None
        self.logger.info('atomical_id')
        self.logger.info(atomical_id)
        try:
            result = self.atomicals_id_cache[atomical_id]
        except KeyError:
            # Now check the mint cache (before flush to db)
            try:
                result = unpack_mint_info(self.general_data_cache[b'mi' + atomical_id])
            except KeyError:
                result = unpack_mint_info(self.db.get_atomical_mint_info_dump(atomical_id))
            
            self.atomicals_id_cache[atomical_id] = result
        
        if result: 
            basic_struct = {
                'atomical_id': location_id_bytes_to_compact(result['id']),
                'atomical_number': result['number'],
                'type': result['type'],
                'subtype': result['subtype']
            }
            ticker = result.get('$ticker', None)
            realm = result.get('$realm', None)
            subrealm = result.get('$subrealm', None)
            container = result.get('$container', None)
            parent_realm_id_compact = result.get('$parent_realm_id_compact', None)
            if ticker: 
                basic_struct['tickers'] = ticker
            if realm: 
                basic_struct['realm'] = realm
            if subrealm: 
                basic_struct['subrealm'] = subrealm
            if subrealm: 
                basic_struct['parent_realm_id'] = parent_realm_id_compact
            if container: 
                basic_struct['container'] = container
            return basic_struct
        return result 

    # Save a ticker symbol to the cache that will be flushed to db
    def put_ticker_data(self, atomical_id, ticker): 
        self.logger.info(f'put_ticker_data: atomical_id={atomical_id.hex()}, ticker={ticker}')
        if not is_valid_ticker_string(ticker):
            raise IndexError(f'Ticker is_valid_ticker_string invalid {ticker}')
        ticker_enc = ticker.encode()
        if self.ticker_data_cache.get(ticker_enc, None) == None: 
            self.ticker_data_cache[ticker_enc] = atomical_id
        
    # Save realm name to the cache that will be flushed to the db 
    def put_realm_data(self, atomical_id, realm): 
        self.logger.info(f'put_realm_data: atomical_id={atomical_id.hex()}, realm={realm}')
        if not is_valid_realm_string_name(realm):
            raise IndexError(f'Realm is_valid_realm_string_name invalid {realm}')
        realm_enc = realm.encode()
        if self.realm_data_cache.get(realm_enc, None) == None: 
            self.realm_data_cache[realm_enc] = atomical_id
    
    # Save subrealm name to the cache that will be flushed to the db 
    def put_subrealm_data(self, atomical_id, subrealm, parent_atomical_id): 
        self.logger.info(f'put_subrealm_data: atomical_id={atomical_id.hex()}, subrealm={subrealm}, parent_atomical_id={parent_atomical_id.hex()}')
        if not is_valid_subrealm_string_name(subrealm):
            raise IndexError(f'Subrealm is_valid_subrealm_string_name invalid {subrealm}')
        subrealm_enc = subrealm.encode()
        if self.subrealm_data_cache.get(parent_atomical_id + subrealm_enc, None) == None: 
            self.subrealm_data_cache[parent_atomical_id + subrealm_enc] = atomical_id

    # Save container name to cache that will be flushed to db
    def put_container_data(self, atomical_id, container): 
        self.logger.info(f'put_container_data: atomical_id={atomical_id.hex()}, container={container}')
        if not is_valid_container_string_name(container):
            raise IndexError(f'Container is_valid_container_string_name invalid {container}')
        container_enc = container.encode()
        if self.container_data_cache.get(container_enc, None) == None: 
            self.container_data_cache[container_enc] = atomical_id

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
        # Use a tombstone to mark deleted because even if it's removd we must
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
        location_map_for_atomical = self.atomicals_utxo_cache.get(atomical_id, None)
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
            spent_cache_values = []
            for key in cache_map.keys(): 
                value_with_tombstone = cache_map[key]
                value = value_with_tombstone['value']
                is_sealed = value[HASHX_LEN + SCRIPTHASH_LEN + 8:]
                if is_sealed == b'00':
                    # Only allow it to be spent if not sealed  
                    spent_cache_values.append(location_id + key + value)
                    value_with_tombstone['deleted'] = True  # Flag it as deleted so the b'a' active location will not be written on flushed
                self.logger.info(f'spend_atomicals_utxo.cache_map: location_id={location_id.hex()} atomical_id={key.hex()}, is_sealed={is_sealed}, value={value}')
            if len(spent_cache_values) > 0:
                return spent_cache_values
        # Search the locations of existing atomicals
        # Key:  b'i' + tx_hash + txout_idx + mint_tx_hash + mint_txout_idx
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
                atomicals_data_list.append(atomical_i_db_key[1:] + atomical_i_db_value)
            self.logger.info(f'spend_atomicals_utxo.utxo_db: location_id={location_id.hex()} atomical_id={atomical_id.hex()}, is_sealed={is_sealed}, value={atomical_i_db_value}')
            # Return all of the atomicals spent at the address
        return atomicals_data_list

    # Remove the ticker only if it exists and if it was associated with atomical_id
    # This is done because another malicious user could mint an invalid mint, and then on rollback result
    # In deleting the legit ticker that was minted before. 
    def delete_ticker_data(self, expected_atomical_id, ticker): 
        ticker_enc = ticker.encode()
        # Check if it's located in the cache first
        ticker_data = self.ticker_data_cache.get(ticker_enc, None)
        if ticker_data and ticker_data == expected_atomical_id:
            # remove from the cache
            self.ticker_data_cache.pop(ticker_enc)
            self.logger.info(f'delete_ticker_data.ticker_data_cache: ticker={ticker}, expected_atomical_id={expected_atomical_id.hex()}')
            # Intentionally fall through to catch it in the db as well just in case

        # Check the db whether or not it was in the cache as a safety measure (todo: Can be removed later as codebase proves robust)
        ticker_data_from_db = self.db.get_ticker(ticker_enc, None)
        if ticker_data_from_db != None and ticker_data_from_db == expected_atomical_id: 
            self.db_deletes.append(b'tick' + ticker_enc)
            self.logger.info(f'delete_ticker_data.db_deletes: ticker={ticker}, expected_atomical_id={expected_atomical_id.hex()}')
        if ticker_data or ticker_data_from_db:
            self.logger.info(f'delete_ticker_data return True: ticker={ticker}, expected_atomical_id={expected_atomical_id.hex()}')
            return True

    # Remove the realm only if it exists and if it was associated with atomical_id
    # This is done because another malicious user could mint an invalid mint, and then on rollback result
    # In deleting the legit ticker that was minted before.
    def delete_realm_data(self, expected_atomical_id, realm): 
        realm_enc = realm.encode()
        # Check if it's located in the cache first
        realm_data = self.realm_data_cache.get(realm_enc, None)
        if realm_data and realm_data == expected_atomical_id:
            # remove from the cache
            self.realm_data_cache.pop(realm_enc)
            self.logger.info(f'delete_realm_data.realm_data_cache: {realm}, expected_atomical_id={expected_atomical_id.hex()}')
            # Intentionally fall through to catch it in the db as well just in case
        
        # Check the db whether or not it was in the cache as a safety measure (todo: Can be removed later as codebase proves robust)
        realm_data_from_db = self.db.get_realm(realm_enc, None)
        if realm_data_from_db != None and realm_data_from_db == expected_atomical_id: 
            self.db_deletes.append(b'rlm' + realm_enc)
            self.logger.info(f'delete_realm_data.db_deletes: realm={realm}, expected_atomical_id={expected_atomical_id.hex()}')
        if realm_data or realm_data_from_db:
            self.logger.info(f'delete_realm_data return True: realm={realm}, expected_atomical_id={expected_atomical_id.hex()}')
            return True

    # Remove the subrealm only if it exists and if it was associated with atomical_id for the parent_atomical_id (subrealm/TLR)
    # This is done because another malicious user could mint an invalid mint, and then on rollback result
    # In deleting the legit ticker that was minted before.
    def delete_subrealm_data(self, parent_atomical_id, expected_atomical_id, subrealm): 
        subrealm_enc = subrealm.encode()
        # Check if it's located in the cache first
        subrealm_data = self.subrealm_data_cache.get(parent_atomical_id + subrealm_enc, None)
        if subrealm_data and subrealm_data == expected_atomical_id:
            # remove from the cache
            self.subrealm_data_cache.pop(parent_atomical_id + subrealm_enc)
            self.logger.info(f'delete_subrealm_data.subrealm_data_cache: subrealm={subrealm}, parent_atomical_id={parent_atomical_id.hex()} atomical_id={atomical_id.hex()}')
            # Intentionally fall through to catch it in the db as well just in case
        
        # Check the db whether or not it was in the cache as a safety measure (todo: Can be removed later as codebase proves robust)
        subrealm_data_from_db = self.db.get_subrealm(parent_atomical_id + subrealm_enc, None)
        if subrealm_data_from_db != None and subrealm_data_from_db == expected_atomical_id: 
            self.db_deletes.append(b'srlm' + parent_atomical_id + subrealm_enc)
            self.logger.info(f'delete_subrealm_data.db_deletes: subrealm={subrealm}, parent_atomical_id={parent_atomical_id.hex()} atomical_id={atomical_id.hex()}')
        if subrealm_data or subrealm_data_from_db:
            self.logger.info(f'delete_subrealm_data return True: subrealm={subrealm}, parent_atomical_id={parent_atomical_id.hex()} atomical_id={atomical_id.hex()}')
            return True

    # Remove the container only if it exists and if it was associated with atomical_id
    # This is done because another malicious user could mint an invalid mint, and then on rollback result
    # In deleting the legit ticker that was minted before.
    def delete_container_data(self, atomical_id, container): 
        container_enc = container.encode()
        # Check if it's located in the cache first
        container_data = self.container_data_cache.get(container_enc, None)
        if container_data and container_data == atomical_id:
            # remove from the cache
            self.container_data_cache.pop(container_enc)
            self.logger.info(f'delete_container_data.container_data_cache: {container}, atomical_id={atomical_id.hex()}')
            # Intentionally fall through to catch it in the db as well just in case
        # Check the db whether or not it was in the cache as a safety measure (todo: Can be removed later as codebase proves robust)
        container_data_from_db = self.db.get_container(container_enc, None)
        if container_data_from_db != None and container_data_from_db == atomical_id: 
            self.db_deletes.append(b'co' + container_enc)
            self.logger.info(f'delete_container_data.db_deletes: container={container}, atomical_id={atomical_id.hex()}')
        if container_data or container_data_from_db:
            self.logger.info(f'delete_container_data return True: container={container}, atomical_id={atomical_id.hex()}')
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
    
    # Whether a subrealm by a given name is a valid subrealm string and if it's not already claimed
    def is_subrealm_acceptable_to_be_created(self, operations_found_at_inputs, atomicals_spent_at_inputs, txout, parent_realm_atomical_id, subrealm, height):
        method = 'is_subrealm_acceptable_to_be_created'
        if not subrealm:
            self.log_subrealm_request(method, 'empty', False, subrealm, parent_realm_atomical_id, height)
            return None 
        if is_valid_subrealm_string_name(subrealm):
            # first step is to validate that the subrealm name meets the requirements of the owner of the parent
            # as far as minting rules go. Recall that a subrealm can be minted by the parent manually OR
            # The parent can set the `realm/reg` key using the `crt` operation to specific what regex/price points
            # are available for the minting of subrealms
            # First we check if it was minted manually by the parent realm and if that fails, then check if there are regex/price points
            # that could be satisfied.
            #
            # After the subrealm minting rules are satisfied the final step is to check that the subrealm is not already taken
            initiated_by_parent = False
            if operations_found_at_inputs['input_index'] == 0 and operations_found_at_inputs['op'] == 'sub':
                atomical_list = atomicals_spent_at_inputs.get(0)
                for atomical_id in atomical_list:
                    # Only mint under the atomical if there is exactly one atomical that requested it
                    if atomical_id == parent_realm_atomical_id:
                        initiated_by_parent = True
                        break
            # If it was initiated by the parent, then we can skip trying to match it against a crt price point
            if not initiated_by_parent:
                price_point = self.get_matched_price_point_for_subrealm_name_by_height(parent_realm_atomical_id, subrealm, txout, height)
                if not price_point:
                    self.log_subrealm_request(method, 'no_price_point', False, subrealm, parent_realm_atomical_id, height)
                    return False 
            # If we got this far it was either initiated by the parent or it matched one of the regex price points
            # All that is left to do is ensure that the name is not taken already
            subrealm_enc = subrealm.encode()
            # It already exists in cache, therefore cannot assign
            if self.subrealm_data_cache.get(parent_realm_atomical_id + subrealm_enc, None) != None: 
                self.log_subrealm_request(method, 'cache_exists', False, subrealm, parent_realm_atomical_id, height)
                return None 
            # Or it already exists in db and also cannot use
            if self.db.get_subrealm(parent_realm_atomical_id, subrealm_enc) != None:
                self.log_subrealm_request(method, 'db_exists', False, subrealm, parent_realm_atomical_id, height)
                return None
            # Congratulations, we can create a subrealm now for the parent realm
            self.log_subrealm_request(method, 'success', True, subrealm, parent_realm_atomical_id, height)
            return subrealm
        self.log_subrealm_request(method, 'is_valid_realm_string_name', False, subrealm, parent_realm_atomical_id, height)
        return None
    
    def log_can_be_created(self, method, msg, subject, validity, val):
        self.logger.info(f'{method} - {msg}: {subject} value {val} is acceptable to be created: {validity}')

    # Whether a realm by a given name is a valid realm string and if it's not already claimed
    def is_realm_acceptable_to_be_created(self, value):
        method = 'is_realm_acceptable_to_be_created'
        if not value:
            self.log_can_be_created(method, 'empty', 'realm', False, value)
            return None 
        if is_valid_realm_string_name(value):
            value_enc = value.encode()
            # It already exists in cache, therefore cannot assign
            if self.realm_data_cache.get(value_enc, None) != None: 
                self.log_can_be_created(method, 'realm_taken_found_in_cache', 'realm', False, value)
                return None 
            if self.db.get_realm(value_enc) != None:
                self.log_can_be_created(method, 'realm_taken_found_in_db', 'realm', False, value)
                return None
            # Return the utf8 because it will be verified and converted elsewhere
            self.log_can_be_created(method, 'success', 'realm', True, value)
            return value
        
        self.log_can_be_created(method, 'is_valid_realm_string_name', 'realm', False, value)
        return None
 
    # Whether a container by a given name is a valid container string and if it's not already claimed
    def is_container_acceptable_to_be_created(self, value):
        method = 'is_container_acceptable_to_be_created'
        if not value:
            self.log_can_be_created(method, 'empty', 'container', False, value)
            return None 
        if is_valid_container_string_name(value):
            value_enc = value.encode()
            # It already exists in cache, therefore cannot assign
            if self.container_data_cache.get(value_enc, None) != None: 
                self.log_can_be_created(method, 'cached', 'container', False, value)
                return None 
            if self.db.get_container(value_enc, None) != None:
                self.log_can_be_created(method, 'db', 'container', False, value)
                return None
            # Return the utf8 because it will be verified and converted elsewhere
            self.log_can_be_created(method, 'success', 'container', True, value)
            return value
        self.log_can_be_created(method, 'is_valid_container_string_name', 'container', False, value)
        return None

    # Whether a ticker by a given name is a valid ticker string and if it's not already claimed
    def is_ticker_acceptable_to_be_created(self, value):
        method = 'is_ticker_acceptable_to_be_created'
        if not value:
            self.log_can_be_created(method, 'empty', 'ticker', False, value)
            return None 
        if is_valid_ticker_string(value):
            value_enc = value.encode()
            # It already exists in cache, therefore cannot assign
            if self.ticker_data_cache.get(value_enc, None) != None: 
                self.log_can_be_created(method, 'cached', 'ticker', False, value)
                return None 
            if self.db.get_ticker(value_enc, None) != None:
                self.log_can_be_created(method, 'db', 'ticker', False, value)
                return None
            # Return the utf8 because it will be verified and converted elsewhere
            self.log_can_be_created(method, 'success', 'ticker', True, value)
            return value
        self.log_can_be_created(method, 'is_valid_ticker_string', 'ticker', False, value)
        return None
 
    # Validate the parameters for an NFT and validate realm/subrealm/container data
    def validate_and_create_nft(self, mint_info, operations_found_at_inputs, atomicals_spent_at_inputs, txout, height, tx_hash):
        if not mint_info or not isinstance(mint_info, dict):
            return False, None, None, None

        realm = mint_info.get('$realm', None)
        subrealm = mint_info.get('$subrealm', None)
        container = mint_info.get('$container', None)

        # Request includes realm to be minted to have successful create
        if realm:
            self.logger.info(f'validate_and_create_nft: realm={realm}, tx_hash={hash_to_hex_str(tx_hash)}')
            # Is the realm taken or available?
            can_assign_realm = self.is_realm_acceptable_to_be_created(realm)
            self.logger.info(f'can_assign_realm {can_assign_realm}')
            # The realm can be assigned and therefore the NFT create can succeed
            if can_assign_realm:
                assert(can_assign_realm == realm)
                self.put_realm_data(mint_info['id'], can_assign_realm)
            else: 
                # Fail the attempt to create NFT because the realm cannot be assigned when it was requested
                return False, None, None, None
        elif subrealm:
            realm_parent_atomical_id_compact = mint_info.get('$parent_realm_id_compact')
            self.logger.info(f'validate_and_create_nft: subrealm={subrealm}, realm_parent_atomical_id_compact={realm_parent_atomical_id_compact}, tx_hash={hash_to_hex_str(tx_hash)}')
            if not realm_parent_atomical_id_compact or not is_compact_atomical_id(realm_parent_atomical_id):
                return False, None, None, None

            realm_parent_atomical_id_long = mint_info.get('$parent_realm_id')
            if not realm_parent_atomical_id_long:
                return False, None, None, None
            
            if compact_to_location_id_bytes(realm_parent_atomical_id_compact) != realm_parent_atomical_id_long:
                raise IndexError(f'validate_and_create_nft realm_parent_atomical_id_compact and realm_parent_atomical_id_long not equal. Index or Developer Error')
             
            # Convert to the 36 byte normalized form of tx_hash + index
            realm_parent_atomical_id_long = compact_to_location_id_bytes(realm_parent_atomical_id_compact)

            # Is the subrealm taken or available for the parent atomical
            can_assign_subrealm = self.is_subrealm_acceptable_to_be_created(operations_found_at_inputs, atomicals_spent_at_inputs, txout, realm_parent_atomical_id_long, subrealm, height)
            
            # The realm can be assigned and therefore the NFT create can succeed
            if can_assign_subrealm:
                assert(can_assign_subrealm == subrealm)
                self.put_subrealm_data(mint_info['id'], can_assign_subrealm, realm_parent_atomical_id_long)
            else: 
                # Fail the attempt to create NFT because the subrealm cannot be assigned when it was requested
                return False, None, None, None
        # Request includes container to be minted to have successful create
        elif container:
            self.logger.info(f'validate_and_create_nft: container={container}, tx_hash={hash_to_hex_str(tx_hash)}')
            # Is the container taken or available?
            can_assign_container = self.is_container_acceptable_to_be_created(container)
            # The container can be assigned and therefore the NFT create can succeed
            if can_assign_container:
                assert(can_assign_container == container)
                self.put_container_data(mint_info['id'], can_assign_container)
            else: 
                # Fail the attempt to create NFT because the realm cannot be assigned when it was requested
                return False, None, None, None

        tx_numb = pack_le_uint64(mint_info['tx_num'])[:TXNUM_LEN]
        value_sats = pack_le_uint64(mint_info['value'])
        # Save the initial location to have the atomical located there
        is_sealed = b'00'
        self.logger.info(f'put_atomicals_utxo: mint_info={mint_info}')
        self.put_atomicals_utxo(mint_info['id'], mint_info['id'], mint_info['hashX'] + mint_info['scripthash'] + value_sats + is_sealed)
        subtype = mint_info['subtype']
        atomical_id = mint_info['id']
        self.logger.info(f'Atomicals Create NFT in Transaction {hash_to_hex_str(tx_hash)}, atomical_id={location_id_bytes_to_compact(atomical_id)}, subtype={subtype}, realm={realm}, subrealm={subrealm}, container={container}, tx_hash={hash_to_hex_str(tx_hash)}')
        return True, realm, subrealm, container
    
    # Validate the parameters for a FT and ticker
    def validate_and_create_ft(self, mint_info, tx_hash):
        ticker = mint_info['$ticker']
        # Request includes realm to be minted to have successful create
        if not ticker:
            raise IndexError(f'Fatal error ticker not set when it shoudl have been. DeveloperError')
        # Is the ticker taken or available?
        can_assign_ticker = self.is_ticker_acceptable_to_be_created(ticker)
        # The ticker can be assigned and therefore the NFT create can succeed
        if can_assign_ticker:
            assert(can_assign_ticker == ticker)
            self.put_ticker_data(mint_info['id'], can_assign_ticker)
        else: 
            # Fail the attempt to create NFT because the ticker cannot be assigned when it was requested
            return False, None
        
        self.logger.info(f'validate_and_create_ft: ticker={ticker}, tx_hash={hash_to_hex_str(tx_hash)}')

        tx_numb = pack_le_uint64(mint_info['tx_num'])[:TXNUM_LEN]
        value_sats = pack_le_uint64(mint_info['value'])
        # Save the initial location to have the atomical located there
        if mint_info['subtype'] != 'distributed':
            is_sealed = b'00'
            self.put_atomicals_utxo(mint_info['id'], mint_info['id'], mint_info['hashX'] + mint_info['scripthash'] + value_sats + is_sealed)
        
        subtype = mint_info['subtype']
        self.logger.info(f'Atomicals Create FT in Transaction {hash_to_hex_str(tx_hash)}, subtype={subtype}, atomical_id={location_id_bytes_to_compact(atomical_id)}, ticker={ticker}, tx_hash={hash_to_hex_str(tx_hash)}')
        return True, mint_info['$ticker']

    # Check whether to create an atomical NFT/FT 
    # Validates the format of the detected input operation and then checks the correct extra data is valid
    # such as realm, container, ticker, etc. Only succeeds if the appropriate names can be assigned
    def create_atomical(self, operations_found_at_inputs, atomicals_spent_at_inputs, header, height, tx_num, atomical_num, tx, tx_hash):
        if not operations_found_at_inputs:
            return None, None, None, None, None
        # Catch the strange case where there are no outputs
        if len(tx.outputs) == 0:
            return None, None, None, None, None

        # All mint types always look at only input 0 to determine if the operation was found
        # This is done to preclude complex scenarios of valid/invalid different mint types across inputs 
        valid_create_op_type, mint_info = get_mint_info_op_factory(self.coin.hashX_from_script, tx_hash, tx, operations_found_at_inputs)
        if not valid_create_op_type or (valid_create_op_type != 'NFT' and valid_create_op_type != 'FT'):
            return None, None, None, None, None

        # The atomical would always be created at the first output
        txout = tx.outputs[0]
        # Since the mint is potentially valid, attach the block specific data that would exist
        mint_info['number'] = atomical_num 
        mint_info['header'] = header 
        mint_info['height'] = height 
        mint_info['tx_num'] = tx_num 

        # Establish the Atomical id from the tx hash and the 0'th output index (since Atomicals can only be minted at the 0'th output)
        atomical_id = tx_hash + pack_le_uint32(0)
        realm_created = None
        subrealm_created = None
        container_created = None 
        ticker_created = None
        if valid_create_op_type == 'NFT':
            is_created, realm, subrealm, container = self.validate_and_create_nft(mint_info, operations_found_at_inputs, atomicals_spent_at_inputs, txout, height, tx_hash)
            if not is_created:
                self.logger.info(f'Atomicals Create NFT validate_and_create_nft returned FALSE in Transaction {hash_to_hex_str(tx_hash)}') 
                return None, None, None, None, None
            if realm and mint_info['$realm'] != realm:
                raise IndexError(f'Fatal logic error found invalid different $realm in mint')
            if subrealm and mint_info['$subrealm'] != subrealm:
                raise IndexError(f'Fatal logic error found invalid different $subrealm in mint')
            if container and mint_info['$container'] != realm:
                raise IndexError(f'Fatal logic error found invalid different $container in mint')
            realm_created = realm
            subrealm_created = subrealm
            container_created = container
        elif valid_create_op_type == 'FT':
            # Validate and create the FT, and also adding the $ticker property if it was requested in the params
            # Adds $ticker symbol to mint_info always or fails
            is_created, ticker = self.validate_and_create_ft(mint_info, tx_hash)
            if not is_created:
                self.logger.info(f'Atomicals Create FT validate_and_create_ft returned FALSE in Transaction {hash_to_hex_str(tx_hash)}') 
                return None, None, None, None, None
            if not(ticker) or mint_info['$ticker'] != ticker:
                raise IndexError(f'Fatal logic error found invalid $ticker in mint ft')
            
            # Add $max_supply informative property
            if mint_info['subtype'] == 'distributed':
                mint_info['$max_supply'] = mint_info['$mint_amount'] * mint_info['$max_mints'] 
            else: 
                mint_info['$max_supply'] = txout.value
            ticker_created = ticker
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
        # Save the output script of the atomical to lookup at a future point
        put_general_data(b'po' + atomical_id, txout.pk_script)
        return atomical_id, realm_created, subrealm_created, ticker_created, container_created

    # Build a map of atomical id to the type, value, and input indexes
    # This information is used below to assess which inputs are of which type and therefore which outputs to color
    def build_atomical_id_info_map(self, map_atomical_ids_to_info, atomicals_list, txin_index):
        for atomicals_entry in atomicals_list:
            atomical_id = atomicals_entry[ ATOMICAL_ID_LEN : ATOMICAL_ID_LEN + ATOMICAL_ID_LEN]
            value, = unpack_le_uint64(atomicals_entry[-8:])
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
    def get_expected_output_indexes_to_color(operations_found_at_inputs, mint_info, tx, atomical_id):
        if mint_info['type'] == 'NFT':
            assert(len(mint_info['input_indexes']) == 1)
            expected_output_indexes = [get_expected_output_index_of_atomical_nft(mint_info, tx, atomical_id, operations_found_at_inputs)]
        elif mint_info['type'] == 'FT':
            assert(len(mint_info['input_indexes']) >= 1)
            expected_output_indexes = get_expected_output_indexes_of_atomical_ft(mint_info, tx, atomical_id, operations_found_at_inputs) 
        else:
            raise IndexError('color_atomicals_outputs: Unknown type. Index Error.')

    # Detect and apply updates-related like operations for an atomical such as mod/evt/crt/sl
    def apply_state_like_updates(operations_found_at_inputs, mint_info, atomical_id, tx_numb, output_idx_le, height):
        put_general_data = self.general_data_cache.__setitem__
        if operations_found_at_inputs and operations_found_at_inputs.get('op', None) == 'mod' and operations_found_at_inputs.get('input_index', None) == 0:
            self.logger.info(f'apply_state_like_updates op=mod, height={height}, atomical_id={atomical_id.hex()}, tx_numb={tx_numb}')
            put_general_data(b'st' + atomical_id + tx_numb + output_idx_le, operations_found_at_inputs['payload_bytes'])
        elif operations_found_at_inputs and operations_found_at_inputs.get('op', None) == 'evt' and operations_found_at_inputs.get('input_index', None) == 0:
            self.logger.info(f'apply_state_like_updates op=moevtd, height={height}, atomical_id={atomical_id.hex()}, tx_numb={tx_numb}')
            put_general_data(b'evt' + atomical_id + tx_numb + output_idx_le, operations_found_at_inputs['payload_bytes'])
        elif operations_found_at_inputs and operations_found_at_inputs.get('op', None) == 'crt' and operations_found_at_inputs.get('input_index', None) == 0:
            self.logger.info(f'apply_state_like_updates op=crt, height={height}, atomical_id={atomical_id.hex()}, tx_numb={tx_numb}')
            height_packed = pack_le_uint32(height)
            put_general_data(b'crt' + atomical_id + tx_numb + output_idx_le + height_packed, pickle.dumps(operations_found_at_inputs['payload']))

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
        for txin_index, atomicals_list in atomicals_spent_at_inputs.items():
            # Accumulate the total input value by atomical_id
            # The value will be used below to determine the amount of input we can allocate for FT's
            self.build_atomical_id_info_map(map_atomical_ids_to_info, atomicals_list, txin_index)
        
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
                if operations_found_at_inputs.get('op', None) == 'sl' and mint_info['type'] == 'NFT' and mint_info.get('subtype', None) == 'container' and operations_found_at_inputs.get('input_index', None) == 0:
                    is_sealed = b'01'
                self.put_atomicals_utxo(location, atomical_id, hashX + scripthash + value_sats + is_sealed)
            
            atomical_ids_touched.append(atomical_id)
        return atomical_ids_touched

    # Create a distributed mint output as long as the rules are satisfied
    def create_distmint_output(self, atomicals_operations_found_at_inputs, tx_hash, tx, height):
        dmt_valid, dmt_return_struct = is_valid_dmt_op_format(tx_hash, atomicals_operations_found_at_inputs)
        if not dmt_valid:
            return None
        
        # get the potential dmt (distributed mint) atomical_id from the ticker given
        potential_dmt_atomical_id = self.db.get_atomical_id_by_ticker(dmt_return_struct['$ticker'])
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
        mint_ticker = mint_info_for_ticker['$ticker']
        mint_height = mint_info_for_ticker['$mint_height']
        
        if mint_ticker != dmt_return_struct['$ticker']:
            dmt_ticker = dmt_return_struct['$ticker']
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
        ticker_num = self.ticker_count
        realm_num = self.realm_count
        subrealm_num = self.subrealm_count
        container_num = self.container_count
        distmint_num = self.distmint_count
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
                    for atomical_id_spent in atomicals_transferred_list:
                        self.logger.info(f'atomicals_transferred_list - tx_hash={hash_to_hex_str(tx_hash)}, txin_index={txin_index}, txin_hash={hash_to_hex_str(txin.prev_hash)}, txin_previdx={txin.prev_idx}, atomical_id_spent={atomical_id_spent.hex()}')

                atomicals_undo_info_extend(atomicals_transferred_list)
                txin_index = txin_index + 1
            
            # Detect all protocol operations in the transaction witness inputs
            atomicals_operations_found_at_inputs = parse_protocols_operations_from_witness_array(tx)
            if atomicals_operations_found_at_inputs:
                size_payload = sys.getsizeof(atomicals_operations_found_at_inputs['payload_bytes'])
                operation_found = atomicals_operations_found_at_inputs['op']
                operation_input_index = atomicals_operations_found_at_inputs['input_index']
                self.logger.info(f'atomicals_operations_found_at_inputs - operation_found={operation_found}, operation_input_index={operation_input_index}, size_payload={size_payload}, tx_hash={hash_to_hex_str(tx_hash)}')

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
            created_atomical_id, created_realm, created_subrealm, created_ticker, created_container = self.create_atomical(atomicals_operations_found_at_inputs, atomicals_spent_at_inputs, header, height, tx_num, atomical_num, tx, tx_hash)
            if created_atomical_id:
                atomical_num += 1
                if created_ticker:
                    ticker_num += 1
                elif created_realm:
                    realm_num += 1
                elif created_subrealm:
                    subrealm_num += 1
                elif created_container:
                    container_num += 1
                # Double hash the created_atomical_id to add it to the history to leverage the existing history db for all operations involving the atomical
                append_hashX(double_sha256(created_atomical_id))
                self.logger.info(f'create_atomical:created_atomical_id - atomical_id={created_atomical_id.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')

            # Color the outputs of any transferred NFT/FT atomicals according to the rules
            atomical_ids_transferred = self.color_atomicals_outputs(atomicals_operations_found_at_inputs, atomicals_spent_at_inputs, tx, tx_hash, tx_numb, height)
            for atomical_id in atomical_ids_transferred:
                self.logger.info(f'color_atomicals_outputs:atomical_ids_transferred - atomical_id={atomical_id.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')
                # Double hash the atomical_id to add it to the history to leverage the existing history db for all operations involving the atomical
                append_hashX(double_sha256(atomical_id))
            
            # Distributed FT mints can be created as long as it is a valid $ticker and the $max_mints has not been reached
            # Check to create a distributed mint output from a valid tx
            atomical_id_of_distmint = self.create_distmint_output(atomicals_operations_found_at_inputs, tx_hash, tx, height)
            if atomical_id_of_distmint:
                distmint_num += 1
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
        
        self.ticker_count = ticker_num
        self.db.ticker_counts.append(ticker_num)
       
        self.realm_count = realm_num
        self.db.realm_counts.append(realm_num)
        
        self.subrealm_count = subrealm_num
        self.db.subrealm_counts.append(subrealm_num)
        
        self.container_count = container_num
        self.db.container_counts.append(container_num)

        self.distmint_count = distmint_num
        self.db.distmint_counts.append(distmint_num)
        
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
            self.db.atomical_counts.pop()
        self.logger.info(f'backed up to height {self.height:,d}')

    # Rollback the spending of an atomical
    def rollback_spend_atomicals(self, tx_hash, tx, idx, tx_numb, height):
        output_index_packed = pack_le_uint32(idx)
        current_location = tx_hash + output_index_packed
        # Spend the atomicals if there were any
        spent_atomicals = self.spend_atomicals_utxo(tx_hash, tx, idx, tx_numb)
        if len(spent_atomicals) > 0:
            # Remove the stored output
            self.db_deletes.append(b'po' + current_location)
        hashXs = []
        for spent_atomical in spent_atomicals:
            self.logger.info(f'rollback_spend_atomicals, atomical_id={spent_atomical.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')
            hashX = spent_atomical[ ATOMICAL_ID_LEN + ATOMICAL_ID_LEN : ATOMICAL_ID_LEN + ATOMICAL_ID_LEN + HASHX_LEN]
            hashXs.append(hashX)
            atomical_id = spent_atomical[ ATOMICAL_ID_LEN : ATOMICAL_ID_LEN + ATOMICAL_ID_LEN]
            location = spent_atomical[ : ATOMICAL_ID_LEN]
            # Just delete any potential state updates indiscriminately
            self.db_deletes.append(b'st' + atomical_id + tx_numb + output_index_packed)
            # Just delete any potential message event messages indiscriminately
            self.db_deletes.append(b'evt' + atomical_id + tx_numb + output_index_packed)
            # Just delete any potential contract crt updates indiscriminately
            height_packed = pack_le_uint32(height)
            self.db_deletes.append(b'crt' + atomical_id + tx_numb + output_index_packed + height_packed)
        return hashXs 

    # Rollback any distributed mints
    def rollback_distmint_data(self, tx_hash, operations_found_at_inputs):
        if not operations_found_at_inputs:
            return
        dmt_valid, dmt_return_struct = is_valid_dmt_op_format(tx_hash, operations_found_at_inputs)
        if dmt_valid: 
            ticker = dmt_return_struct['$ticker']
            self.logger.info(f'rollback_distmint_data: dmt found in tx, tx_hash={hash_to_hex_str(tx_hash)}, ticker={ticker}')
            # get the potential dmt (distributed mint) atomical_id from the ticker given
            potential_dmt_atomical_id = self.db.get_atomical_id_by_ticker(dmt_return_struct['$ticker'])
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
    def delete_atomical_mint_data(self, atomical_id, atomical_num):
        self.logger.info(f'delete_atomical_mint_data: atomical_id={atomical_id.hex()}, atomical_num={atomical_num}')
        self.db_deletes.append(b'md' + atomical_id)
        self.db_deletes.append(b'mi' + atomical_id)
        # Make sure to remove the atomical number
        atomical_numb = pack_be_uint64(atomical_num) 
        self.db_deletes.append(b'n' + atomical_numb)
        # remove the b'a' atomicals entry at the mint location
        self.db_deletes.append(b'a' + atomical_id + atomical_id)
        # remove the b'i' atomicals entry at the mint location
        self.db_deletes.append(b'i' + atomical_id + atomical_id)

    # Delete atomical mint data and any associated realms, subrealms, containers and tickers
    def delete_atomical_mint_data_with_realms_container(self, tx_hash, tx, atomical_num, operations_found_at_inputs):
        was_mint_found = False
        was_realm_type = False
        was_subrealm_type = False
        was_container_type = False 
        was_fungible_type = False
        # All mint types always look at only input 0 to determine if the operation was found
        # This is done to preclude complex scenarios of valid/invalid different mint types across inputs 
        valid_create_op_type, mint_info = get_mint_info_op_factory(self.coin.hashX_from_script, tx_hash, tx, operations_found_at_inputs)
        if not valid_create_op_type:
            self.logger.info(f'delete_atomical_mint_data_with_realms_container not valid_create_op_type')
            return False, False, False, False 
        if not valid_create_op_type == 'NFT' or not valid_create_op_type == 'FT':
            raise IndexError(f'Could not delete ticker symbol, should never happen. Developer Error, IndexError {tx_hash}')
        atomical_id = tx_hash + pack_le_uint32(0)
        # If it was an NFT
        if valid_create_op_type == 'NFT':
            self.logger.info(f'delete_atomical_mint_data_with_realms_container - NFT: atomical_id={atomical_id.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')
            # If realm or container was specificed then it was only minted if the realm/container was created for the atomical
            realm = mint_info.get('$realm', None)
            subrealm = mint_info.get('$subrealm', None)
            container = mint_info.get('$container', None)
            parent_realm_id = mint_info.get('$parent_realm_id', None)
            # Only consider it a mint if the realm was successfully deleted for the CURRENT atomical_id
            if realm and self.delete_realm_data(atomical_id, realm):
                was_realm_type = True
                was_mint_found = True
            elif subrealm and parent_realm_id and self.delete_subrealm_data(parent_realm_id, atomical_id, subrealm):
                was_subrealm_type = True
                was_mint_found = True
            elif container and self.delete_container_data(atomical_id, container):
                was_container_type = True
                was_mint_found = True
        # If it was an FT
        elif valid_create_op_type == 'FT': 
            self.logger.info(f'delete_atomical_mint_data_with_realms_container FT: atomical_id={atomical_id.hex()}, tx_hash={hash_to_hex_str(tx_hash)}')
            # Only consider it a mint if the ticker was successfully deleted for the CURRENT atomical_id
            if self.delete_ticker_data(atomical_id, mint_info['$ticker']):
                was_fungible_type = True
                was_mint_found = True
            else: 
                raise IndexError(f'Could not delete ticker symbol, should never happen. Developer Error, IndexError {tx_hash}')
        else: 
            assert('Invalid mint developer error fatal')
        
        if was_mint_found:
            self.delete_atomical_mint_data(atomical_id, atomical_num)

        self.logger.info(f'delete_atomical_mint_data_with_realms_container return values atomical_id={atomical_id.hex()}, atomical_num={atomical_num}, was_mint_found={was_mint_found}, was_realm_type={was_realm_type}, was_subrealm_type={was_subrealm_type}, was_container_type={was_container_type}, was_fungible_type={was_fungible_type}')
        return was_mint_found, was_realm_type, was_subrealm_type, was_container_type, was_fungible_type       

    # Get the price regex list for a subrealm atomical
    # Returns the most recent value sorted by height descending
    def get_subrealm_regex_price_list_from_height(self, atomical_id, height):
        crt_history = self.db.get_contract_interface_history(atomical_id)
        # The crt history is when the value was set at height
        # The conention is that the data in b'crt' only becomes valid exactly 6 blocks after the height
        # The reason for this is that a price list cannot be changed with active transactions.
        # This prevents the owner of the atomical from rapidly changing prices and defrauding users 
        # For example, if the owner of a realm saw someone paid the fee for an atomical, they could front run the block
        # And update their price list before the block is mined, and then cheat out the person from getting their subrealm
        # This is sufficient notice (about 1 hour) for apps to notice that the price list changed, and act accordingly.
        MIN_BLOCKS_AFTER = 6 # Magic number that requires a grace period of 6 blocks 
        for crt_item in crt_history:
            crt_valid_from = crt_item['history'] + MIN_BLOCKS_AFTER
            if height < crt_valid_from:
                continue
            # Found a valid crt entry
            # Get the `realm/reg` path for the array of the form: Array<{r: regex, p: satoshis, o: output script}>
            if not crt_item['payload'] or not isinstance(crt_item['payload'], dict):
                self.logger.info(f'get_subrealm_regex_price_list_from_height payload is not valid atomical_id={atomical_id.hex()}')
                continue
            # It is at least a dictionary
            realm_namespace = crt_item['payload'].get('realm', None)
            if not realm_namespace or not realm_namespace.get('r', None):
                self.logger.info(f'get_subrealm_regex_price_list_from_height realm.r value not found atomical_id={atomical_id.hex()}')
                continue
            # There is a path realm/r that exists
            regexes = realm_namespace['r']
            if not isinstance(regexes, list):
                self.logger.info(f'get_subrealm_regex_price_list_from_height realm.r value not a list atomical_id={atomical_id.hex()}')
                continue
            # The realm/r is an array/list type
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
    # Recall that the 'crt' (contract) values will take effect only after 6 blocks have passed after the height in
    # which the update 'crt' operation was mined.
    def get_matched_price_point_for_subrealm_name_by_height(self, parent_atomical_id, proposed_subrealm_name, txout, height):
        regex_price_point_list = self.get_subrealm_regex_price_list_from_height(parent_atomical_id, height)
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
                if txout.value < satoshis:
                    self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height txout.value < satoshis txout.value={txout.value}, satoshis={satoshis} parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                    continue
                if txout.pk_script != output: 
                    self.logger.info(f'get_matched_price_point_for_subrealm_name_by_height txout.pk_script != output txout.pk_script={txout.pk_script}, txout.output={txout.output}, satoshis={satoshis} parent_atomical_id={parent_atomical_id.hex()}, proposed_subrealm_name={proposed_subrealm_name}, height={height}')
                    continue

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
                location: atomicals_location,
                atomical_id: atomicals_atomical_id,
                value: atomicals_value
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
        ticker_num = self.ticker_count
        realm_num = self.realm_count
        subrealm_num = self.subrealm_count
        container_num = self.container_count
        distmint_num = self.distmint_count
        atomicals_minted = 0
        tickers_minted = 0
        realms_minted = 0
        subrealms_minted = 0
        containers_minted = 0
        distmints_minted = 0

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
                hashXs_spent = self.rollback_spend_atomicals(tx_hash, idx, tx, tx_numb)
                for hashX_spent in hashXs_spent:
                    touched.add(hashX_spent)

            # Backup any Atomicals NFT, FT, or FTD mints
            operations_found_at_inputs = parse_protocols_operations_from_witness_array(tx)
            was_mint_found, was_realm_type, was_subrealm_type, was_container_type, was_fungible_type = self.delete_atomical_mint_data_with_realms_container(tx_hash, tx, atomical_num, operations_found_at_inputs)
            if was_mint_found:
                atomical_num -= 1
                atomicals_minted += 1
                if was_realm_type:
                    realms_minted += 1
                    realm_num -= 1
                elif was_subrealm_type:
                    subrealms_minted += 1
                    subrealm_num -= 1
                elif was_container_type:
                    containers_minted += 1
                    container_num -= 1
                elif was_fungible_type:
                    tickers_minted += 1
                    ticker_num -= 1
    
            # If there were any distributed mint creation, then delete
            dmint_create_found = self.rollback_distmint_data(tx_hash, operations_found_at_inputs)
            if dmint_create_found:
                dmints_minted += 1
                dmint_num -= 1

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
                potential_atomicals_list_to_restore = atomicals_undo_info_map.get(txin.prev_hash + pack_le_uint32(txin.prev_idx), None)
                if potential_atomicals_list_to_restore != None:
                    for atomical_to_restore in potential_atomicals_list_to_restore:
                        self.put_atomicals_utxo(atomical_to_restore['location'], atomical_to_restore['atomical_id'], atomical_to_restore['value'])
                        touched.add(double_sha256(atomical_to_restore['atomical_id']))
            
            tx_num -= 1

        assert n == 0
        assert m == 0

        self.tx_count -= len(txs)
        self.atomical_count -= atomicals_minted
        self.ticker_count -= tickers_minted
        self.realm_count -= realms_minted
        self.subrealm_count -= subrealms_minted
        self.container_count -= containers_minted
        self.distmint_count -= distmints_minted
        # Sanity checks...
        assert(atomical_num == atomical_count)
        assert(ticker_num == ticker_count)
        assert(realm_num == realm_count)
        assert(subrealm_num == subrealm_count)
        assert(container_num == container_count)
        assert(distmint_num == distmint_count)

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
        self.ticker_count = self.db.db_ticker_count
        self.realm_count = self.db.db_realm_count
        self.subrealm_count = self.db.db_subrealm_count
        self.container_count = self.db.db_container_count
        self.distmint_count = self.db.db_distmint_count

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
