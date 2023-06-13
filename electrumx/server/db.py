# Copyright (c) 2016-2020, Neil Booth
# Copyright (c) 2017, the ElectrumX authors
#
# All rights reserved.
#
# See the file "LICENCE" for information about the copyright
# and warranty status of this software.

'''Interface to the blockchain database.'''

from array import array
import ast
import os
import time
from bisect import bisect_right
from dataclasses import dataclass
from glob import glob
from typing import Dict, List, Sequence, Tuple, Optional, TYPE_CHECKING

import attr
from aiorpcx import run_in_thread, sleep

import electrumx.lib.util as util
from electrumx.lib.hash import hash_to_hex_str, HASHX_LEN, double_sha256
from electrumx.lib.merkle import Merkle, MerkleCache
from electrumx.lib.util import (
    formatted_time, pack_be_uint16, pack_be_uint32, pack_le_uint64, pack_be_uint64, pack_le_uint32,
    unpack_le_uint32, unpack_be_uint32, unpack_le_uint64, unpack_be_uint64
)
from electrumx.lib.util_atomicals import get_tx_hash_index_from_location_id, location_id_bytes_to_compact, check_unpack_field_data
from electrumx.server.storage import db_class, Storage
from electrumx.server.history import History, TXNUM_LEN
from electrumx.lib.script import SCRIPTHASH_LEN
from cbor2 import dumps, loads, CBORDecodeError

import pickle

if TYPE_CHECKING:
    from electrumx.server.env import Env

ATOMICAL_ID_LEN = 36

@dataclass(order=True)
class UTXO:
    __slots__ = 'tx_num', 'tx_pos', 'tx_hash', 'height', 'value'
    tx_num: int      # index of tx in chain order
    tx_pos: int      # tx output idx
    tx_hash: bytes   # txid
    height: int      # block height
    value: int       # in satoshis

@attr.s(slots=True)

class FlushData:
    height = attr.ib()
    tx_count = attr.ib() 
    headers = attr.ib()
    block_tx_hashes = attr.ib()
    # The following are flushed to the UTXO DB if undo_infos is not None
    undo_infos = attr.ib()  # type: List[Tuple[Sequence[bytes], int]]
    adds = attr.ib()  # type: Dict[bytes, bytes]  # txid+out_idx -> hashX+tx_num+value_sats
    deletes = attr.ib()  # type: List[bytes]  # b'h' db keys, and b'u' db keys, and Atomicals and related keys
    tip = attr.ib()
    atomical_count = attr.ib()
    ticker_count = attr.ib() 
    realm_count = attr.ib() 
    subrealm_count = attr.ib() 
    container_count = attr.ib() 
    distmint_count = attr.ib() 
    atomicals_undo_infos = attr.ib()  # type: List[Tuple[Sequence[bytes], int]]
    atomicals_adds = attr.ib()  # type: Dict[bytes, bytes]  # b'a' + atomical_id(txid+out_idx) + location(txid+out_idx) -> hashX + scripthash + value_sats
    general_adds = attr.ib()  # type: List[Tuple[Sequence[bytes], Sequence[bytes]]]
    ticker_adds = attr.ib()  # type: List[Tuple[Sequence[bytes], Sequence[bytes]]]
    realm_adds = attr.ib()  # type: List[Tuple[Sequence[bytes], Sequence[bytes]]]
    subrealm_adds = attr.ib()  # type: List[Tuple[Sequence[bytes], Sequence[bytes]]]
    container_adds = attr.ib()  # type: List[Tuple[Sequence[bytes], Sequence[bytes]]]
    distmint_adds = attr.ib()  # type: List[Tuple[Sequence[bytes], Sequence[bytes]]]
    
COMP_TXID_LEN = 4

class DB:
    '''Simple wrapper of the backend database for querying.

    Performs no DB update, though the DB will be cleaned on opening if
    it was shutdown uncleanly.
    '''

    DB_VERSIONS = (6, 7, 8)

    utxo_db: Optional['Storage']

    class DBError(Exception):
        '''Raised on general DB errors generally indicating corruption.'''

    def __init__(self, env: 'Env'):
        self.logger = util.class_logger(__name__, self.__class__.__name__)
        self.env = env
        self.coin = env.coin

        # Setup block header size handlers
        if self.coin.STATIC_BLOCK_HEADERS:
            self.header_offset = self.coin.static_header_offset
            self.header_len = self.coin.static_header_len
        else:
            self.header_offset = self.dynamic_header_offset
            self.header_len = self.dynamic_header_len

        self.logger.info(f'switching current directory to {env.db_dir}')
        os.chdir(env.db_dir)

        self.db_class = db_class(self.env.db_engine)
        self.history = History()
        # Key: b'u' + address_hashX + txout_idx + tx_num
        # Value: the UTXO value as a 64-bit unsigned integer (in satoshis)
        # "at address, at outpoint, there is a UTXO of value v"
        # ---
        # Key: b'h' + compressed_tx_hash + txout_idx + tx_num
        # Value: hashX
        # "some outpoint created a UTXO at address"
        # ---
        # Key: b'U' + block_height
        # Value: byte-concat list of (hashX + tx_num + value_sats)
        # "undo data: list of UTXOs spent at block height"
        # ---
        # Key: b'i' + location(tx_hash + txout_idx) + atomical_id(tx_hash + txout_idx)
        # Value: hashX + scripthash + value_sats
        # "Map location to all the Atomicals located there. Permanently stored for every location even if spent."
        # ---
        # Key: b'a' + atomical_id(tx_hash + txout_idx) + location(tx_hash + txout_idx)
        # Value: hashX + scripthash + value_sats
        # "Map Atomical ID to an unspent location. Used to located the NFT/FT Atomical UTXOs."
        # ---
        # Key: b'L' + block_height
        # Value: byte-concat list of (tx_hash + txout_idx + atomical_id(mint_tx_hash + mint_txout_idx) + hashX + scripthash + value_sats)
        # "undo data: list of atomicals UTXOs spent at block height"
        # ---
        # Key: b'md' + atomical_id
        # Value: mint data serialized.
        # "maps atomical_id to mint data fields { object key-value pairs } "
        # ---
        # Key: b'mi' + atomical_id
        # Value: mint info serialized.
        # "maps atomical_id to mint information such as block info"
        # ---
        # Key: b'no' + atomical_number (8 bytes integer)
        # Value: Atomical_id
        # "maps atomical number to an atomical_id"
        # ---
        # Key: b'st' + atomical_id + tx_num + out_idx
        # Value: maps the atomical, transaction number and output that took the state
        # "maps the atomical, transaction number and output for the general state"
        # ---
        # Key: b'evt' + atomical_id + tx_num + out_idx
        # Value: maps the atomical, transaction number and output for the request/reply event operation
        # "maps the atomical, transaction number and output for the event state"
        # ---
        # Key: b'crt' + atomical_id + tx_num + out_idx + height
        # Value: maps the atomical, transaction number and output for the crt contract update operation
        # "maps the atomical, transaction number and output for the contract state"
        # ---
        # Key: b'po' + tx_hash + tx_out_idx
        # Value: pk_script output
        # "maps arbitrary location to an output script. Useful for decoding what address/scripthash as the Atomicals"
        # ---
        # Key: b'rlm' + name bytes
        # Value: atomical_id bytes
        # "maps name to atomical id (NFT)"
        # ---
        # Key: b'srlm' + parent_realm(atomical_id) + name
        # Value: atomical_id bytes
        # "maps parent realm atomical id and sub-name to the atomical_id (NFT)"
        # ---
        # Key: b'tick' + tick bytes
        # Value: atomical_id bytes
        # "maps name to atomical id (FT)"
        # ---
        # Key: b'gi' + atomical_id + location_id
        # Value: satoshis at the output 
        # "maps generated atomical mint and location to a value"
        self.utxo_db = None
        self.utxo_flush_count = 0
        self.fs_height = -1
        self.fs_tx_count = 0
        self.fs_atomical_count = 0
        self.fs_ticker_count = 0
        self.fs_realm_count = 0
        self.fs_subrealm_count = 0
        self.fs_container_count = 0
        self.fs_distmint_count = 0
        self.db_height = -1
        self.db_tx_count = 0
        self.db_atomical_count = 0
        self.db_ticker_count = 0
        self.db_realm_count = 0
        self.db_container_count = 0
        self.db_distmint_count = 0
        self.db_tip = None  # type: Optional[bytes]
        self.tx_counts = None
        self.atomical_counts = None
        self.ticker_counts = None
        self.realm_counts = None
        self.subrealm_counts = None
        self.container_counts = None
        self.distmint_counts = None
        self.last_flush = time.time()
        self.last_flush_tx_count = 0
        self.last_flush_atomical_count = 0
        self.last_flush_ticker_count = 0
        self.last_flush_realm_count = 0
        self.last_flush_subrealm_count = 0
        self.last_flush_container_count = 0
        self.last_flush_distmint_count = 0
        self.wall_time = 0
        self.first_sync = True
        self.db_version = -1
        self.logger.info(f'using {self.env.db_engine} for DB backend')

        # Header merkle cache
        self.merkle = Merkle()
        self.header_mc = MerkleCache(self.merkle, self.fs_block_hashes)

        # on-disk: raw block headers in chain order
        self.headers_file = util.LogicalFile('meta/headers', 2, 16000000)
        # on-disk: cumulative number of txs at the end of height N
        self.tx_counts_file = util.LogicalFile('meta/txcounts', 2, 2000000)
        # on-disk: cumulative number of atomicals counts at the end of height N
        self.atomical_counts_file = util.LogicalFile('meta/atomicalscounts', 2, 2000000)
        # on-disk: cumulative number of ticker counts at the end of height N
        self.ticker_counts_file = util.LogicalFile('meta/tickercounts', 2, 2000000)
        # on-disk: cumulative number of realm names counts at the end of height N
        self.realm_counts_file = util.LogicalFile('meta/realmcounts', 2, 2000000)
        # on-disk: cumulative number of sub realm names counts at the end of height N
        self.subrealm_counts_file = util.LogicalFile('meta/subrealmcounts', 2, 2000000)
        # on-disk: cumulative number of collection counts at the end of height N
        self.container_counts_file = util.LogicalFile('meta/collectioncounts', 2, 2000000)
        # on-disk: cumulative number of distmint counts at the end of height N
        self.distmint_counts_file = util.LogicalFile('meta/distmintcounts', 2, 2000000)
        # on-disk: 32 byte txids in chain order, allows (tx_num -> txid) map
        self.hashes_file = util.LogicalFile('meta/hashes', 4, 16000000)
        if not self.coin.STATIC_BLOCK_HEADERS:
            self.headers_offsets_file = util.LogicalFile(
                'meta/headers_offsets', 2, 16000000)

    async def _read_tx_counts(self):
        if self.tx_counts is not None:
            return
        # tx_counts[N] has the cumulative number of txs at the end of
        # height N.  So tx_counts[0] is 1 - the genesis coinbase
        size = (self.db_height + 1) * 8
        tx_counts = self.tx_counts_file.read(0, size)
        assert len(tx_counts) == size
        self.tx_counts = array('Q', tx_counts)
        if self.tx_counts:
            assert self.db_tx_count == self.tx_counts[-1]
        else:
            assert self.db_tx_count == 0
    
    async def _read_atomical_counts(self):
        if self.atomical_counts is not None:
            return
        # tx_counts[N] has the cumulative number of txs at the end of
        # height N.  So tx_counts[0] is 1 - the genesis coinbase
        size = (self.db_height + 1) * 8
        atomical_counts = self.atomical_counts_file.read(0, size)
        assert len(atomical_counts) == size
        self.atomical_counts = array('Q', atomical_counts)
        if self.atomical_counts:
            assert self.db_atomical_count == self.atomical_counts[-1]
        else:
            assert self.db_atomical_count == 0

    async def _read_ticker_counts(self):
        if self.ticker_counts is not None:
            return
        # tx_counts[N] has the cumulative number of txs at the end of
        # height N.  So tx_counts[0] is 1 - the genesis coinbase
        size = (self.db_height + 1) * 8
        ticker_counts = self.ticker_counts_file.read(0, size)
        assert len(ticker_counts) == size
        self.ticker_counts = array('Q', ticker_counts)
        if self.ticker_counts:
            assert self.db_ticker_count == self.ticker_counts[-1]
        else:
            assert self.db_ticker_count == 0

    async def _read_container_counts(self):
        if self.container_counts is not None:
            return
        # tx_counts[N] has the cumulative number of txs at the end of
        # height N.  So tx_counts[0] is 1 - the genesis coinbase
        size = (self.db_height + 1) * 8
        container_counts = self.container_counts_file.read(0, size)
        assert len(container_counts) == size
        self.container_counts = array('Q', container_counts)
        if self.container_counts:
            assert self.db_container_count == self.container_counts[-1]
        else:
            assert self.db_container_count == 0

    async def _read_distmint_counts(self):
        if self.distmint_counts is not None:
            return
        # tx_counts[N] has the cumulative number of txs at the end of
        # height N.  So tx_counts[0] is 1 - the genesis coinbase
        size = (self.db_height + 1) * 8
        distmint_counts = self.distmint_counts_file.read(0, size)
        assert len(distmint_counts) == size
        self.distmint_counts = array('Q', distmint_counts)
        if self.distmint_counts:
            assert self.db_distmint_count == self.distmint_counts[-1]
        else:
            assert self.db_distmint_count == 0

    async def _read_realm_counts(self):
        if self.realm_counts is not None:
            return
        # tx_counts[N] has the cumulative number of txs at the end of
        # height N.  So tx_counts[0] is 1 - the genesis coinbase
        size = (self.db_height + 1) * 8
        realm_counts = self.realm_counts_file.read(0, size)
        assert len(realm_counts) == size
        self.realm_counts = array('Q', realm_counts)
        if self.realm_counts:
            assert self.db_realm_count == self.realm_counts[-1]
        else:
            assert self.db_realm_count == 0

    async def _read_subrealm_counts(self):
        if self.subrealm_counts is not None:
            return
        # tx_counts[N] has the cumulative number of txs at the end of
        # height N.  So tx_counts[0] is 1 - the genesis coinbase
        size = (self.db_height + 1) * 8
        subrealm_counts = self.subrealm_counts_file.read(0, size)
        assert len(subrealm_counts) == size
        self.subrealm_counts = array('Q', subrealm_counts)
        if self.subrealm_counts:
            assert self.db_subrealm_count == self.subrealm_counts[-1]
        else:
            assert self.db_subrealm_count == 0

    async def _open_dbs(self, for_sync: bool, compacting: bool):
        assert self.utxo_db is None

        # First UTXO DB
        self.utxo_db = self.db_class('utxo', for_sync)
        if self.utxo_db.is_new:
            self.logger.info('created new database')
            self.logger.info('creating metadata directory')
            os.mkdir('meta')
            with util.open_file('COIN', create=True) as f:
                f.write(f'ElectrumX databases and metadata for '
                        f'{self.coin.NAME} {self.coin.NET}'.encode())
            if not self.coin.STATIC_BLOCK_HEADERS:
                self.headers_offsets_file.write(0, b'\0\0\0\0\0\0\0\0')
        else:
            self.logger.info(f'opened UTXO DB (for sync: {for_sync})')
        self.read_utxo_state()

        # Then history DB
        self.utxo_flush_count = self.history.open_db(self.db_class, for_sync,
                                                     self.utxo_flush_count,
                                                     compacting)
        self.clear_excess_undo_info()

        self.clear_excess_atomicals_undo_info()

        # Read TX counts (requires meta directory)
        await self._read_tx_counts()

        # Read Atomicals number counts (requires meta directory)
        await self._read_atomical_counts()

        # Read ticker number counts (requires meta directory)
        await self._read_ticker_counts()

        # Read realm name number counts (requires meta directory)
        await self._read_realm_counts()

        # Read sub realm name number counts (requires meta directory)
        await self._read_subrealm_counts()

        # Read collection number counts (requires meta directory)
        await self._read_container_counts()

        # Read distmint number counts (requires meta directory)
        await self._read_distmint_counts()

    async def open_for_compacting(self):
        await self._open_dbs(True, True)

    async def open_for_sync(self):
        '''Open the databases to sync to the daemon.

        When syncing we want to reserve a lot of open files for the
        synchronization.  When serving clients we want the open files for
        serving network connections.
        '''
        await self._open_dbs(True, False)

    async def open_for_serving(self):
        '''Open the databases for serving.  If they are already open they are
        closed first.
        '''
        if self.utxo_db:
            self.logger.info('closing DBs to re-open for serving')
            self.utxo_db.close()
            self.history.close_db()
            self.utxo_db = None
        await self._open_dbs(False, False)

    # Header merkle cache

    async def populate_header_merkle_cache(self):
        self.logger.info('populating header merkle cache...')
        length = max(1, self.db_height - self.env.reorg_limit)
        start = time.monotonic()
        await self.header_mc.initialize(length)
        elapsed = time.monotonic() - start
        self.logger.info(f'header merkle cache populated in {elapsed:.1f}s')

    async def header_branch_and_root(self, length, height):
        return await self.header_mc.branch_and_root(length, height)

    # Flushing
    def assert_flushed(self, flush_data):
        '''Asserts state is fully flushed.'''
        assert flush_data.tx_count == self.fs_tx_count == self.db_tx_count
        assert flush_data.atomical_count == self.fs_atomical_count == self.db_atomical_count
        assert flush_data.ticker_count == self.fs_ticker_count == self.db_ticker_count
        assert flush_data.realm_count == self.fs_realm_count == self.db_realm_count
        assert flush_data.subrealm_count == self.fs_subrealm_count == self.db_subrealm_count
        assert flush_data.container_count == self.fs_container_count == self.db_container_count
        assert flush_data.distmint_count == self.fs_distmint_count == self.db_distmint_count
        assert flush_data.height == self.fs_height == self.db_height
        assert flush_data.tip == self.db_tip
        assert not flush_data.headers
        assert not flush_data.block_tx_hashes
        assert not flush_data.adds
        assert not flush_data.atomicals_adds
        assert not flush_data.general_adds
        assert not flush_data.ticker_adds
        assert not flush_data.realm_adds
        assert not flush_data.subrealm_adds
        assert not flush_data.container_adds
        assert not flush_data.distmint_adds
        assert not flush_data.deletes
        assert not flush_data.undo_infos
        assert not flush_data.atomicals_undo_infos
        self.history.assert_flushed()

    def flush_dbs(self, flush_data, flush_utxos, estimate_txs_remaining):
        '''Flush out cached state.  History is always flushed; UTXOs are
        flushed if flush_utxos.'''
        if flush_data.height == self.db_height:
            self.assert_flushed(flush_data)
            return

        start_time = time.time()
        prior_flush = self.last_flush
        tx_delta = flush_data.tx_count - self.last_flush_tx_count
        atomical_delta = flush_data.atomical_count - self.last_flush_atomical_count
        ticker_delta = flush_data.ticker_count - self.last_flush_ticker_count
        realm_delta = flush_data.realm_count - self.last_flush_realm_count
        subrealm_delta = flush_data.subrealm_count - self.last_flush_subrealm_count
        container_delta = flush_data.container_count - self.last_flush_container_count
        distmint_delta = flush_data.distmint_count - self.last_flush_distmint_count

        # Flush to file system
        self.flush_fs(flush_data)

        # Then history
        self.flush_history()

        # Flush state last as it reads the wall time.
        with self.utxo_db.write_batch() as batch:
            if flush_utxos:
                self.flush_utxo_db(batch, flush_data)
            self.flush_state(batch)

        # Update and put the wall time again - otherwise we drop the
        # time it took to commit the batch
        self.flush_state(self.utxo_db)

        elapsed = self.last_flush - start_time
        self.logger.info(f'flush #{self.history.flush_count:,d} took '
                         f'{elapsed:.1f}s.  Height {flush_data.height:,d} '
                         f'txs: {flush_data.tx_count:,d} ({tx_delta:+,d}) '
                         f'Atomical txs: {flush_data.atomical_count:,d} ({atomical_delta:+,d}) '
                         f'Realms: {flush_data.realm_count:,d} ({realm_delta:+,d}) '
                         f'Sub-Realms: {flush_data.subrealm_count:,d} ({subrealm_delta:+,d}) '
                         f'Tickers: {flush_data.ticker_count:,d} ({ticker_delta:+,d}) '
                         f'Containers: {flush_data.container_count:,d} ({container_delta:+,d}) '
                         f'Distributed Mints: {flush_data.distmint_count:,d} ({distmint_delta:+,d})')

        # Catch-up stats
        if self.utxo_db.for_sync:
            flush_interval = self.last_flush - prior_flush
            tx_per_sec_gen = int(flush_data.tx_count / self.wall_time)
            tx_per_sec_last = 1 + int(tx_delta / flush_interval)
            eta = estimate_txs_remaining() / tx_per_sec_last
            self.logger.info(f'tx/sec since genesis: {tx_per_sec_gen:,d}, '
                             f'since last flush: {tx_per_sec_last:,d}')
            self.logger.info(f'sync time: {formatted_time(self.wall_time)}  '
                             f'ETA: {formatted_time(eta)}')

    def flush_fs(self, flush_data):
        '''Write headers, tx counts and block tx hashes to the filesystem.

        The first height to write is self.fs_height + 1.  The FS
        metadata is all append-only, so in a crash we just pick up
        again from the height stored in the DB.
        '''
        prior_tx_count = (self.tx_counts[self.fs_height]
                          if self.fs_height >= 0 else 0)
        assert len(flush_data.block_tx_hashes) == len(flush_data.headers)
        assert flush_data.height == self.fs_height + len(flush_data.headers)
        assert flush_data.tx_count == (self.tx_counts[-1] if self.tx_counts
                                       else 0)
        assert len(self.tx_counts) == flush_data.height + 1
        hashes = b''.join(flush_data.block_tx_hashes)
        flush_data.block_tx_hashes.clear()
        assert len(hashes) % 32 == 0
        assert len(hashes) // 32 == flush_data.tx_count - prior_tx_count

        # Write the headers, tx counts, and tx hashes
        start_time = time.monotonic()
        height_start = self.fs_height + 1
        offset = self.header_offset(height_start)
        self.headers_file.write(offset, b''.join(flush_data.headers))
        self.fs_update_header_offsets(offset, height_start, flush_data.headers)
        flush_data.headers.clear()

        offset = height_start * self.tx_counts.itemsize
        self.tx_counts_file.write(offset,
                                  self.tx_counts[height_start:].tobytes())

        atomical_offset = height_start * self.atomical_counts.itemsize
        self.atomical_counts_file.write(atomical_offset,
                                  self.atomical_counts[height_start:].tobytes())

        ticker_offset = height_start * self.ticker_counts.itemsize
        self.ticker_counts_file.write(ticker_offset, self.ticker_counts[height_start:].tobytes())

        realm_offset = height_start * self.realm_counts.itemsize
        self.realm_counts_file.write(realm_offset, self.realm_counts[height_start:].tobytes())

        subrealm_offset = height_start * self.subrealm_counts.itemsize
        self.subrealm_counts_file.write(subrealm_offset, self.subrealm_counts[height_start:].tobytes())

        container_offset = height_start * self.container_counts.itemsize
        self.container_counts_file.write(container_offset, self.container_counts[height_start:].tobytes())

        distmint_offset = height_start * self.distmint_counts.itemsize
        self.distmint_counts_file.write(distmint_offset, self.distmint_counts[height_start:].tobytes())

        offset = prior_tx_count * 32
        self.hashes_file.write(offset, hashes)

        self.fs_height = flush_data.height
        self.fs_tx_count = flush_data.tx_count
        self.fs_atomical_count = flush_data.atomical_count
        self.fs_ticker_count = flush_data.ticker_count
        self.fs_realm_count = flush_data.realm_count
        self.fs_subrealm_count = flush_data.subrealm_count
        self.fs_container_count = flush_data.container_count
        self.fs_distmint_count = flush_data.distmint_count

        if self.utxo_db.for_sync:
            elapsed = time.monotonic() - start_time
            self.logger.info(f'flushed filesystem data in {elapsed:.2f}s')

    def flush_history(self):
        self.history.flush()

    def flush_utxo_db(self, batch, flush_data: FlushData):
        '''Flush the cached DB writes and UTXO set to the batch.'''
        # Care is needed because the writes generated by flushing the
        # UTXO state may have keys in common with our write cache or
        # may be in the DB already.
        start_time = time.monotonic()
        add_count = len(flush_data.adds)

        atomical_add_count = 0
        for location_key, atomical_map in flush_data.atomicals_adds.items():
            for atomical_id, value_with_tombstone in atomical_map.items():
                atomical_add_count = atomical_add_count + 1

        spend_count = len(flush_data.deletes) // 2

        # Spends
        batch_delete = batch.delete

        for key in sorted(flush_data.deletes):
            batch_delete(key)

        flush_data.deletes.clear()

        # General data adds (ie: for Atomicals mints)
        batch_put = batch.put
        for key, v in flush_data.general_adds.items():
            batch_put(key, v)
        flush_data.general_adds.clear()

        # ticker data adds
        batch_put = batch.put
        for key, v in flush_data.ticker_adds.items():
            batch_put(b'tick' + key, v)
          
        flush_data.ticker_adds.clear()

        # Realm data adds
        batch_put = batch.put
        for key, v in flush_data.realm_adds.items():
            batch_put(b'rlm' + key, v)

        flush_data.realm_adds.clear()

        # Sub-Realm data adds
        # the key is the parent atomical id and the sub realm name
        batch_put = batch.put
        for key, v in flush_data.subrealm_adds.items():
            batch_put(b'srlm' + key, v)

        flush_data.subrealm_adds.clear()

        # Collection data adds
        batch_put = batch.put
        for key, v in flush_data.container_adds.items():
            batch_put(b'co' + key, v)

        flush_data.container_adds.clear()

        # New UTXOs
        batch_put = batch.put
        for key, value in flush_data.adds.items():
            # key: txid+out_idx, value: hashX+tx_num+value_sats
            hashX = value[:HASHX_LEN]
            txout_idx = key[-4:]
            tx_num = value[HASHX_LEN: HASHX_LEN+TXNUM_LEN]
            value_sats = value[-8:]
            suffix = txout_idx + tx_num
            batch_put(b'h' + key[:COMP_TXID_LEN] + suffix, hashX)
            batch_put(b'u' + hashX + suffix, value_sats)
        flush_data.adds.clear()
        
        # New atomicals UTXOs
        batch_put = batch.put
        for location_key, atomical_map in flush_data.atomicals_adds.items():
            for atomical_id, value_with_tombstone in atomical_map.items():
                value = value_with_tombstone['value']
                hashX = value[:HASHX_LEN]
                scripthash = value[HASHX_LEN : HASHX_LEN + SCRIPTHASH_LEN]
                value_sats = value[HASHX_LEN + SCRIPTHASH_LEN: HASHX_LEN + SCRIPTHASH_LEN + 8]
                is_sealed = value[HASHX_LEN + SCRIPTHASH_LEN + 8]
                batch_put(b'i' + location_key + atomical_id, hashX + scripthash + value_sats + is_sealed)
                # Add the active b'a' atomicals location if it was not deleted
                if not value_with_tombstone.get('deleted', False):
                    batch_put(b'a' + atomical_id + location_key, hashX + scripthash + value_sats)
        flush_data.atomicals_adds.clear()
 
        # Distributed mint data adds
        batch_put = batch.put
        for atomical_id_key, location_map in flush_data.distmint_adds.items():
            for location_id, value in location_map.items():
                # the value is the format of: scripthash + value_sats
                batch_put(b'gi' + atomical_id_key + location_id, value)

        flush_data.distmint_adds.clear()

        # New undo information
        self.flush_undo_infos(batch_put, flush_data.undo_infos)
        flush_data.undo_infos.clear()

        self.flush_atomicals_undo_infos(batch_put, flush_data.atomicals_undo_infos)
        flush_data.atomicals_undo_infos.clear()

        if self.utxo_db.for_sync:
            block_count = flush_data.height - self.db_height
            tx_count = flush_data.tx_count - self.db_tx_count
            atomical_count = flush_data.atomical_count - self.db_atomical_count
            ticker_count = flush_data.ticker_count - self.db_ticker_count
            realm_count = flush_data.realm_count - self.db_realm_count
            subrealm_count = flush_data.subrealm_count - self.db_subrealm_count
            container_count = flush_data.container_count - self.db_container_count
            distmint_count = flush_data.distmint_count - self.db_distmint_count
            elapsed = time.monotonic() - start_time
            self.logger.info(f'flushed {block_count:,d} blocks with '
                             f'{tx_count:,d} txs, {add_count:,d} UTXO adds, '
                             f'{atomical_count:,d} Atomical txs, {atomical_add_count:,d} Atomical UTXO adds, '
                             f'{ticker_count:,d} Tickers, '
                             f'{realm_count:,d} Realms, '
                             f'{subrealm_count:,d} Sub-Realms, '
                             f'{container_count:,d} Containers, '
                             f'{distmint_count:,d} Distributed Mints, '
                             f'{spend_count:,d} spends in '
                             f'{elapsed:.1f}s, committing...')

        self.utxo_flush_count = self.history.flush_count
        self.db_height = flush_data.height
        self.db_tx_count = flush_data.tx_count
        self.db_atomical_count = flush_data.atomical_count
        self.db_ticker_count = flush_data.ticker_count
        self.db_realm_count = flush_data.realm_count
        self.db_subrealm_count = flush_data.subrealm_count
        self.db_container_count = flush_data.container_count
        self.db_distmint_count = flush_data.distmint_count
        self.db_tip = flush_data.tip

    def flush_state(self, batch):
        '''Flush chain state to the batch.'''
        now = time.time()
        self.wall_time += now - self.last_flush
        self.last_flush = now
        self.last_flush_tx_count = self.fs_tx_count
        self.last_flush_atomical_count = self.fs_atomical_count
        self.last_flush_ticker_count = self.fs_ticker_count
        self.last_flush_realm_count = self.fs_realm_count
        self.last_flush_subrealm_count = self.fs_subrealm_count
        self.last_flush_container_count = self.fs_container_count
        self.last_flush_distmint_count = self.fs_distmint_count
        self.write_utxo_state(batch)

    def flush_backup(self, flush_data, touched):
        '''Like flush_dbs() but when backing up.  All UTXOs are flushed.'''
        assert not flush_data.headers
        assert not flush_data.block_tx_hashes
        assert flush_data.height < self.db_height
        self.history.assert_flushed()

        start_time = time.time()
        tx_delta = flush_data.tx_count - self.last_flush_tx_count
        atomical_delta = flush_data.atomical_count - self.last_flush_atomical_count
        ticker_delta = flush_data.ticker_count - self.last_flush_ticker_count
        realm_delta = flush_data.realm_count - self.last_flush_realm_count
        subrealm_delta = flush_data.subrealm_count - self.last_flush_subrealm_count
        container_delta = flush_data.container_count - self.last_flush_container_count
        distmint_delta = flush_data.distmint_count - self.last_flush_distmint_count

        self.backup_fs(flush_data.height, flush_data.tx_count, flush_data.atomical_count, flush_data.ticker_count, flush_data.realm_count, flush_data.subrealm_count, flush_data.container_count, flush_data.distmint_count)
        # Do not need to do anything with atomical_count for history.backup
        self.history.backup(touched, flush_data.tx_count)
        with self.utxo_db.write_batch() as batch:
            self.flush_utxo_db(batch, flush_data)
            # Flush state last as it reads the wall time.
            self.flush_state(batch)
            
        elapsed = self.last_flush - start_time
        self.logger.info(f'backup flush #{self.history.flush_count:,d} took '
                         f'{elapsed:.1f}s.  Height {flush_data.height:,d} '
                         f'txs: {flush_data.tx_count:,d}'  f'({tx_delta:+,d}) ' 
                         f'Atomical txs: {flush_data.atomical_count:,d} ({atomical_delta:+,d}) '
                         f'Realms: {flush_data.realm_count:,d} ({realm_delta:+,d}) '
                         f'Sub-Realms: {flush_data.subrealm_count:,d} ({subrealm_delta:+,d}) '
                         f'Tickers: {flush_data.ticker_count:,d} ({ticker_delta:+,d}) '
                         f'Containers: {flush_data.container_count:,d} ({container_delta:+,d}) '
                         f'Distributed Mints: {flush_data.distmint_count:,d} ({distmint_delta:+,d})')

    def fs_update_header_offsets(self, offset_start, height_start, headers):
        if self.coin.STATIC_BLOCK_HEADERS:
            return
        offset = offset_start
        offsets = []
        for h in headers:
            offset += len(h)
            offsets.append(pack_le_uint64(offset))
        # For each header we get the offset of the next header, hence we
        # start writing from the next height
        pos = (height_start + 1) * 8
        self.headers_offsets_file.write(pos, b''.join(offsets))

    def dynamic_header_offset(self, height):
        assert not self.coin.STATIC_BLOCK_HEADERS
        offset, = unpack_le_uint64(self.headers_offsets_file.read(height * 8, 8))
        return offset

    def dynamic_header_len(self, height):
        return self.dynamic_header_offset(height + 1)\
               - self.dynamic_header_offset(height)

    def backup_fs(self, height, tx_count, atomical_count, ticker_count, realm_count, subrealm_count, container_count, distmint_count):
        '''Back up during a reorg.  This just updates our pointers.'''
        self.fs_height = height
        self.fs_tx_count = tx_count
        self.fs_atomical_count = atomical_count
        self.fs_ticker_count = ticker_count
        self.fs_realm_count = realm_count
        self.fs_subrealm_count = subrealm_count
        self.fs_container_count = container_count
        self.fs_distmint_count = distmint_count
        # Truncate header_mc: header count is 1 more than the height.
        self.header_mc.truncate(height + 1)

    async def raw_header(self, height):
        '''Return the binary header at the given height.'''
        header, n = await self.read_headers(height, 1)
        if n != 1:
            raise IndexError(f'height {height:,d} out of range')
        return header

    async def read_headers(self, start_height, count):
        '''Requires start_height >= 0, count >= 0.  Reads as many headers as
        are available starting at start_height up to count.  This
        would be zero if start_height is beyond self.db_height, for
        example.

        Returns a (binary, n) pair where binary is the concatenated
        binary headers, and n is the count of headers returned.
        '''
        if start_height < 0 or count < 0:
            raise self.DBError(f'{count:,d} headers starting at '
                               f'{start_height:,d} not on disk')

        def read_headers():
            # Read some from disk
            disk_count = max(0, min(count, self.db_height + 1 - start_height))
            if disk_count:
                offset = self.header_offset(start_height)
                size = self.header_offset(start_height + disk_count) - offset
                return self.headers_file.read(offset, size), disk_count
            return b'', 0

        return await run_in_thread(read_headers)

    def fs_tx_hash(self, tx_num):
        '''Return a pair (tx_hash, tx_height) for the given tx number.

        If the tx_height is not on disk, returns (None, tx_height).'''
        tx_height = bisect_right(self.tx_counts, tx_num)
        if tx_height > self.db_height:
            tx_hash = None
        else:
            tx_hash = self.hashes_file.read(tx_num * 32, 32)
        return tx_hash, tx_height

    def fs_tx_hashes_at_blockheight(self, block_height):
        '''Return a list of tx_hashes at given block height,
        in the same order as in the block.
        '''
        if block_height > self.db_height:
            raise self.DBError(f'block {block_height:,d} not on disk (>{self.db_height:,d})')
        assert block_height >= 0
        if block_height > 0:
            first_tx_num = self.tx_counts[block_height - 1]
        else:
            first_tx_num = 0
        num_txs_in_block = self.tx_counts[block_height] - first_tx_num
        tx_hashes = self.hashes_file.read(first_tx_num * 32, num_txs_in_block * 32)
        assert num_txs_in_block == len(tx_hashes) // 32
        return [tx_hashes[idx * 32: (idx+1) * 32] for idx in range(num_txs_in_block)]

    async def tx_hashes_at_blockheight(self, block_height):
        return await run_in_thread(self.fs_tx_hashes_at_blockheight, block_height)

    async def fs_block_hashes(self, height, count):
        headers_concat, headers_count = await self.read_headers(height, count)
        if headers_count != count:
            raise self.DBError(f'only got {headers_count:,d} headers starting '
                               f'at {height:,d}, not {count:,d}')
        offset = 0
        headers = []
        for n in range(count):
            hlen = self.header_len(height + n)
            headers.append(headers_concat[offset:offset + hlen])
            offset += hlen

        return [self.coin.header_hash(header) for header in headers]

    async def limited_history(self, hashX, *, limit=1000):
        '''Return an unpruned, sorted list of (tx_hash, height) tuples of
        confirmed transactions that touched the address, earliest in
        the blockchain first.  Includes both spending and receiving
        transactions.  By default returns at most 1000 entries.  Set
        limit to None to get them all.
        '''
        def read_history():
            tx_nums = list(self.history.get_txnums(hashX, limit))
            fs_tx_hash = self.fs_tx_hash
            return [fs_tx_hash(tx_num) for tx_num in tx_nums]

        while True:
            history = await run_in_thread(read_history)
            if all(hash is not None for hash, height in history):
                return history
            self.logger.warning(f'limited_history: tx hash '
                                f'not found (reorg?), retrying...')
            await sleep(0.25)

    # -- Undo information

    def min_undo_height(self, max_height):
        '''Returns a height from which we should store undo info.'''
        return max_height - self.env.reorg_limit + 1

    def undo_key(self, height: int) -> bytes:
        '''DB key for undo information at the given height.'''
        return b'U' + pack_be_uint32(height)

    def atomicals_undo_key(self, height: int) -> bytes:
        '''DB key for atomicals undo information at the given height.'''
        return b'L' + pack_be_uint32(height)

    def read_undo_info(self, height):
        '''Read undo information from a file for the current height.'''
        return self.utxo_db.get(self.undo_key(height))

    def read_atomicals_undo_info(self, height):
        '''Read atomicals undo information from a file for the current height.'''
        return self.utxo_db.get(self.atomicals_undo_key(height))

    def flush_undo_infos(
            self, batch_put, undo_infos: Sequence[Tuple[Sequence[bytes], int]]
    ):
        '''undo_infos is a list of (undo_info, height) pairs.'''
        for undo_info, height in undo_infos:
            batch_put(self.undo_key(height), b''.join(undo_info))

    def flush_atomicals_undo_infos(
                self, batch_put, atomicals_undo_infos: Sequence[Tuple[Sequence[bytes], Sequence[bytes]]]
        ):
        '''undo_infos is a list of (atomicals_undo_info, height) pairs.'''
        for atomicals_undo_info, height in atomicals_undo_infos:
            batch_put(self.atomicals_undo_key(height), b''.join(atomicals_undo_info))

    def raw_block_prefix(self):
        return 'meta/block'

    def raw_block_path(self, height):
        return f'{self.raw_block_prefix()}{height:d}'

    def read_raw_block(self, height):
        '''Returns a raw block read from disk.  Raises FileNotFoundError
        if the block isn't on-disk.'''
        with util.open_file(self.raw_block_path(height)) as f:
            return f.read(-1)

    def write_raw_block(self, block, height):
        '''Write a raw block to disk.'''
        with util.open_truncate(self.raw_block_path(height)) as f:
            f.write(block)
        # Delete old blocks to prevent them accumulating
        try:
            del_height = self.min_undo_height(height) - 1
            os.remove(self.raw_block_path(del_height))
        except FileNotFoundError:
            pass

    def clear_excess_undo_info(self):
        '''Clear excess undo info.  Only most recent N are kept.'''
        prefix = b'U'
        min_height = self.min_undo_height(self.db_height)
        keys = []
        for key, _hist in self.utxo_db.iterator(prefix=prefix):
            height, = unpack_be_uint32(key[-4:])
            if height >= min_height:
                break
            keys.append(key)

        if keys:
            with self.utxo_db.write_batch() as batch:
                for key in keys:
                    batch.delete(key)
            self.logger.info(f'deleted {len(keys):,d} stale undo entries')

        # delete old block files
        prefix = self.raw_block_prefix()
        paths = [path for path in glob(f'{prefix}[0-9]*')
                 if len(path) > len(prefix)
                 and int(path[len(prefix):]) < min_height]
        if paths:
            for path in paths:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
            self.logger.info(f'deleted {len(paths):,d} stale block files')

    def clear_excess_atomicals_undo_info(self):
        '''Clear excess atomicals undo info.  Only most recent N are kept.'''
        prefix = b'L'
        min_height = self.min_undo_height(self.db_height)
        keys = []
        for key, _hist in self.utxo_db.iterator(prefix=prefix):
            height, = unpack_be_uint32(key[-4:])
            if height >= min_height:
                break
            keys.append(key)

        if keys:
            with self.utxo_db.write_batch() as batch:
                for key in keys:
                    batch.delete(key)
            self.logger.info(f'deleted {len(keys):,d} stale atomicals undo entries')

        # delete old block files
        prefix = self.raw_block_prefix()
        paths = [path for path in glob(f'{prefix}[0-9]*')
                 if len(path) > len(prefix)
                 and int(path[len(prefix):]) < min_height]
        if paths:
            for path in paths:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    pass
            self.logger.info(f'deleted {len(paths):,d} stale atomicals block files')

    # -- UTXO database

    def read_utxo_state(self):
        state = self.utxo_db.get(b'state')
        if not state:
            self.db_height = -1
            self.db_tx_count = 0
            self.db_atomical_count = 0
            self.db_ticker_count = 0
            self.db_realm_count = 0
            self.db_subrealm_count = 0
            self.db_container_count = 0
            self.db_distmint_count = 0
            self.db_tip = b'\0' * 32
            self.db_version = max(self.DB_VERSIONS)
            self.utxo_flush_count = 0
            self.wall_time = 0
            self.first_sync = True
        else:
            state = ast.literal_eval(state.decode())
            if not isinstance(state, dict):
                raise self.DBError('failed reading state from DB')
            self.db_version = state['db_version']
            if self.db_version not in self.DB_VERSIONS:
                raise self.DBError(f'your UTXO DB version is {self.db_version} '
                                   f'but this software only handles versions '
                                   f'{self.DB_VERSIONS}')
            # backwards compat
            genesis_hash = state['genesis']
            if isinstance(genesis_hash, bytes):
                genesis_hash = genesis_hash.decode()
            if genesis_hash != self.coin.GENESIS_HASH:
                raise self.DBError(f'DB genesis hash {genesis_hash} does not '
                                   f'match coin {self.coin.GENESIS_HASH}')
            self.db_height = state['height']
            self.db_tx_count = state['tx_count']
            self.db_atomical_count = state['atomical_count']
            self.db_ticker_count = state['ticker_count']
            self.db_realm_count = state['realm_count']
            self.db_subrealm_count = state['subrealm_count']
            self.db_container_count = state['container_count']
            self.db_distmint_count = state['distmint_count']
           
            self.db_tip = state['tip']
            self.utxo_flush_count = state['utxo_flush_count']
            self.wall_time = state['wall_time']
            self.first_sync = state['first_sync']

        # These are our state as we move ahead of DB state
        self.fs_height = self.db_height
        self.fs_tx_count = self.db_tx_count
        self.fs_atomical_count = self.db_atomical_count
        self.fs_ticker_count = self.db_ticker_count
        self.fs_realm_count = self.db_realm_count
        self.fs_subrealm_count = self.db_subrealm_count
        self.fs_container_count = self.db_container_count
        self.fs_distmint_count = self.db_distmint_count 

        self.last_flush_tx_count = self.fs_tx_count
        self.last_flush_atomical_count = self.fs_atomical_count
        self.last_flush_ticker_count = self.fs_ticker_count
        self.last_flush_realm_count = self.fs_realm_count
        self.last_flush_subrealm_count = self.fs_subrealm_count
        self.last_flush_container_count = self.fs_container_count
        self.last_flush_distmint_count = self.fs_distmint_count

        # Upgrade DB
        if self.db_version != max(self.DB_VERSIONS):
            self.upgrade_db()

        # Log some stats
        self.logger.info(f'UTXO DB version: {self.db_version:d}')
        self.logger.info(f'coin: {self.coin.NAME}')
        self.logger.info(f'network: {self.coin.NET}')
        self.logger.info(f'height: {self.db_height:,d}')
        self.logger.info(f'tip: {hash_to_hex_str(self.db_tip)}')
        self.logger.info(f'tx count: {self.db_tx_count:,d}')
        self.logger.info(f'atomical count: {self.db_atomical_count:,d}')
        self.logger.info(f'ticker count: {self.db_ticker_count:,d}')
        self.logger.info(f'realm count: {self.db_realm_count:,d}')
        self.logger.info(f'subrealm count: {self.db_subrealm_count:,d}')
        self.logger.info(f'container count: {self.db_container_count:,d}')
        self.logger.info(f'distmint count: {self.db_distmint_count:,d}')

        if self.utxo_db.for_sync:
            self.logger.info(f'flushing DB cache at {self.env.cache_MB:,d} MB')
        if self.first_sync:
            self.logger.info(
                f'sync time so far: {util.formatted_time(self.wall_time)}'
            )

    def upgrade_db(self):
        self.logger.info(f'UTXO DB version: {self.db_version}')
        self.logger.info('Upgrading your DB; this can take some time...')

        def upgrade_u_prefix(prefix):
            count = 0
            with self.utxo_db.write_batch() as batch:
                batch_delete = batch.delete
                batch_put = batch.put
                # Key: b'u' + address_hashX + tx_idx + tx_num
                for db_key, db_value in self.utxo_db.iterator(prefix=prefix):
                    if len(db_key) == 21:
                        return
                    break
                if self.db_version == 6:
                    for db_key, db_value in self.utxo_db.iterator(prefix=prefix):
                        count += 1
                        batch_delete(db_key)
                        batch_put(db_key[:14] + b'\0\0' + db_key[14:] + b'\0', db_value)
                else:
                    for db_key, db_value in self.utxo_db.iterator(prefix=prefix):
                        count += 1
                        batch_delete(db_key)
                        batch_put(db_key + b'\0', db_value)
            return count

        last = time.monotonic()
        count = 0
        for cursor in range(65536):
            prefix = b'u' + pack_be_uint16(cursor)
            count += upgrade_u_prefix(prefix)
            now = time.monotonic()
            if now > last + 10:
                last = now
                self.logger.info(f'DB 1 of 3: {count:,d} entries updated, '
                                 f'{cursor * 100 / 65536:.1f}% complete')
        self.logger.info('DB 1 of 3 upgraded successfully')

        def upgrade_h_prefix(prefix):
            count = 0
            with self.utxo_db.write_batch() as batch:
                batch_delete = batch.delete
                batch_put = batch.put
                # Key: b'h' + compressed_tx_hash + tx_idx + tx_num
                for db_key, db_value in self.utxo_db.iterator(prefix=prefix):
                    if len(db_key) == 14:
                        return
                    break
                if self.db_version == 6:
                    for db_key, db_value in self.utxo_db.iterator(prefix=prefix):
                        count += 1
                        batch_delete(db_key)
                        batch_put(db_key[:7] + b'\0\0' + db_key[7:] + b'\0', db_value)
                else:
                    for db_key, db_value in self.utxo_db.iterator(prefix=prefix):
                        count += 1
                        batch_delete(db_key)
                        batch_put(db_key + b'\0', db_value)
            return count

        last = time.monotonic()
        count = 0
        for cursor in range(65536):
            prefix = b'h' + pack_be_uint16(cursor)
            count += upgrade_h_prefix(prefix)
            now = time.monotonic()
            if now > last + 10:
                last = now
                self.logger.info(f'DB 2 of 3: {count:,d} entries updated, '
                                 f'{cursor * 100 / 65536:.1f}% complete')

        # Upgrade tx_counts file
        size = (self.db_height + 1) * 8
        tx_counts = self.tx_counts_file.read(0, size)
        if len(tx_counts) == (self.db_height + 1) * 4:
            tx_counts = array('I', tx_counts)
            tx_counts = array('Q', tx_counts)
            self.tx_counts_file.write(0, tx_counts.tobytes())

        self.db_version = max(self.DB_VERSIONS)
        with self.utxo_db.write_batch() as batch:
            self.write_utxo_state(batch)
        self.logger.info('DB 2 of 3 upgraded successfully')

    def write_utxo_state(self, batch):
        '''Write (UTXO) state to the batch.'''
        state = {
            'genesis': self.coin.GENESIS_HASH,
            'height': self.db_height,
            'tx_count': self.db_tx_count,
            'atomical_count': self.db_atomical_count,
            'ticker_count': self.db_ticker_count,
            'realm_count': self.db_realm_count,
            'subrealm_count': self.db_subrealm_count,
            'container_count': self.db_container_count,
            'distmint_count': self.db_distmint_count,
            'tip': self.db_tip,
            'utxo_flush_count': self.utxo_flush_count,
            'wall_time': self.wall_time,
            'first_sync': self.first_sync,
            'db_version': self.db_version,
        }
        batch.put(b'state', repr(state).encode())

    def set_flush_count(self, count):
        self.utxo_flush_count = count
        with self.utxo_db.write_batch() as batch:
            self.write_utxo_state(batch)

    def get_atomical_mint_info_dump(self, atomical_id):
        mint_info_value_dump = self.utxo_db.get(b'mi' + atomical_id)
        if not mint_info_value_dump:
            raise IndexError(f'get_atomical_mint_info_dump {atomical_id.hex()} mint db record not found. Index error.')
        return mint_info_value_dump
 
    async def get_atomical_id_by_atomical_number(self, atomical_number):
        def read_atomical_id():
            atomical_num_key = b'no' + pack_be_uint64(int(atomical_number))
            atomical_id_value = self.utxo_db.get(atomical_num_key)
            if not atomical_id_value:
                self.logger.error(f'n{atomical_id_value} atomical id not found by atomical number')
                return None
            return atomical_id_value

        return await run_in_thread(read_atomical_id)
 
    def get_realm(self, realm):
        realm_key = b'rlm' + realm
        realm_value = self.utxo_db.get(realm_key)
        if realm_value:
            return realm_value
        return None
    
    def get_subrealm(self, parent_atomical_id, subrealm):
        subrealm_key = b'srlm' + parent_atomical_id + subrealm
        subrealm_value = self.utxo_db.get(subrealm_key)
        if subrealm_value:
            return subrealm_value
        return None

    def get_container(self, container):
        container_key = b'co' + container
        container_value = self.utxo_db.get(container_key)
        if container_value:
            return container_value
        return None

    def get_ticker(self, ticker):
        ticker_key = b'tick' + ticker
        ticker_value = self.utxo_db.get(ticker_key)
        if ticker_value:
            return ticker_value
        return None

    # Query all the contract crt properties and return them sorted descending by height
    def get_contract_interface_history(self, atomical_id):
        crt_atomical_id_key_prefix = b'crt' + atomical_id
        crts = []
        for crt_atomical_id_key, crt_atomical_id_value in self.utxo_db.iterator(prefix=crt_atomical_id_key_prefix):
            atomical_id_ex = crt_atomical_id_key[3 : ATOMICAL_ID_LEN]
            if atomical_id_ex != atomical_id:
                raise IndexError(f'Developer error for contract prefix {atomical_id}')
            
            atomical_id_ex = crt_atomical_id_key[3 : ATOMICAL_ID_LEN]
            txnum_padding = bytes(8-TXNUM_LEN)
            tx_numb = crt_atomical_id_key[3 + ATOMICAL_ID_LEN : 3 + ATOMICAL_ID_LEN + TXNUM_LEN]
            tx_num_padded, = unpack_le_uint64(tx_numb + txnum_padding)
            height_le = crt_atomical_id_key[-4:]
            height, = unpack_le_uint32(height_le)
            obj = {
                'atomical_id': atomical_id,
                'tx_num': tx_num,
                'height': height,
                'payload': pickle.loads(crt_atomical_id_value)
            }
            crts.append(obj)
        # Sort by descending height
        crts.sort(key=lambda x: x.height, reverse=True)
        return crts

    def get_atomicals_by_location(self, location): 
        # Get any other atomicals at the same location
        atomicals_at_location = []
        atomicals_at_location_prefix = b'i' + location
        for location_key, location_result_value in self.utxo_db.iterator(prefix=atomicals_at_location_prefix):
            atomicals_at_location.append(location_id_bytes_to_compact(location_key[ 1 + ATOMICAL_ID_LEN : 1 + ATOMICAL_ID_LEN + ATOMICAL_ID_LEN]))
        return atomicals_at_location

    def get_atomicals_by_utxo(self, utxo):
        location = utxo.tx_hash + pack_le_uint32(utxo.tx_pos)
        return self.get_atomicals_by_location(location)

    async def get_by_atomical_id(self, atomical_id, verbose_mint_data = False):
        '''Return all UTXOs for an address sorted in no particular order.'''

        def read_atomical():
            utxos = []
            utxos_append = utxos.append
            mint_tx_hash, mint_output_index = get_tx_hash_index_from_location_id(atomical_id)
            # Get Mint data
            atomical_mint_data_key = b'md' + atomical_id
            db_mint_value = self.utxo_db.get(atomical_mint_data_key)
            if not db_mint_value:
                return None
    
            # Get Mint Info
            atomical_mint_info_key = b'mi' + atomical_id
            atomical_mint_info_value = self.utxo_db.get(atomical_mint_info_key)
            if not atomical_mint_info_value:
                self.logger.error(f'mi{atomical_id} mint info not found for get_by_atomical_id')
                return None
            mint_info = pickle.loads(atomical_mint_info_value)
            assert(mint_output_index == mint_info['index'])

            location_info = []
            atomical_active_location_key_prefix = b'a' + atomical_id
            for atomical_active_location_key, atomical_active_location_value in self.utxo_db.iterator(prefix=atomical_active_location_key_prefix):
                if not atomical_active_location_value:
                    self.logger.error(f'a{atomical_id.hex()} active location not found for get_by_atomical_id')
                    # This can happen if the DB was updated between
                    # getting the hashXs and getting the UTXOs
                    return None
                location = atomical_active_location_key[1 + ATOMICAL_ID_LEN : 1 + ATOMICAL_ID_LEN + ATOMICAL_ID_LEN]
                atomical_output_script_key = b'po' + location
                atomical_output_script_value = self.utxo_db.get(atomical_output_script_key)
                if not atomical_output_script_value:
                    self.logger.error(f'a{atomical_id.hex()} location {location.hex()} script not found for get_by_atomical_id')
                    return None
                location_script = atomical_output_script_value
                location_tx_hash = location[ : 32]
                atomical_location_idx, = unpack_le_uint32(location[ 32 : 36])
                location_scripthash = atomical_active_location_value[HASHX_LEN : HASHX_LEN + SCRIPTHASH_LEN]  
                location_value, = unpack_le_uint64(atomical_active_location_value[HASHX_LEN + SCRIPTHASH_LEN : HASHX_LEN + SCRIPTHASH_LEN + 8])
                atomicals_at_location = self.get_atomicals_by_location(location)
                location_info.append({
                    'location': location_id_bytes_to_compact(location),
                    'txid': hash_to_hex_str(location_tx_hash),
                    'index': atomical_location_idx,
                    'scripthash': hash_to_hex_str(location_scripthash),
                    'scripthash_hex': location_scripthash.hex(),
                    'value': location_value,
                    'script': location_script.hex(),
                    'atomicals_at_location': atomicals_at_location
                })
            prefix = b'st' + atomical_id
            state_fields = {}
            state_history = []
            for db_state_key, db_state_value in self.utxo_db.iterator(prefix=prefix, reverse=True):
                # We obtain the tx number from big endian 64 bit and convert it to 64 bit little endian, with the TXNUM_LEN truncation
                tx_numb = db_state_key[ 1 + ATOMICAL_ID_LEN : 1 + ATOMICAL_ID_LEN + TXNUM_LEN]
                # Now lookup from the file system and add the padding
                txnum_padding = bytes(8-TXNUM_LEN)
                tx_num_padded, = unpack_le_uint64(tx_numb + txnum_padding)
                state_tx_hash, state_height = self.fs_tx_hash(tx_num_padded)
                out_idx_packed = db_state_key[ 1 + ATOMICAL_ID_LEN + TXNUM_LEN: 1 + ATOMICAL_ID_LEN + TXNUM_LEN + 4]
                out_idx = unpack_le_uint32(out_idx_packed)
                state_entry = {'tx_num': tx_num_padded, 'height': state_height, 'txid': hash_to_hex_str(state_tx_hash), 'index': out_idx}
                state_data = {}
                FIELD_START_IDX = 1 + ATOMICAL_ID_LEN + TXNUM_LEN + 1
                field_name = db_state_key[FIELD_START_IDX : ]
                state_entry['data'] = db_state_value
                state_history.append(state_entry)

            prefix = b'evt' + atomical_id
            rpc_fields = {}
            msg_history = []
            for db_rpc_key, db_rpc_value in self.utxo_db.iterator(prefix=prefix, reverse=True):
                # We obtain the tx number from big endian 64 bit and convert it to 64 bit little endian, with the TXNUM_LEN truncation
                tx_numb = db_rpc_value[ 1 + ATOMICAL_ID_LEN : 1 + ATOMICAL_ID_LEN + TXNUM_LEN]
                # Now lookup from the file system and add the padding
                txnum_padding = bytes(8-TXNUM_LEN)
                tx_num_padded, = unpack_le_uint64(tx_numb + txnum_padding)
                rpc_tx_hash, rpc_height = self.fs_tx_hash(tx_num_padded)
                out_idx_packed = db_rpc_key[ 1 + ATOMICAL_ID_LEN + TXNUM_LEN: 1 + ATOMICAL_ID_LEN + TXNUM_LEN + 4]
                out_idx = unpack_le_uint32(out_idx_packed)
                rpc_entry = {'tx_num': tx_num_padded, 'height': rpc_height, 'txid': hash_to_hex_str(rpc_tx_hash), 'index': out_idx}
                state_data = {}
                FIELD_START_IDX = 1 + ATOMICAL_ID_LEN + TXNUM_LEN + 1
                field_name = db_rpc_key[FIELD_START_IDX : ]
                rpc_entry['data'] = db_rpc_value
                state_history.append(rpc_entry)

            # Get Atomical number and check match
            atomical_number = mint_info['number']
            atomical_number_key = b'no' + pack_be_uint64(atomical_number)
            atomical_number_value = self.utxo_db.get(atomical_number_key)
            if not atomical_number_value:
                self.logger.error(f'n{atomical_number} atomical number not found. IndexError.')
                raise IndexError(f'n{atomical_number} atomical number not found. IndexError.')
           
            assert(atomical_number_value == atomical_id)
            assert(atomical_number_value == mint_tx_hash + pack_le_uint32(mint_output_index))
            assert(mint_output_index == mint_info['index'])
 
            atomical = {
                'atomical_id': atomical_id,
                'atomical_number': atomical_number,
                'type': mint_info['type'],
                'location_info': location_info,
                'mint_info': {
                    'txid':  hash_to_hex_str(mint_tx_hash),
                    'index': mint_output_index,
                    'blockheader': mint_info['header'].hex(),
                    'blockhash': hash_to_hex_str(self.coin.header_hash(mint_info['header'])),
                    'height': mint_info['height'],
                    'scripthash': hash_to_hex_str(mint_info['scripthash']),
                    'scripthash_hex': mint_info['scripthash'].hex(),
                    'script': mint_info['script'].hex(),
                    'value': mint_info['value'],
                    'args': mint_info['args'],
                    'meta': mint_info['meta']
                },
                'state': {
                    'history': state_history
                },
                'evt': {
                    'history': msg_history
                }
            }

            mint_deferred = mint_info.get('$mint_deferred', False)
            if mint_deferred:
                atomical['$mint_deferred'] = True
                atomical['$max_supply'] = mint_info['$max_supply']
            else: 
                atomical['$mint_deferred'] = False
                atomical['$max_supply'] = mint_info['$max_supply']
                atomical['$mint_height'] = mint_info['$mint_height']
                atomical['$mint_amount'] = mint_info['$mint_amount']
                atomical['$max_mints'] = mint_info['$max_mints']

            unpacked_data_summary = check_unpack_field_data(db_mint_value)
            if unpacked_data_summary != None:
                atomical['mint_info']['fields'] = unpacked_data_summary
            else: 
                atomical['mint_info']['fields'] = {}
            return atomical
        atomical = await run_in_thread(read_atomical)
        return atomical

    def get_atomical_id_by_realm(self, name):
        name_key = b'rlm' + name
        return self.utxo_db.get(name_key)

    def get_atomical_id_by_ticker(self, ticker):
        ticker_key = b'tick' + ticker
        return self.utxo_db.get(ticker_key)
 
    def get_atomical_id_by_container(self, container):
        container_key = b'co' + container
        return self.utxo_db.get(container_key) 

    async def get_atomicals_list(self, limit, offset, asc = False):
        if limit > 50:
            limit = 50

        def read_atomical_list():   
            atomical_ids = []
            search_starting_at_atomical_number = 0
            # If no offset provided, then assume we want to start from the highest
            if offset == None or offset == 0: 
                prefix = b'no'
                for atomical_number_key, atomical_id_value in self.utxo_db.iterator(prefix=prefix, reverse=True):
                    search_starting_at_atomical_number, = unpack_be_uint64(atomical_number_key[ 1 : 1 + 8])
                    break
            else:
                search_starting_at_atomical_number = offset
            
            # Generate up to limit number of keys to search
            list_of_keys = []
            for x in range(limit):
                if asc:
                    current_key = b'no' + pack_be_uint64(search_starting_at_atomical_number + x)
                    list_of_keys.append(current_key)
                else:
                    # Do not go to 0 or below
                    if search_starting_at_atomical_number - x <= 0:
                        break 
                    current_key = b'no' + pack_be_uint64(search_starting_at_atomical_number - x)
                    list_of_keys.append(current_key)

            # Get all of the atomicals in the order of the keys
            for search_key in list_of_keys:
                atomical_id_value = self.utxo_db.get(search_key)
                if atomical_id_value:
                    atomical_ids.append(atomical_id_value)
                else: 
                    # Once we do not find one, then we are done because there should be no more
                    break
            return atomical_ids

        return await run_in_thread(read_atomical_list)
 
    async def all_utxos(self, hashX):
        '''Return all UTXOs for an address sorted in no particular order.'''
        def read_utxos():
            utxos = []
            utxos_append = utxos.append
            txnum_padding = bytes(8-TXNUM_LEN)
            # Key: b'u' + address_hashX + txout_idx + tx_num
            # Value: the UTXO value as a 64-bit unsigned integer
            prefix = b'u' + hashX
            for db_key, db_value in self.utxo_db.iterator(prefix=prefix):
                txout_idx, = unpack_le_uint32(db_key[-TXNUM_LEN-4:-TXNUM_LEN])
                tx_num, = unpack_le_uint64(db_key[-TXNUM_LEN:] + txnum_padding)
                value, = unpack_le_uint64(db_value)
                tx_hash, height = self.fs_tx_hash(tx_num)
                utxos_append(UTXO(tx_num, txout_idx, tx_hash, height, value))
            return utxos

        while True:
            utxos = await run_in_thread(read_utxos)
            if all(utxo.tx_hash is not None for utxo in utxos):
                return utxos
            self.logger.warning(f'all_utxos: tx hash not '
                                f'found (reorg?), retrying...')
            await sleep(0.25)
 
    async def lookup_utxos(self, prevouts):
        '''For each prevout, lookup it up in the DB and return a (hashX,
        value) pair or None if not found.

        Used by the mempool code.
        '''
        def lookup_hashXs():
            '''Return (hashX, suffix) pairs, or None if not found,
            for each prevout.
            '''
            def lookup_hashX(tx_hash, tx_idx):
                idx_packed = pack_le_uint32(tx_idx)
                txnum_padding = bytes(8-TXNUM_LEN)

                # Key: b'h' + compressed_tx_hash + tx_idx + tx_num
                # Value: hashX
                prefix = b'h' + tx_hash[:COMP_TXID_LEN] + idx_packed

                # Find which entry, if any, the TX_HASH matches.
                for db_key, hashX in self.utxo_db.iterator(prefix=prefix):
                    tx_num_packed = db_key[-TXNUM_LEN:]
                    tx_num, = unpack_le_uint64(tx_num_packed + txnum_padding)
                    hash, _height = self.fs_tx_hash(tx_num)
                    if hash == tx_hash:
                        return hashX, idx_packed + tx_num_packed
                return None, None
            return [lookup_hashX(*prevout) for prevout in prevouts]

        def lookup_utxos(hashX_pairs):
            def lookup_utxo(hashX, suffix):
                if not hashX:
                    # This can happen when the daemon is a block ahead
                    # of us and has mempool txs spending outputs from
                    # that new block
                    return None
                # Key: b'u' + address_hashX + tx_idx + tx_num
                # Value: the UTXO value as a 64-bit unsigned integer
                key = b'u' + hashX + suffix
                db_value = self.utxo_db.get(key)
                if not db_value:
                    # This can happen if the DB was updated between
                    # getting the hashXs and getting the UTXOs
                    return None
                value, = unpack_le_uint64(db_value)
                return hashX, value
            return [lookup_utxo(*hashX_pair) for hashX_pair in hashX_pairs]

        hashX_pairs = await run_in_thread(lookup_hashXs)
        return await run_in_thread(lookup_utxos, hashX_pairs)
