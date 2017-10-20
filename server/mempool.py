# Copyright (c) 2016, Neil Booth
#
# All rights reserved.
#
# See the file "LICENCE" for information about the copyright
# and warranty status of this software.

'''Mempool handling.'''

import asyncio
import itertools
import time
from collections import defaultdict

from lib.hash import hash_to_str, hex_str_to_hash
import lib.util as util
from server.daemon import DaemonError


class MemPool(util.LoggedClass):
    '''Representation of the daemon's mempool.

    Updated regularly in caught-up state.  Goal is to enable efficient
    response to the value() and transactions() calls.
    
    tx_hash=交易id，即sha256(sha256(交易内容))
    !!hashX=sha256(交易脚本)[0:11]，
    对于比特币区块数据而言，txout中存放的为交易脚本，hashX为P2SH或者是P2PKH的sha256的一部分，如果是P2PK，则转换成P2PKH再sha256
    对于比特币地址而言，hashX需要根据地址类型，组装成p2sh或p2pkh后，再sha256
    To that end we maintain the following maps:
       tx_hash -> (txin_pairs, txout_pairs)
       hashX   -> set of all tx hashes in which the hashX appears

    A pair is a (hashX, value) tuple.  tx hashes are hex strings.
    txin_pairs=(hashX(从txhex+txidx转换过来), amout(输入交易的总数量，从txhex+txidx转换过来))
    txout_pairs=(hashX, amount)
    '''

    def __init__(self, bp, controller):
        super().__init__()
        self.daemon = bp.daemon
        self.controller = controller
        self.coin = bp.coin
        self.db = bp
        #touched标记有变化的交易
        self.touched = bp.touched

        self.touched_event = asyncio.Event()
        self.prioritized = set()
        self.stop = False
        #交易hash->txin,txout, 包含为确认的交易
        self.txs = {}
        #地址->出现过该地址的交易hash，包含未被确认的交易
        self.hashXs = defaultdict(set)  # None can be a key

    def prioritize(self, tx_hash):
        '''Prioritize processing the given hash.  This is important during
        initial mempool sync.'''
        self.prioritized.add(tx_hash)
 
    def resync_daemon_hashes(self, unprocessed, unfetched):
        '''Re-sync self.txs with the list of hashes in the daemon's mempool.

        Additionally, remove gone hashes from unprocessed and
        unfetched.  Add new ones to unprocessed.
        unfetched: 所有为确认的交易的tx_hash
        unprocessed: 
        '''
        txs = self.txs
        hashXs = self.hashXs
        touched = self.touched
        #从bitcoind取出缓存的未确认交易
        hashes = self.daemon.cached_mempool_hashes()
        #取之前未确认交易中已经被确认了的交易
        gone = set(txs).difference(hashes)
        for hex_hash in gone:
            unfetched.discard(hex_hash)
            unprocessed.pop(hex_hash, None)
            item = txs.pop(hex_hash)
            if item:
                txin_pairs, txout_pairs = item
                tx_hashXs = set(hashX for hashX, value in txin_pairs)
                tx_hashXs.update(hashX for hashX, value in txout_pairs)
                #txin和txout中的hashX都已经被确认，从hashXs中删除掉
                for hashX in tx_hashXs:
                    hashXs[hashX].remove(hex_hash)
                    #如果hashXs中该hashX的全部交易已经被确认完毕，将入口删除
                    if not hashXs[hashX]:
                        del hashXs[hashX]
                touched.update(tx_hashXs)
        #获取新收到的未确认交易
        new = hashes.difference(txs)
        unfetched.update(new)
        for hex_hash in new:
            #未确认交易标记为None,用来确认是否有交易依赖了还未被确认的交易
            txs[hex_hash] = None

    async def main_loop(self):
        '''Asynchronously maintain mempool status with daemon.

        Processes the mempool each time the daemon's mempool refresh
        event is signalled.
        '''
        #unprocessed为所有未确认交易的tx_hash->raw_tx(即tx的二进制数据)
        unprocessed = {}
        #unfetched为当前所有未确认的交易的交易tx_hash
        unfetched = set()
        txs = self.txs
        fetch_size = 800
        process_some = self.async_process_some(unfetched, fetch_size // 2)

        await self.daemon.mempool_refresh_event.wait()
        self.logger.info('beginning processing of daemon mempool.  '
                         'This can take some time...')
        next_log = 0
        loops = -1  # Zero during initial catchup

        while True:
            # Avoid double notifications if processing a block
            if self.touched and not self.processing_new_block():
                self.touched_event.set()

            # Log progress / state
            todo = len(unfetched) + len(unprocessed)
            if loops == 0:
                pct = (len(txs) - todo) * 100 // len(txs) if txs else 0
                self.logger.info('catchup {:d}% complete '
                                 '({:,d} txs left)'.format(pct, todo))
            if not todo:
                loops += 1
                now = time.time()
                if now >= next_log and loops:
                    self.logger.info('{:,d} txs touching {:,d} addresses'
                                     .format(len(txs), len(self.hashXs)))
                    next_log = now + 150

            try:
                if not todo:
                    self.prioritized.clear()
                    await self.daemon.mempool_refresh_event.wait()
                #删除已经被确认的交易，将新增加的未确认交易的tx_hash添加到unfetched里
                self.resync_daemon_hashes(unprocessed, unfetched)
                self.daemon.mempool_refresh_event.clear()

                if unfetched:
                    count = min(len(unfetched), fetch_size)
                    #用unfetched取出数据写入unproessed
                    hex_hashes = [unfetched.pop() for n in range(count)]                    
                    unprocessed.update(await self.fetch_raw_txs(hex_hashes))

                if unprocessed:
                    #处理unprocessed
                    await process_some(unprocessed)
            except DaemonError as e:
                self.logger.info('ignoring daemon error: {}'.format(e))
            except asyncio.CancelledError:
                # This aids clean shutdowns
                self.stop = True
                break

    def async_process_some(self, unfetched, limit):
        pending = []
        txs = self.txs

        async def process(unprocessed):
            nonlocal pending

            raw_txs = {}
            #优先处理prioritized中的交易
            for hex_hash in self.prioritized:
                if hex_hash in unprocessed:
                    raw_txs[hex_hash] = unprocessed.pop(hex_hash)
            #获取处理交易，直到达到限制
            while unprocessed and len(raw_txs) < limit:
                hex_hash, raw_tx = unprocessed.popitem()
                raw_txs[hex_hash] = raw_tx

            if unprocessed:
                deferred = []
            else:
                deferred = pending
                pending = []

            result, deferred = await self.controller.run_in_executor(
                self.process_raw_txs, raw_txs, deferred)

            #把当前轮无法处理的放到下一轮，跟下一轮的一起处理
            pending.extend(deferred)
            hashXs = self.hashXs
            touched = self.touched
            #把当前轮成功处理的写到txs和hashXs两个表中。
            for hex_hash, in_out_pairs in result.items():
                if hex_hash in txs:
                    txs[hex_hash] = in_out_pairs
                    for hashX, value in itertools.chain(*in_out_pairs):
                        touched.add(hashX)
                        hashXs[hashX].add(hex_hash)

        return process

    def processing_new_block(self):
        '''Return True if we're processing a new block.'''
        #如果新区块处理完，cached_height应该等于db_height,因为
        # db_height处理完后被赋值为cached_height而最新的height
        # 还没有被从bitcoind同步过来
        return self.daemon.cached_height() > self.db.db_height

    async def fetch_raw_txs(self, hex_hashes):
        '''Fetch a list of mempool transactions.'''
        raw_txs = await self.daemon.getrawtransactions(hex_hashes)

        # Skip hashes the daemon has dropped.  Either they were
        # evicted or they got in a block.
        return {hh: raw for hh, raw in zip(hex_hashes, raw_txs) if raw}

    def process_raw_txs(self, raw_tx_map, pending):
        '''Process the dictionary of raw transactions and return a dictionary
        of updates to apply to self.txs.

        This runs in the executor so should not update any member
        variables it doesn't own.  Atomic reads of self.txs that do
        not depend on the result remaining the same are fine.
        '''
        script_hashX = self.coin.hashX_from_script
        deserializer = self.coin.DESERIALIZER
        db_utxo_lookup = self.db.db_utxo_lookup
        txs = self.txs

        # Deserialize each tx and put it in our priority queue
        for tx_hash, raw_tx in raw_tx_map.items():
            if tx_hash not in txs:
                continue
            tx, _tx_hash = deserializer(raw_tx).read_tx()

            # Convert the tx outputs into (hashX, value) pairs
            # tx的output为hashX,value,通过script_hashX处理hashX，实际上就是将所有P2PK转换为P2PKH，以节省内存存储空间
            txout_pairs = [(script_hashX(txout.pk_script), txout.value)
                           for txout in tx.outputs]

            # Convert the tx inputs to ([prev_hex_hash, prev_idx) pairs
            # tx的input为utxo的tx_hash和tx_idx
            txin_pairs = [(hash_to_str(txin.prev_hash), txin.prev_idx)
                          for txin in tx.inputs]

            pending.append((tx_hash, txin_pairs, txout_pairs))

        # Now process what we can
        result = {}
        deferred = []

        for item in pending:
            if self.stop:
                break

            tx_hash, old_txin_pairs, txout_pairs = item
            if tx_hash not in txs:
                continue

            mempool_missing = False
            txin_pairs = []

            try:
                #之所以叫old_txin_pairs是因为里面存的是tx_hash,tx_idx,需要等待转换为与txout一样的
                #utxo所在的hashX与amount
                for prev_hex_hash, prev_idx in old_txin_pairs:
                    #txs.get存在三种情况
                    #1 prev_hex_hash已经被确认,则txs中不会存在，返回0
                    #2 prev_hex_hash是本轮刚刚收到的新交易，在之前被设置为None，则此处返回None
                    #3 prev_hex_hash是本轮之前收到的新交易，但是目前还没有被确认，此处返回其数据tx_info
                    tx_info = txs.get(prev_hex_hash, 0)
                    if tx_info is None:
                        #情况2，虽然是刚刚接收到的新交易，但是看看交易是否较当前交易早接收到，如果是则已经被处理了
                        tx_info = result.get(prev_hex_hash)
                        if not tx_info:
                            #情况2,并且不在本轮已经被处理完成的请求内，则放到deferred里
                            mempool_missing = True
                            continue
                    if tx_info:
                        #数据在内存中，取该交易的txout的第prev_idx个utxo，放到当前的txin_pairs里
                        txin_pairs.append(tx_info[1][prev_idx])
                    elif not mempool_missing:
                        #如果是情况2的最终情况，则尝试从数据库里取得，应该在数据库里
                        prev_hash = hex_str_to_hash(prev_hex_hash)
                        #注意这里取出的txin已经从(tx_hash, tx_idx)转换为了(hashX, value)，即utxo的地址和支付数量
                        txin_pairs.append(db_utxo_lookup(prev_hash, prev_idx))
            except (self.db.MissingUTXOError, self.db.DBError):
                # DBError can happen when flushing a newly processed
                # block.  MissingUTXOError typically happens just
                # after the daemon has accepted a new block and the
                # new mempool has deps on new txs in that block.
                continue

            if mempool_missing:
                #加入延期
                deferred.append(item)
            else:
                #放到result里，可以供后续查找
                result[tx_hash] = (txin_pairs, txout_pairs)

        return result, deferred

    async def transactions(self, hashX):
        '''Generate (hex_hash, tx_fee, unconfirmed) tuples for mempool
        entries for the hashX.

        unconfirmed is True if any txin is unconfirmed.
        '''
        # hashXs is a defaultdict
        # 获取一个地址hashX在mempool中记录的交易
        if hashX not in self.hashXs:
            return []

        deserializer = self.coin.DESERIALIZER
        hex_hashes = self.hashXs[hashX]
        raw_txs = await self.daemon.getrawtransactions(hex_hashes)
        result = []
        for hex_hash, raw_tx in zip(hex_hashes, raw_txs):
            item = self.txs.get(hex_hash)
            if not item or not raw_tx:
                continue
            txin_pairs, txout_pairs = item
            tx_fee = (sum(v for hashX, v in txin_pairs) -
                      sum(v for hashX, v in txout_pairs))
            tx, tx_hash = deserializer(raw_tx).read_tx()
            unconfirmed = any(hash_to_str(txin.prev_hash) in self.txs
                              for txin in tx.inputs)
            result.append((hex_hash, tx_fee, unconfirmed))
        return result

    def value(self, hashX):
        '''Return the unconfirmed amount in the mempool for hashX.

        Can be positive or negative.
        '''
        #这里快速计算一个地址hashX有多少未确认交易额度
        value = 0
        # hashXs is a defaultdict
        if hashX in self.hashXs:
            for hex_hash in self.hashXs[hashX]:
                txin_pairs, txout_pairs = self.txs[hex_hash]
                value -= sum(v for h168, v in txin_pairs if h168 == hashX)
                value += sum(v for h168, v in txout_pairs if h168 == hashX)
        return value
