#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 18 11:52:31 2018

@author: adrian
"""
from binance.client import Client
from binance.depthcache import DepthCacheManager
import threading
import queue
import time
import csv



client = Client("", "")


def process_depth(depth_cache):
    if depth_cache is not None:
        print("ProducetThread: Adding {} asks and bits.".format(depth_cache.symbol))
        asks.put(depth_cache.get_asks())
        bids.put(depth_cache.get_bids())
        print("ProducetThread: Added!")
    else:
        pass


def write_to_csv(order_set, typeof, writer):
    for order in order_set:
        l = list(order)
        l.append(typeof)
        l.append(time.time())
        writer.writerow(l)

def consumer(q, side):
    old_item_set = set()
    with open(side+'.xls','a') as file:
        writer = csv.writer(file)
        while(not e.isSet()):
            new_item = q.get()
            print(side+"Thread: processing item")
            new_item_set = set(tuple(l) for l in new_item)
            new_orders = new_item_set.difference(old_item_set)
            write_to_csv(new_orders,'open',writer)
            closed_orders = old_item_set.difference(new_item_set)
            write_to_csv(closed_orders,'close',writer)
            old_item_set = new_item_set 
            q.task_done()
            print(side+"Thread: Item processed")
    
def producer(e):
    # the main thread will put new items to the queue
    dcm = DepthCacheManager(client, 'BNBBTC', callback = process_depth)
    e.wait()
    dcm.close()    

if __name__ == '__main__':
    e = threading.Event()
    asks = queue.Queue()
    bids = queue.Queue()
    
    consume_asks = threading.Thread(name = "AsksConsumerThread", target=consumer, args=(asks,'Ask'))
    consume_asks.start()
    consume_bids = threading.Thread(name = "BitsConsumerThread", target=consumer, args=(bids,'Bid'))
    consume_bids.start()

    producer = threading.Thread(name = "ProducerThread", target=producer, args=(e,))
    producer.start()
    asks.join()
    bids.join()
    e.set()
