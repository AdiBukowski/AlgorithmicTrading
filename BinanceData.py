#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Nov 18 11:52:31 2018

@author: adrian
"""
from binance.client import Client
import pandas as pd
from binance.depthcache import DepthCacheManager
import threading
import queue
import time
import csv

    

#
#old_item_df = pd.DataFrame({'Price':[1,2,3,4,1,2,3,4,22,2,2], 'Volume': [2,2,3,4,5,1,2,3,4,5,2]})
#item_df = pd.DataFrame({'Price':[1,2,3,21,1,2,3,4,3,2,2], 'Volume': [1,1,3,4,5,1,2,3,4,5,2]})
#
#symmetric_difference_df = symmetric_difference(old_item_df,item_df)
#new = symmetric_difference_df.loc[idx[:,'new'],:]
#old = symmetric_difference_df.loc[idx[:,'old'],:]
#write_to_csv()
#with open('Asks.xls','a') as file:
#    writer = csv.writer(file)
#    write_to_csv(new,'new',writer)

client = Client("Syjq3IzBw1c9lcAKtHkdXEFyPBFVULjDLXMlnwqPFce9wQ73szBYLtUIO7gsH1YM", "52d6ZBGnWa9WeltzPiwSgQP5nIfc2Z3SQGzxedUl0Nnp3xZBRF1jzM7W0BYQb8QM")


def process_depth(depth_cache):
    if depth_cache is not None:
        print("Adding {} asks and bits.".format(depth_cache.symbol))
        asks.put(depth_cache.get_asks()[:2])
        bids.put(depth_cache.get_bids()[:2])
        print("Added!")
    else:
        pass

def symmetric_difference(old,new):
    old_df = pd.DataFrame(old)
    new_df = pd.DataFrame(new)
    new_df['new'] = 'new'
    new_df.set_index('new', append=True, inplace = True)
    old_df['old'] = 'old'
    old_df.set_index('old', append=True, inplace = True)
    merged = old_df.append(new_df)
    merged = merged.drop_duplicates().sort_index()
    return(merged)


def write_to_csv(df, typeof, writer):
    for index, row in df.iterrows():
        l = list(row[['Price','Volume']].values)
        l.append(typeof)
        l.append(time.time())
        print(l)

def consumer(q, side):
    old_item_df = pd.DataFrame({'Price' : [], 'Volume': []})
    with open(side+'.xls','a') as file:
        writer = csv.writer(file)
        while(not e.isSet()):
            item = asks.get()
            print(side+"Thread BNBBTC consumed:"+ str(item) + '\n')
            item_df = pd.DataFrame({'Price': [l[0] for l in item],'Volume': [l[1] for l in item]})
            symmetric_difference_df = symmetric_difference(old_item_df,item_df)
            idx = pd.IndexSlice
            try:
                new = symmetric_difference_df.loc[idx[:,'new'],:]
                write_to_csv(new,'open',writer)
            except:
                pass
            try:
                old = symmetric_difference_df.loc[idx[:,'old'],:]
                write_to_csv(old,'done',writer)
            except:
                pass
            old_item_df = pd.DataFrame({'Price': [l[0] for l in item],'Volume': [l[1] for l in item]})
            q.task_done()
    
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
