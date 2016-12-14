/**
 * Copyright (c) 2012 - 2016 YCSB contributors. All rights reserved.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import javax.cache.Cache.Entry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.transactions.Transaction;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Apache Ignite client.
 *
 * @author qadahtm
 */
public class IgniteClient extends DB {

  private static final String CONF_FILE_PROPERTY = "ignite.conffile";
  private static final String CACHE_NAME_PROPERTY = "ignite.cachename";
  private static final String TX_MODE_PROPERTY = "ignite.txmode";

  private final Logger log = Logger.getLogger(getClass().getName());

  private Ignite ignite = null;
  private IgniteCache<String, String> db = null;
  private boolean transactional = false;

  private CacheConfiguration<String, String> cacheCfg = null;

  private static AtomicInteger threadCount = new AtomicInteger();

  private Transaction txn = null;
  
  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String confFile = props.getProperty(CONF_FILE_PROPERTY);
    String cacheName = props.getProperty(CACHE_NAME_PROPERTY);
    transactional = Boolean.parseBoolean(props.getProperty(TX_MODE_PROPERTY));

    if (confFile == null) {
      throw new DBException(String.format(
          "Required property \"%s\" missing for IgniteClient",
          CONF_FILE_PROPERTY));
    }

    if (cacheName == null) {
      throw new DBException(String.format(
          "Required property \"%s\" missing for IgniteClient",
          CACHE_NAME_PROPERTY));
    }

    cacheCfg = new CacheConfiguration<String, String>(cacheName);

//    if (transactional) {
//      cacheCfg.setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL);      
//    } else {
//      cacheCfg.setAtomicityMode(CacheAtomicityMode.ATOMIC);
//    }
//
    IgniteConfiguration cfg = Ignition.loadSpringBean(confFile, "ignite.cfg");
//
//    cfg.setCacheConfiguration(cacheCfg);
//    cfg.setPeerClassLoadingEnabled(false);

    // Optional transaction configuration. Configure TM lookup here.
//    TransactionConfiguration txCfg = new TransactionConfiguration();
//
//    cfg.setTransactionConfiguration(txCfg);
//    cfg.setClientMode(true);

    // create ignite client
    
    ignite = Ignition.getOrStart(cfg);
    
    db = ignite.getOrCreateCache(cacheName);

    int c = threadCount.incrementAndGet();
    
//    System.out.println("thread count = " + c);
  }

  @Override
  public void cleanup() throws DBException {
    if (ignite != null) {
      int c = threadCount.get();
//      System.out.println("thread count = " + c);
      System.out.println("cleaning up");
      c = threadCount.decrementAndGet();
//      System.out.println("new thread count = " + c);
      if (c == 0) {
        System.out.println("all threads are cleaning up, closing ignite " + c);
        ignite.close();
      }
      log.info("ignite instance is closed");
    } else {
      log.info("ignite instance is null!!");
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.yahoo.ycsb.DB#start()
   */
  @Override
  public void start() throws DBException {
    if (transactional) {
      if (txn == null) {
        txn = ignite.transactions().txStart();
      }      
    }
    super.start();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.yahoo.ycsb.DB#commit()
   */
  @Override
  public void commit() throws DBException {
    if (transactional) {
      if (txn != null) {
        txn.commit();
      }
      txn = null;
    }
    super.commit();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.yahoo.ycsb.DB#abort()
   */
  @Override
  public void abort() throws DBException {
    super.abort();
    throw new DBException("Abort is not supported in Ignite");
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      // need to serialize values
      String svalue = Utils.stringify(values);
      db.put(Utils.formatKey(table, key), svalue);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    try {
      boolean res = db.remove(Utils.formatKey(table, key));
      // TODO: what if res == false???
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      HashMap<String, ByteIterator> result) {
    try {
      String dbkey = Utils.formatKey(table, key);
      HashMap<String, String> values = new HashMap<String, String>();
      String svalue = db.get(dbkey);
      values = Utils.deStringfyValue(fields, svalue);

      StringByteIterator.putAllAsByteIterators(result, values);
      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      String svalue = Utils.stringify(values);
      db.put(Utils.formatKey(table, key), svalue);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // this will ignore the startkey and reads the number of records specified
    // by recordcount

    Iterator<Entry<String, String>> iter = db.iterator();

    while (iter.hasNext() && recordcount > 0) {
      recordcount--;
      Entry<String, String> rec = iter.next();
      String svalue = rec.getValue();
      HashMap<String, ByteIterator> val = StringByteIterator
          .getByteIteratorMap((Utils.deStringfyValue(fields, svalue)));
      result.add(val);
    }

    return Status.OK;
  }
}
