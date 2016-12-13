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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

import alluxio.AlluxioURI;
import alluxio.client.marketdb.BaseMarketDBReaderWriter;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileAlreadyExistsException;
import alluxio.client.mkeyvalue.MuKeyValueSystem;
import alluxio.client.mkeyvalue.MuKeyValueStore;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

/**
 * MarketDB client.
 *
 * @author qadahtm
 */
public class MarketDBClient extends DB implements
    com.yahoo.ycsb.StoredProcedureExecutor {

  private static final String STORE_URI_PROPERTY = "marketdb.uri";
  private static final String USE_STORAGE_LAYER_PROPERTY = "marketdb.mukv";
  private static final String KV_DEL = ":";
  private static final String ENTRY_DEL = ",";
  private final Logger log = Logger.getLogger(getClass().getName());

  private BaseMarketDBReaderWriter db1;
  private MuKeyValueStore mukv;
  private MuKeyValueSystem kvs;
  private boolean useMuKVstore = false;

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String storeUri = props.getProperty(STORE_URI_PROPERTY);

    if (storeUri == null) {
      throw new DBException(String.format(
          "Required property \"%s\" missing for MarketDBClient",
          STORE_URI_PROPERTY));
    }

    try {
      useMuKVstore = Boolean.valueOf(props
          .getProperty(USE_STORAGE_LAYER_PROPERTY));
    } catch (Exception e) {
      e.printStackTrace();
    }

    try {
      if (useMuKVstore) {
        kvs = MuKeyValueSystem.Factory.create();
        try {
          mukv = kvs.createStore(new AlluxioURI(storeUri));  
        } catch (FileAlreadyExistsException e) {
          try {
            mukv = kvs.openStore(new AlluxioURI(storeUri));
          } catch (Exception e2) {
            throw new DBException("could not connect to database at "
                + storeUri);
          }
        }
        
      } else {
        db1 = new BaseMarketDBReaderWriter(new AlluxioURI(storeUri));
      }

      // System.out.println("Initialized a client for thread "
      // + Thread.currentThread().getId());
    } catch (IOException e) {
      e.printStackTrace();
    } catch (AlluxioException e) {
      e.printStackTrace();
    }

  }

  @Override
  public void cleanup() throws DBException {
    
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.yahoo.ycsb.DB#start()
   */
  @Override
  public void start() throws DBException {

    super.start();
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.yahoo.ycsb.DB#commit()
   */
  @Override
  public void commit() throws DBException {
    // TODO Auto-generated method stub
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
    throw new DBException("Abort is not supported in MarketDB");
  }

  @Override
  public Status insert(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      // need to serialize values
      String svalue = stringify(values);
      // System.out.println(formatKey(table, key));
      // System.out.println(svalue);
      if (useMuKVstore) {
        mukv.put(ByteBuffer.wrap(formatKey(table, key).getBytes()),
            ByteBuffer.wrap(svalue.getBytes()));
      } else {
        db1.put(formatKey(table, key).getBytes(), svalue.getBytes());
      }

      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  private static String stringify(HashMap<String, ByteIterator> values) {
    return stringify(values, KV_DEL, ENTRY_DEL);
  }

  /**
   * @param values
   * @return
   */
  public static String stringify(HashMap<String, ByteIterator> values,
      final String kvDelimiter, final String eDelimiter) {
    StringBuffer sb = new StringBuffer();
    HashMap<String, String> svalues = StringByteIterator.getStringMap(values);

    final ArrayList<String> slist = new ArrayList<String>();

    svalues.forEach(new BiConsumer<String, String>() {

      @Override
      public void accept(String t, String u) {
        String mt = t.replace(',', ';');
        String mu = u.replace(',', ';');
        slist.add(mt + kvDelimiter + mu);
      }

    });

    for (int i = 0; i < slist.size(); i++) {
      if (i != 0) {
        sb.append(eDelimiter);
      }
      sb.append(slist.get(i));
    }
    return sb.toString();
  }

  private String formatKey(String table, String key) {
    return String.format("%s.%s", table, key);
  }

  @Override
  public Status delete(String table, String key) {
    try {
      // boolean res = db.remove(formatKey(table, key));
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
      String dbkey = formatKey(table, key);
      HashMap<String, String> values = new HashMap<String, String>();
      String svalue = null;
      if (useMuKVstore) {
        mukv.get(ByteBuffer.wrap(dbkey.getBytes()));
      } else {
        svalue = new String(db1.get(dbkey.getBytes()));
      }

      values = deStringfyValue(fields, svalue);

      StringByteIterator.putAllAsByteIterators(result, values);
      return Status.OK;

    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  /**
   * @param fields
   * @param values
   * @param svalue
   */
  public static HashMap<String, String> deStringfyValue(Set<String> fields,
      String svalue) {
    if (svalue == null) {
      return new HashMap<String, String>();
    }
    String[] svalues = svalue.split(ENTRY_DEL);
    HashMap<String, String> values = new HashMap<String, String>();

    for (int i = 0; i < svalues.length; i++) {
      String[] kv = svalues[i].split(KV_DEL);
      if (fields == null || fields.isEmpty()) {
        values.put(kv[0], kv[1]);
      } else {
        // add only the requested fields
        if (fields.contains(kv[0])) {
          values.put(kv[0], kv[1]);
        }

      }
    }

    return values;
  }

  @Override
  public Status update(String table, String key,
      HashMap<String, ByteIterator> values) {
    try {
      String svalue = stringify(values);
      if (useMuKVstore) {
        mukv.put(ByteBuffer.wrap(formatKey(table, key).getBytes()),
            ByteBuffer.wrap(svalue.getBytes()));
      } else {
        db1.put(formatKey(table, key).getBytes(), svalue.getBytes());
      }
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
    //
    // Iterator<Entry<String, String>> iter = db.iterator();
    //
    // while (iter.hasNext() && recordcount > 0){
    // recordcount--;
    // Entry<String, String> rec = iter.next();
    // String svalue = rec.getValue();
    // HashMap<String, ByteIterator> val =
    // StringByteIterator.getByteIteratorMap((deStringfyValue(fields, svalue)));
    // result.add(val);
    // }
    //
    return Status.OK;
  }

  @Override
  public Status execStoredProcedure(String spName, List<String> keys) {
    // TODO Auto-generated method stub
    return null;
  }
}
