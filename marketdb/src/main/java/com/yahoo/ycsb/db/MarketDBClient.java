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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.function.BiConsumer;
import java.util.logging.Logger;

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
public class MarketDBClient extends DB implements com.yahoo.ycsb.StoredProcedureExecutor {

  private static final String CONF_FILE_PROPERTY = "ignite.conffile";
  private static final String CACHE_NAME_PROPERTY = "ignite.cachename";

  private static final String KV_DEL = ":";
  private static final String ENTRY_DEL = ",";

  private final Logger log = Logger.getLogger(getClass().getName());

  @Override
  public void init() throws DBException {
    Properties props = getProperties();

    String confFile = props.getProperty(CONF_FILE_PROPERTY);
    String cacheName = props.getProperty(CACHE_NAME_PROPERTY);

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
//      db.put(formatKey(table, key), svalue);
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
        slist.add(t + kvDelimiter + u);
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
//      boolean res = db.remove(formatKey(table, key));
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
//      String svalue = db.get(dbkey);
//      values = deStringfyValue(fields, svalue);

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
//      db.put(formatKey(table, key), svalue);
      return Status.OK;
    } catch (Exception e) {
      e.printStackTrace();
    }
    return Status.ERROR;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    // this will ignore the startkey and reads the number of records specified by recordcount
//    
//    Iterator<Entry<String, String>> iter = db.iterator();
//    
//    while (iter.hasNext() && recordcount > 0){
//      recordcount--;
//      Entry<String, String> rec = iter.next();
//      String svalue = rec.getValue();
//      HashMap<String, ByteIterator> val = StringByteIterator.getByteIteratorMap((deStringfyValue(fields, svalue)));
//      result.add(val);
//    }
//    
    return Status.OK;
  }

  @Override
  public Status execStoredProcedure(String spName, List<String> keys) {
    // TODO Auto-generated method stub
    return null;
  }
}
