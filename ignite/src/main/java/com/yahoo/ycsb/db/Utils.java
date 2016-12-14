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
import java.util.Set;
import java.util.function.BiConsumer;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Utility class.
 * @author qadahtm
 */
public final class Utils {
  
  private static final String KV_DEL = ":";
  private static final String ENTRY_DEL = ",";

  private Utils() {}

  public static String stringify(HashMap<String, ByteIterator> values) {
    return stringify(values, KV_DEL, ENTRY_DEL);
  }

  /**
   * Converts value hashmap into a single string.
   * @param values hash map of values
   * @param kvDelimiter string used between kv pairs in values
   * @param eDelimiter a string used between entries 
   * @return a string representation of values
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

  public static String formatKey(String table, String key) {
    return String.format("%s.%s", table, key);
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
}
