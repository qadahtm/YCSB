// Jacob Leverich <leverich@stanford.edu>, 2011
// Memcached client for YCSB framework.
//
// Properties:
//   memcached.server=memcached.xyz.com
//   memcached.port=11211

package com.yahoo.ycsb.db;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.codehaus.jackson.*;
import org.codehaus.jackson.map.ObjectMapper;

import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.StringByteIterator;

import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.AddrUtil;

public class Memcached extends com.yahoo.ycsb.DB
{
  MemcachedClientWithKeyStats client;
  Properties props;
  int fliedlength = 0;

  public static final int OK = 0;
  public static final int ERROR = -1;
  public static final int NOT_FOUND = -2;
  
  final static String chars = "abcdefghijklmnopqrstuvwxyz";
	static String buildAString(int size){
		
		StringBuilder sb = new StringBuilder();
		for (int i=0;i< size;++i){
			int j = (int) Math.floor(Math.random()*chars.length());
			sb.append(chars.charAt(j));
		}
		return sb.toString();		
	}
	
	

  /**
   * Initialize any state for this DB.  Called once per DB instance;
   * there is one DB instance per client thread.
   */
  public void init() throws DBException {
	  
	  // Loading properties from the file
	props = getProperties();
	
	// getting the server list in JSON format
    String serversList = props.getProperty("serverslist");
    this.fliedlength = Integer.parseInt(props.getProperty("fieldlength")); 
    
    if (serversList == null)
      throw new DBException("serverslist param must be specified in JSON format");

  
    try {
    	
    	  ObjectMapper mapper = new ObjectMapper();
    	  
    	  JsonParser jp = mapper.getJsonFactory().createJsonParser(serversList);
    	  JsonNode root = mapper.readTree(jp);
    	  
//    	  System.out.println(root.toString());
    	  ArrayList<InetSocketAddress> mcservers = new ArrayList<InetSocketAddress>();
    	  
    	  // Create server entries according to multiplier value. 
    	  for (JsonNode server : root.path("servers")) {
    		    String ip = server.get("ip").asText();
    		    int port = server.get("port").asInt();
    		    int mul = server.get("multiplier").asInt();
    		    
    		    for (int i = 0; i < mul; i++){
    		    	mcservers.add(new InetSocketAddress(ip,port));	
    		    }    		    
    	  }
    	  
    	  for (InetSocketAddress s : mcservers){
    		  System.out.printf("%s %d \n", s.getHostName(),s.getPort());   		   
    	  }
    	  
    	  
      client = new MemcachedClientWithKeyStats(mcservers);
    	  
    	  
    } catch (IOException e) { 
    	throw new DBException(e); 
    
    }

//	  try {
//	      client = new MemcachedClient(AddrUtil.getAddresses("128.46.122.52:11211 128.46.122.53:11211 128.46.122.153:11211"));
//	    } catch (IOException e) { throw new DBException(e); }

    }

  /**
   * Cleanup any state for this DB.  Called once per DB instance;
   * there is one DB instance per client thread.
   */
  public void cleanup() throws DBException
  {
	  client.printStats();
   client.shutdown();
  }

  /**
   * Read a record from the database. Each field/value pair from the
   * result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return Zero on success, a non-zero error code on error or "not found".
   */
  public int read(String table, String key, Set<String> fields,
                  HashMap<String,ByteIterator> result) {
    HashMap<String, byte[]> values = 
      (HashMap<String, byte[]>) client.get(table + ":" + key);
    
    client.updateStats(client.GET, key);	
    
    if (values == null) return NOT_FOUND;
    if (values.keySet().isEmpty()) return NOT_FOUND;
    if (fields == null) fields = values.keySet();

    for (String k: fields) {
      byte[] v = values.get(k);
      if (v == null) return NOT_FOUND;
      result.put(k, new ByteArrayByteIterator(v));
    }

    return OK;
  }

  /**
   * Perform a range scan for a set of records in the database. Each
   * field/value pair from the result will be stored in a HashMap.
   *
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set
   * field/value pairs for one record
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  public int scan(String table, String startkey, int recordcount,
                  Set<String> fields,
                  Vector<HashMap<String,ByteIterator>> result) {
    return ERROR;
  }
	
  /**
   * Update a record in the database. Any field/value pairs in the
   * specified values HashMap will be written into the record with the
   * specified record key, overwriting any existing values with the
   * same field name.
   *
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  public int update(String table, String key,
                    HashMap<String,ByteIterator> values) {
    HashMap<String, byte[]> new_values = new HashMap<String, byte[]>();
   
    for (String k: values.keySet()) {
      	
      byte[] vb = values.get(k).toArray();
      new_values.put(k, vb);
	  //sum += vb.length;
	  //System.out.println(k+":-"+vb+"- , "+vb.length);
      
    }
    
    OperationFuture<Boolean> f =
      client.set(table + ":" + key, 3600, new_values);

    try { 
    	int res = f.get() ? OK : ERROR;
    	client.updateStats(client.SET, key);		
    	return res; }
    catch (InterruptedException e) { return ERROR; }
    catch (ExecutionException e) { return ERROR; }
  }

  /**
   * Insert a record in the database. Any field/value pairs in the
   * specified values HashMap will be written into the record with the
   * specified record key.
   *
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the
   * record
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  public int insert(String table, String key,
                    HashMap<String,ByteIterator> values) {
    return update(table, key, values);
  }

  /**
   * Delete a record from the database.
   *
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return Zero on success, a non-zero error code on error.  See
   * this class's description for a discussion of error codes.
   */
  public int delete(String table, String key) {
    client.delete(table + ":" + key);
    return OK; // FIXME check future
  }
}
