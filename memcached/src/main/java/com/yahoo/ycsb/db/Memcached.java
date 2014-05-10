// Modified By: Thamir Qadah 2014
//
// Original:
// Jacob Leverich <leverich@stanford.edu>, 2011
// Memcached client for YCSB framework.
// 

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
import com.yahoo.ycsb.measurements.Measurements;

import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.AddrUtil;

public class Memcached extends com.yahoo.ycsb.DB
{
  MemcachedClientWithKeyStats client;
  Properties props;
  int fliedlength = 0;

  public static final int OK = 0;
  public static final int ERROR = -1;
  public static final int ERROR_IE = -11;
  public static final int ERROR_EE = -12;
  public static final int NOT_FOUND_V = -2; // Miss
  public static final int NOT_FOUND_K = -3;
  public static final int NOT_FOUND_SV = -4;
  
  final static String chars = "abcdefghijklmnopqrstuvwxyz";
	static String buildAString(int size){
		
		StringBuilder sb = new StringBuilder();
		for (int i=0;i< size;++i){
			int j = (int) Math.floor(Math.random()*chars.length());
			sb.append(chars.charAt(j));
		}
		return sb.toString();		
	}
	
	HashMap<String,Integer> DistinctkeyCount = new HashMap<String,Integer>();
    HashMap<String,HashMap<String,ArrayList<Integer>>> distinctKeyCountPerServer = new HashMap<String,HashMap<String,ArrayList<Integer>>>();
    
    HashMap<String,Integer> DistinctkeyCountR = new HashMap<String,Integer>();
    HashMap<String,HashMap<String,ArrayList<Integer>>> distinctKeyCountPerServerR = new HashMap<String,HashMap<String,ArrayList<Integer>>>();
	
	

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
    		    InetSocketAddress sa = new InetSocketAddress(ip,port);
    		    for (int i = 0; i < mul; i++){
    		    	mcservers.add(sa);	
    		    }    		    
    		    distinctKeyCountPerServer.put(sa.getHostName(), new HashMap<String,ArrayList<Integer>>());
    		    distinctKeyCountPerServerR.put(sa.getHostName(), new HashMap<String,ArrayList<Integer>>());
    	  }
    	  
//    	  for (InetSocketAddress s : mcservers){
//    		  System.out.printf("%s %d \n", s.getHostName(),s.getPort());   		   
//    	  }
    	  
      client = new MemcachedClientWithKeyStats(mcservers);
//      client.setMeasurements(Measurements.getMeasurements());  
      
      
    	  
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
	  System.out.printf("[INSERTKeyCount], Key Count Data\n");
	  System.out.printf("[INSERTKeyCount], Key, keyCount\n");
	  Iterator<String> it = DistinctkeyCount.keySet().iterator();
	  
	  while(it.hasNext()){
		  String k = it.next();
		  System.out.printf("[INSERTKeyCount], %s, %d\n",k,DistinctkeyCount.get(k));
	  }
	  
	  System.out.printf("[INSERTKeyCountServer], Per Server Key Count\n");
	  System.out.printf("[INSERTKeyCountServer], Host, key, keyCount\n");
	  it = distinctKeyCountPerServer.keySet().iterator();
	  
	  while (it.hasNext()){
		  String sname = it.next();
		  Iterator<String> it2 = distinctKeyCountPerServer.get(sname).keySet().iterator();
		  while (it2.hasNext()){
			  String k2 = it2.next();
			  
			  ArrayList<Integer> is = distinctKeyCountPerServer.get(sname).get(k2);
			  if (is != null){
				  System.out.printf("[INSERTKeyCountServer], %s,%s,",sname,k2);
				  for (Integer r : is){
					  System.out.printf("%d,",r);
				  }
				  System.out.println();
			  }
		  }
	  }
	  
	  System.out.printf("[READKeyCount], Key Count Data\n");
	  System.out.printf("[READKeyCount], Key, keyCount\n");
	   it = DistinctkeyCountR.keySet().iterator();
	  
	  while(it.hasNext()){
		  String k = it.next();
		  System.out.printf("[READKeyCount], %s, %d\n",k,DistinctkeyCountR.get(k));
	  }
	  
	  System.out.printf("[READKeyCountServer], Per Server Key Count\n");
	  System.out.printf("[READKeyCountServer], Host, key, keyCount\n");
	  it = distinctKeyCountPerServerR.keySet().iterator();
	  
	  while (it.hasNext()){
		  String sname = it.next();
		  Iterator<String> it2 = distinctKeyCountPerServerR.get(sname).keySet().iterator();
		  while (it2.hasNext()){
			  String k2 = it2.next();
			  ArrayList<Integer> is = distinctKeyCountPerServerR.get(sname).get(k2);
			  if (is != null){
				  System.out.printf("[READKeyCountServer], %s,%s,",sname,k2);
				  for (Integer r : is){
					  System.out.printf("%d,",r);
				  }
				  System.out.println();
			  }
		  }
	  }
	  
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
  @SuppressWarnings("unchecked")
public int read(String table, String key, Set<String> fields,
                  HashMap<String,ByteIterator> result) {
	  
	  long st=System.nanoTime();
    HashMap<String, byte[]> values = 
      (HashMap<String, byte[]>) client.get(table + ":" + key);
    long en=System.nanoTime();
    
    client.updateStats(client.GET, key);
    
	Measurements.getMeasurements().measure("Memcached:GET@"+client.getServerHostname(key), (int)((en-st)/1000));
    
    if (values == null){
    	Measurements.getMeasurements().reportReturnCode("Memcached:GET@"+client.getServerHostname(key), NOT_FOUND_V);
    	return countReadKey(key,NOT_FOUND_V);
    }
    if (values.keySet().isEmpty()){
    	
    	Measurements.getMeasurements().reportReturnCode("Memcached:GET@"+client.getServerHostname(key), NOT_FOUND_K);
    	return countReadKey(key,NOT_FOUND_K);
    }
    if (fields == null) fields = values.keySet();

    for (String k: fields) {
      byte[] v = values.get(k);
      if (v == null){
    	  Measurements.getMeasurements().reportReturnCode("Memcached:GET@"+client.getServerHostname(key), NOT_FOUND_SV);
    	  return countReadKey(key,NOT_FOUND_SV);
      }
      result.put(k, new ByteArrayByteIterator(v));
    }
    
    Measurements.getMeasurements().reportReturnCode("Memcached:GET@"+client.getServerHostname(key), OK);    
    return countReadKey(key,OK);
  }

/**
 * @param key
 */
private int countReadKey(String key,int status) {
	Integer i = DistinctkeyCountR.get(key);
	if (i == null){
		DistinctkeyCountR.put(key, 1);
	}
	else{
		DistinctkeyCountR.put(key, i+1);
	}
	
	HashMap<String,ArrayList<Integer>> temp = distinctKeyCountPerServerR.get(client.getServerHostname(key));
	
	if (temp.get(key) == null){
		ArrayList<Integer> retcodes = new ArrayList<Integer>();
		retcodes.add(status);
		temp.put(key, retcodes);
	}
	else{
		temp.get(key).add(status);
	}
	return status;
}

private int countInsertKey(String key, int status) {
	Integer i = DistinctkeyCount.get(key);
	if (i == null){
		DistinctkeyCount.put(key,1);
	}
	else{
		DistinctkeyCount.put(key, i+1);
	}
	
	HashMap<String,ArrayList<Integer>> temp = distinctKeyCountPerServer.get(client.getServerHostname(key));
	
	if (temp.get(key) == null){
		ArrayList<Integer> retcodes = new ArrayList<Integer>();
		retcodes.add(status);
		temp.put(key, retcodes);
	}
	else{
		temp.get(key).add(status);
	}
	return status;
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
	  
    }
    
//    Measurements.getMeasurements().measure("DataSize", sum);
//    Measurements.getMeasurements().measure("KeySize", key.getBytes().length);
    OperationFuture<Boolean> f =
      client.set(table + ":" + key, 3600, new_values);

    try { 
  	  long st=System.nanoTime();
    	int res = f.get() ? OK : ERROR;
  	  long en=System.nanoTime();
  	  client.updateStats(client.SET, key);	
    	Measurements.getMeasurements().measure("Memcached:SET@"+client.getServerHostname(key), (int)((en-st)/1000));
    	Measurements.getMeasurements().reportReturnCode("Memcached:SET@"+client.getServerHostname(key), res);
    	countInsertKey(key,res);
    	    	
    	return res; }
    catch (InterruptedException e) {
    	Measurements.getMeasurements().reportReturnCode("Memcached:SET@"+client.getServerHostname(key), ERROR_IE);
    	return ERROR_IE; }
    catch (ExecutionException e) {
    	Measurements.getMeasurements().reportReturnCode("Memcached:SET@"+client.getServerHostname(key), ERROR_EE);
    	return ERROR_EE; }
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
