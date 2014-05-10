// Thamir Qadah, 2014
// Extended Memcached client for YCSB framework and spymemcached.
//
// Properties:
//   memcached.server=memcached.xyz.com
//   memcached.port=11211

package com.yahoo.ycsb.db;


import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.MemcachedNode;

import com.yahoo.ycsb.measurements.Measurements;


public class XMemcachedClientWithKeyStats extends MemcachedClient{
	
	final static int GET = 0;
	final static int SET = 1;
	
	private boolean ycsbm = false;
	
	HashMap<InetSocketAddress,Integer> keyCount = new HashMap<InetSocketAddress,Integer>();
	HashMap<InetSocketAddress,Integer> keyCount_read = new HashMap<InetSocketAddress,Integer>();
	
	public XMemcachedClientWithKeyStats(ConnectionFactory cf, List<InetSocketAddress> addrs)
			throws IOException {
		super(cf, addrs);
		for (InetSocketAddress a : addrs){
			keyCount.put(a, 0);
			keyCount_read.put(a,0);
		}
	}

	public XMemcachedClientWithKeyStats(InetSocketAddress... ia) throws IOException {
		super(ia);
		for (InetSocketAddress a : ia){
			keyCount.put(a, 0);
			keyCount_read.put(a,0);
		}
	}

	public XMemcachedClientWithKeyStats(List<InetSocketAddress> addrs)
			throws IOException {
		super(addrs);
		for (InetSocketAddress a : addrs){
			keyCount.put(a, 0);
			keyCount_read.put(a,0);
		}
	}
	
	private MemcachedNode getHashedServer(String key){
		return super.mconn.getLocator().getPrimary(key);
	}
	
	public String getServerHostname(String key){
		InetSocketAddress iadd = (InetSocketAddress)this.getHashedServer(key).getSocketAddress();
		return iadd.getHostName();
	}
	
	public void updateStats(int m, String key){
		InetSocketAddress iadd = (InetSocketAddress)this.getHashedServer(key).getSocketAddress();
		
		if (this.ycsbm){
			if (m == SET){
				int count = keyCount.get(iadd);
				count++;
				keyCount.put(iadd,count);
				Measurements.getMeasurements().measure("Memcached:SET@"+iadd.getHostName(), count);
			}
			else if (m == GET){
				int count = keyCount_read.get(iadd);
				count++;
				keyCount_read.put(iadd,count);
				Measurements.getMeasurements().measure("Memcached:GET@"+iadd.getHostName(), count);
			}
			
		}
		else{
			if (m == SET){
				int count = keyCount.get(iadd);
				count++;
				keyCount.put(iadd,count);
			}
			else if (m == GET){
				int count = keyCount_read.get(iadd);
				count++;
				keyCount_read.put(iadd,count);
			}			
		}
		
		
	}
	
	public void setMeasurements(boolean m){
		this.ycsbm = m;
	}
	
	public void printStats(){
		if (!this.ycsbm){
			System.out.printf("Host, keyCount, keyCount_read \n");

			for (InetSocketAddress a : keyCount.keySet()){
				System.out.printf("%s, %d , %d\n",a.getHostName(),keyCount.get(a),keyCount_read.get(a));
			}
		}
		
	}
	
	
}