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

public class MemcachedClientWithKeyStats extends MemcachedClient{
	
	final static int GET = 0;
	final static int SET = 1;
	
	HashMap<InetSocketAddress,Integer> keyCount = new HashMap<InetSocketAddress,Integer>();
	HashMap<InetSocketAddress,Integer> keyCount_read = new HashMap<InetSocketAddress,Integer>();
	
	public MemcachedClientWithKeyStats(ConnectionFactory cf, List<InetSocketAddress> addrs)
			throws IOException {
		super(cf, addrs);
		for (InetSocketAddress a : addrs){
			keyCount.put(a, 0);
		}
	}

	public MemcachedClientWithKeyStats(InetSocketAddress... ia) throws IOException {
		super(ia);
		for (InetSocketAddress a : ia){
			keyCount.put(a, 0);
		}
	}

	public MemcachedClientWithKeyStats(List<InetSocketAddress> addrs)
			throws IOException {
		super(addrs);
		for (InetSocketAddress a : addrs){
			keyCount.put(a, 0);
		}
	}
	
	private MemcachedNode getHashedServer(String key){
		return super.mconn.getLocator().getPrimary(key);
	}
	
	
	public void updateStats(int m, String key){
		if (m == SET){
			int count = keyCount.get((InetSocketAddress)this.getHashedServer(key).getSocketAddress());
			count++;
			keyCount.put((InetSocketAddress)this.getHashedServer(key).getSocketAddress(),count);
		}
		else if (m == GET){
			int count = keyCount_read.get((InetSocketAddress)this.getHashedServer(key).getSocketAddress());
			count++;
			keyCount_read.put((InetSocketAddress)this.getHashedServer(key).getSocketAddress(),count);
		}
		
	}
	
	public void printStats(){
		System.out.printf("Host \t\t keyCount \t\t keyCount_read \n");
		
		for (InetSocketAddress a : keyCount.keySet()){
			System.out.printf("%s \t\t %d \t\t %d\n",a.getHostName(),keyCount.get(a),keyCount_read.get(a));
		}
	}
	
	
}