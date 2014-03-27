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
	
	HashMap<InetSocketAddress,Integer> keyCount = new HashMap<InetSocketAddress,Integer>();
	
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
	
	
	public void updateStats(String key){
		int count = keyCount.get((InetSocketAddress)this.getHashedServer(key).getSocketAddress());
		count++;
		keyCount.put((InetSocketAddress)this.getHashedServer(key).getSocketAddress(),count);
	}
	
	public void printStats(){
		Collection<MemcachedNode> mns = super.mconn.getLocator().getAll();
		System.out.printf("Host \t\t keyCount \n");
		for (MemcachedNode mn : mns){
			InetSocketAddress a = ((InetSocketAddress) mn.getSocketAddress());				
			System.out.printf("%s \t\t %d \n",a.getHostName(),keyCount.get(a));
		}
	}
	
	
}