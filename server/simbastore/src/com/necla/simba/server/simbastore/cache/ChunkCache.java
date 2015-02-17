/*******************************************************************************
 *   Copyright 2015 Dorian Perkins, Younghwan Go, Nitin Agrawal, Akshat Aranya
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *******************************************************************************/
package com.necla.simba.server.simbastore.cache;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.necla.simba.server.Preferences;
import com.necla.simba.server.simbastore.cache.ChunkCache;

import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkCache {
	private static final Logger LOG = LoggerFactory.getLogger(ChunkCache.class);

	private Cache<String, byte[]> cache;

	public ChunkCache(int cacheSizeInMB) {

		long cacheMem = cacheSizeInMB * 1024 * 1024;
		cache = CacheBuilder.newBuilder()
				.maximumSize(cacheMem / Preferences.MAX_FRAGMENT_SIZE)
				.concurrencyLevel(16)
				.recordStats()
				// .maximumWeight(cacheMem)
				// .weigher(new Weigher<String, byte[]>() {
				// public int weigh(String key, byte[] value) {
				// return value.length;
				// }
				// })
				.build();
		
		// flush the print buffer every N seconds (N = 30)
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new TimerTask() {
			public void run() {
				StringBuilder sb = new StringBuilder();
				sb.append("CHUNK_CACHE:");
				sb.append("\nhits=" + cache.stats().hitCount());
				sb.append("\nmisses=" + cache.stats().missCount());
				sb.append("\nload=" + cache.stats().loadCount());
				sb.append("\nevictions=" + cache.stats().evictionCount());
				sb.append("\nrequests=" + cache.stats().requestCount());
				System.out.println(sb.toString());
			}
		}, 0, 30000);

	}

	public byte[] get(String key, Callable<byte[]> onMiss) throws IOException {

		try {
			return cache.get(key, onMiss);
		} catch (ExecutionException e) {
			throw new IOException(e.getMessage());
		}

	}

	public void put(String key, byte[] value) {
		LOG.debug("Add " + key + " to cache: " + value.length + " bytes");
		cache.put(key, value);
	}

	public void invalidate(String key) {
		cache.invalidate(key);
	}

}
