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
package com.necla.simba.util;

import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

public class ConsistentHash {

	private List<String> servers;
	static final int SEED = 373588321; // always use the same seed value

	public ConsistentHash() {
		//quick hack for just using hash() only
	}

	/*
	 * Initializes the list of server options
	 */
	public ConsistentHash(List<String> nodes) {
		servers = Lists.newArrayList(nodes);
	}

	public int hash(String token, int size) {
		return Hashing.consistentHash(
				Hashing.murmur3_128(SEED).hashString(token), size);
	}

	public int hash(int token, int size) {
		return Hashing.consistentHash(Hashing.murmur3_128(SEED).hashInt(token),
				size);
	}

	/*
	 * Hashes the String 'id' to determine an index. Returns the item at that
	 * index in the List 'servers'.
	 */
	public String getNode(String id) {
		int bucket = Hashing.consistentHash(Hashing.murmur3_128(SEED)
				.hashString(id), servers.size());
		return servers.get(bucket);
	}

	public boolean addNode(String ip) {
		return servers.add(ip);
	}

	public boolean removeNode(String ip) {
		return servers.remove(ip);
	}

	public List<String> listNodes() {
		return servers;
	}
}
