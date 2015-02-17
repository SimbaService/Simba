/*******************************************************************************
 *    Copyright 2015 Dorian Perkins, Younghwan Go, Nitin Agrawal, Akshat Aranya
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
package com.necla.simba.client;

import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/***
 * Utility class that maintains the client authentication information.
 * 
 * @file AuthenticationManager.java
 * @author shao@nec-labs.com
 * @created 5:21:39 PM, Jul 23, 2012
 * @modified 5:21:39 PM, Jul 23, 2012
 */
public class AppAuthenticationManager {
	private static ConcurrentHashMap<String, String> map = new ConcurrentHashMap<String, String>();
	
	private static Random r = new Random(System.currentTimeMillis());

	public static String register(String uid) {
		/* TODO: plug in authentication module here */
		String tid = new Long(r.nextLong()).toString();
		map.put(tid, uid);
		return tid;
	}

	public static String authenticate(String tid) {
		String ret = null;
		if (map.containsKey(tid)) {
			ret = map.get(tid);
		}

		return ret;
	}

	public static void unregister(String tid) {
		if (map.containsKey(tid)) {
			map.remove(tid);
		}
	}
	
	public static void unregisterByUID(String uid) {
		for (Entry<String, String> s: map.entrySet()) {
			if (s.getValue().equals(uid)) {
				map.remove(s.getKey());
				break;
			}
		}
	}
}
