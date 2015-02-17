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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import com.necla.simba.server.simbastore.cache.ObjectChanges;

public class ObjectChanges {
	public Map<Integer, UUID> chunks;
	public String name;
	public Integer size;

	public ObjectChanges(String name){
		this.name = name;
		chunks = new HashMap<Integer, UUID>();
		size = null;
	}
	
	public void addChunk(int offset, UUID key){
		chunks.put(offset, key);
	}

	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		
		sb.append("\t\tname = ")
		.append(name)
		.append("\n")
		.append("\t\tchunks = {\n");
		
		Iterator<Entry<Integer, UUID>> it = chunks.entrySet().iterator();
		while(it.hasNext()){
			Entry<Integer, UUID> next = it.next();
			sb.append("\t\t[")
			.append(next.getKey())
			.append(",")
			.append(next.getValue())
			.append("]\n");
		}
		sb.append("\t\t}\n");
		
		return sb.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ObjectChanges other = (ObjectChanges) obj;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		return true;
	}
	
	
}
