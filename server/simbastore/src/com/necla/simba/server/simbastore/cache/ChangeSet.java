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

import java.util.LinkedList;
import java.util.List;

import com.necla.simba.server.simbastore.cache.ObjectChanges;

public class ChangeSet {
	// boolean table_changed;
	public int version;
	public List<String> table_changes;
	public List<ObjectChanges> object_changes;
	public boolean deleted;

	public ChangeSet() {
		// table_changed = false;
		table_changes = new LinkedList<String>();
		object_changes = new LinkedList<ObjectChanges>();
	}
	
	@Override
	public String toString(){
		StringBuilder sb = new StringBuilder();
		
		sb.append("{\n")
		.append("\tversion = ")
		.append(version)
		.append("\n\ttable_changes = {");
		
		for(String s : table_changes){
			sb.append(s)
			.append(",");
		}
		
		sb.append("\n\t}\n")
		.append("\tobject_changes = {");
		
		for(ObjectChanges oc : object_changes){
			sb.append("\t" + oc.toString());
		}
		sb.append("\t}\n}");
		
		return sb.toString();
	}
}
