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
package com.necla.simba.server.simbastore.table;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.PriorityQueue;

import com.necla.simba.protocol.Common.ObjectFragment;

/**
 * The SimbaObject will store object fragments of an object and concatenate them together on-demand into a byte array.
 * It uses a priority queue to sort object fragments in ascending order by their offset value.
 */
public class SimbaObject {
	PriorityQueue<ObjectFragment> fragments;
	boolean complete;
	int size = 0;
	
	public SimbaObject(){
		ObjectFragmentComparator comparator = new ObjectFragmentComparator();
		fragments = new PriorityQueue<ObjectFragment>(1, comparator);
		//since the common case might be to have 1 fragment, let's start with a smaller memory footprint
	}
	
	public void add(ObjectFragment fragment){
		if(fragment.getEof() == true){
			complete = true;
		}
		
		size += fragment.getData().size();
		
		fragments.add(fragment);
	}
	
	public byte[] toByteArray(){
		ByteBuffer b = ByteBuffer.allocate(size);
		if(complete == true){
			for(ObjectFragment f : fragments){
				b.put(f.getData().asReadOnlyByteBuffer());
			}
		} else {
			return null;
		}
		b.flip();
		return b.array();
	}
	
	public class ObjectFragmentComparator implements Comparator<ObjectFragment>{

		@Override
		public int compare(ObjectFragment x, ObjectFragment y) {
			if(x.getOffset() < y.getOffset())
				return -1;
			else 
				return 1;
		}
		
	}
}
