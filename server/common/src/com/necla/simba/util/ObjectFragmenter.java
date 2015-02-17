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
/**
 * 
 */
package com.necla.simba.util;

import java.util.LinkedList;

import com.google.protobuf.ByteString;
import com.necla.simba.protocol.Common.ObjectFragment;
import com.necla.simba.server.Preferences;

public class ObjectFragmenter {

	public static LinkedList<ObjectFragment> fragment(int obj_id, int trans_id, byte[] object){
		if (object != null){
		
			LinkedList<ObjectFragment> fragments = new LinkedList<ObjectFragment>();
			
			int num_chunks = (int) Math.ceil((object.length/(double)Preferences.MAX_FRAGMENT_SIZE));
			
			
			for(int i = 0; i < num_chunks; i ++){
				
				ObjectFragment f = ObjectFragment.newBuilder()
						.setData(ByteString.copyFrom(object, (i*Preferences.MAX_FRAGMENT_SIZE), 
								Math.min(
										object.length - (i*Preferences.MAX_FRAGMENT_SIZE), 
										Preferences.MAX_FRAGMENT_SIZE)
										)
								)
						.setTransId(trans_id)
						.setOid(obj_id)
						.setOffset(i)
						.setEof((i == num_chunks - 1)? true : false)
						.build();
				
				fragments.add(f);
			}
		
			return fragments;
		
		} else { 
			return null;
		}
	}
}
