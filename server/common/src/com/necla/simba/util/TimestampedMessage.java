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

import java.nio.ByteBuffer;

/**
 * @author Dorian Perkins <dperkins@nec-labs.com>
 * @created Sep 24, 2013 5:17:54 PM
 */
public class TimestampedMessage {
	public ByteBuffer data;
	public long ts;
	public int seq;
	
	public TimestampedMessage(ByteBuffer data, int seq){
		this.data = data;
		this.ts = System.currentTimeMillis();
		this.seq = seq;
	}
}
