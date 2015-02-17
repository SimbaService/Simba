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

import java.nio.ByteBuffer;
import java.util.BitSet;

import com.necla.simba.protocol.SyncHeader;
import com.necla.simba.protocol.SyncResponse;

/**
 * 
 * Random crap of shared static functions
 * 
 * @author aranya
 * 
 */
public class Utils {

	// simpler, less verbose methods for logging some stuff
	public static String stringify(SyncHeader sh) {
		StringBuffer sb = new StringBuffer();
		sb.append(SyncHeader.class.getCanonicalName());
		sb.append("(app = ").append(sh.getApp()).append(" tbl = ")
				.append(sh.getTbl()).append(" dirtyRows.size = ")
				.append(sh.getDirtyRows().size())
				.append(" deletedRows.size = ")
				.append(sh.getDeletedRows().size()).append(")");

		return sb.toString();
	}

	public static String stringify(SyncResponse sr) {
		StringBuffer sb = new StringBuffer();
		sb.append(SyncResponse.class.getCanonicalName());
		sb.append("(result = ").append(sr.getResult())
				.append(" syncedRows.size = ").append(sr.getSyncedRows().size())
				.append(" conflictedRows.size = ")
				.append(sr.getConflictedRows().size()).append(")");

		return sb.toString();
	}

	public static byte[] longToBytes(long x) {
		ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.putLong(x);
		return buffer.array();
	}

	public static long bytesToLong(byte[] bytes) {
		ByteBuffer buffer = ByteBuffer.allocate(8);
		buffer.put(bytes);
		buffer.flip();
		return buffer.getLong();
	}

	public static byte[] bitsToByteArray(BitSet bits) {
		byte[] bytes = new byte[(bits.length() + 7) / 8];
		for (int i = 0; i < bits.length(); i++) {
			if (bits.get(i)) {
				bytes[bytes.length - i / 8 - 1] |= 1 << (i % 8);
			}
		}
		return bytes;
	}

	// Returns a bitset containing the values in bytes.
	public static BitSet fromByteArray(byte[] bytes) {
		BitSet bits = new BitSet();
		for (int i = 0; i < bytes.length * 8; i++) {
			if ((bytes[bytes.length - i / 8 - 1] & (1 << (i % 8))) > 0) {
				bits.set(i);
			}
		}
		return bits;
	}
}
