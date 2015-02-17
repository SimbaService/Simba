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
package com.necla.simba.client;

public class Preferences {

	public static int COMPRESS_HEURISTIC_SIZE = 0;
	public static int COMPRESS_HEURISTIC_ENTROPY = 1;
	public static boolean WRITE_COMPRESS = true;
	public static boolean READ_COMPRESS = true;
	public static int COMPRESS_LEVEL = 9; // Best Compression
	public static int COMPRESS_HEURISTIC = COMPRESS_HEURISTIC_SIZE; // pick one of size or entropy
	public static int COMPRESSABLE_SIZE = 100; // ensure 13 bytes notifications do not get get compressed
	public static final int READ_COMPRESS_BUFSIZE = 4096; // buffer to hold uncompressed data

	public static final int RECONNECT_TIMER = 10000; // check network every 10s
	
	public static final int DT_CONTROL_MSGS_PULL = 2000; // DT for control messages - passive pull
	public static final int DT_CONTROL_MSGS_ACK = 2000; // DT for control messages - notify ack

	public Preferences() {
	}

}
