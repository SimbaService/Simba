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

import static org.fusesource.leveldbjni.JniDBFactory.asString;
import static org.fusesource.leveldbjni.JniDBFactory.bytes;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;

import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBFactory;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;
import org.iq80.leveldb.ReadOptions;

import android.os.Environment;
import android.util.Log;

public class SimbaLevelDB {
	private final static String TAG = "SimbaLevelDB";
	DBFactory factory = JniDBFactory.factory;

	static {
		System.loadLibrary("leveldb");
	}

	private final static int CHUNKSIZE = SharedConstants.CHUNKSIZE; 

	static DB db;
	private Options options;
	private static long max_id;

	public SimbaLevelDB() {
		options = new Options().createIfMissing(true);
		factory = JniDBFactory.factory;

		try {
			db = factory.open(
					new File(Environment.getExternalStorageDirectory()
							+ "/SimbaLevelDB"), options);
		} catch (IOException e) {
			Log.v(TAG, e.getMessage());
		}

		// check max obj_id
		max_id = 1;
		DBIterator iterator = db.iterator();
		try {
			iterator.seekToLast();
			if (iterator.hasNext()) {
				String keys[] = asString(iterator.peekNext().getKey()).split(
						",");
				max_id = Long.valueOf(keys[0]) + 1;
			} else {
				max_id = 1;
			}
			iterator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		Log.d(TAG, "Setting MAX_OBJ_ID to " + max_id);
	}

	public static int getChunkSize() {
		return CHUNKSIZE;
	}

	public static DB getSimbaLevelDB() {
		return db;
	}

	public static long getMaxObjID() {
		return max_id;
	}

	public static void setMaxObjID(long obj_id) {
		max_id = obj_id;
	}

	public static ReadOptions takeSnapshot() {
		return (new ReadOptions().snapshot(db.getSnapshot()));
	}

	public static void closeSnapshot(ReadOptions ro) {
		try {
			ro.snapshot().close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static int getNumChunks(long obj_id) {
		int num = 0;
		DBIterator iterator = db.iterator();
		// TODO: chunk for obj_id may not start with 0!
		String key = Long.toString(obj_id) + ",0";
		try {
			for (iterator.seek(bytes(key)); iterator.hasNext(); iterator.next()) {
				if (!(asString(iterator.peekNext().getKey()).startsWith(Long
						.toString(obj_id) + ",")))
					break;
				num++;
			}
			iterator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return num;
	}

	public static void setObjectDirty(long obj_id) {
		int chunk_num = 0;
		DBIterator iterator = db.iterator();
		String key = Long.toString(obj_id) + "," + Integer.toString(chunk_num);
		try {
			for (iterator.seek(bytes(key)); iterator.hasNext(); iterator.next()) {
				if (!(asString(iterator.peekNext().getKey()).startsWith(Long
						.toString(obj_id) + ",")))
					break;

				SimbaChunkList.addDirtyChunk(obj_id, chunk_num);
				chunk_num++;
			}
			iterator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static int truncate(long obj_id, int length, boolean makeDirty) {
		int chunk_num = length / CHUNKSIZE;
		int off = length % CHUNKSIZE;
		int index = 0;

		DBIterator iterator = db.iterator();
		String key = Long.toString(obj_id) + ",0";
		try {
			for (iterator.seek(bytes(key)); iterator.hasNext(); iterator.next()) {
				if (!(asString(iterator.peekNext().getKey()).startsWith(Long
						.toString(obj_id) + ",")))
					break;
				if (index == chunk_num) {
					if (makeDirty) {
						// mark last chunk as dirty
						SimbaChunkList.addDirtyChunk(obj_id, index);
					}

					// if part of chunk is deleted, mark this chunk as dirty
					byte[] buf = db.get(iterator.peekNext().getKey());
					if (buf.length > off) {
						Log.d(TAG, "RESIZE <"
								+ asString(iterator.peekNext().getKey())
								+ "> from " + buf.length + " -> " + off);
						byte[] new_buf = new byte[off];
						System.arraycopy(buf, 0, new_buf, 0, off);
						db.put(iterator.peekNext().getKey(), new_buf);
					}
				}
				// delete all chunks beyond the chunk_num
				else if (index > chunk_num) {
					// remove from dirty chunk list if it was set as dirty
					SimbaChunkList.removeDirtyChunk(obj_id, index);

					// delete chunk
					Log.d(TAG, "DELETE <"
							+ asString(iterator.peekNext().getKey()) + ">");
					db.delete(iterator.peekNext().getKey());
				}
				index++;
			}
			iterator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return length;
	}

	public static int write(long obj_id, int chunk_num, byte[] buffer,
			int length) {
		// add dirty chunk number to SimbaChunkList
		BitSet mcl = SimbaChunkList.getDirtyChunks(obj_id);
		if (mcl == null) {
			SimbaChunkList.addDirtyChunk(obj_id, chunk_num);
		} else {
			if (mcl.get(chunk_num) == false) {
				SimbaChunkList.addDirtyChunk(obj_id, chunk_num);
			}
		}

		// put object to leveldb
		String key = Long.toString(obj_id) + "," + Integer.toString(chunk_num);
		Log.d(TAG, "PUT <" + key + ">, buffer[" + buffer.length + "]");
		// Log.d(TAG, "PUT <" + key + ">, buffer[" + buffer.length + "] = "
		// + asString(buffer));
		db.put(bytes(key), buffer);

		return length;
	}

	public int read(long obj_id, byte[] buffer, int offset) {
		String key = null;
		byte[] buf = new byte[CHUNKSIZE];
		int chunk_num = offset / CHUNKSIZE;
		offset -= chunk_num * CHUNKSIZE;
		int len = 0;
		boolean hasData = false;

		DBIterator iterator = db.iterator();
		key = Long.toString(obj_id) + "," + Integer.toString(chunk_num);
		try {
			for (iterator.seek(bytes(key)); iterator.hasNext(); iterator.next()) {
				if (!(asString(iterator.peekNext().getKey()).startsWith(Long
						.toString(obj_id) + ",")))
					break;

				hasData = true;
				buf = db.get(iterator.peekNext().getKey());

				if (buffer.length - len < buf.length - offset) {
					System.arraycopy(buf, offset, buffer, len, buffer.length
							- len);
					len += buffer.length - len;
				} else {
					System.arraycopy(buf, offset, buffer, len, buf.length
							- offset);
					len += buf.length - offset;
				}
				Log.d(TAG, "GET <" + asString(iterator.peekNext().getKey())
						+ ">, buffer[" + buf.length + "]");
				// Log.d(TAG, "GET <" + asString(iterator.peekNext().getKey())
				// + ">, buffer[" + buf.length + "] = " + asString(buf));
				if (len >= buffer.length || buf.length < CHUNKSIZE)
					break;

				offset = 0;
			}
			iterator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (hasData) {
			return len;
		} else {
			return -1;
		}
	}

	public static byte[] getChunk(ReadOptions ro, long obj_id, int chunk_num) {
		String key = Long.toString(obj_id) + "," + Integer.toString(chunk_num);
		byte[] buf = db.get(bytes(key), ro);
		Log.d(TAG, "GET <" + key + ">, buffer[" + buf.length + "]");

		return buf;
	}

	public static void updateObject(long fromObj, long toObj) {
		BitSet dirtyChunks = SimbaChunkList.getDirtyChunks(fromObj);
		for (int i = dirtyChunks.nextSetBit(0); i >= 0;) {
			int chunk_num = i;
			i = dirtyChunks.nextSetBit(i + 1);

			// read chunk received from server
			String fromKey = Long.toString(fromObj) + ","
					+ Integer.toString(chunk_num);
			byte[] chunkBuffer = db.get(bytes(fromKey));

			// write chunk to object id stored in local table
			String toKey = Long.toString(toObj) + ","
					+ Integer.toString(chunk_num);
			db.put(bytes(toKey), chunkBuffer);

			Log.d(TAG, "UPDATE <" + fromKey + "> -> <" + toKey + ">");

			// delete received chunk from leveldb
			Log.d(TAG, "DELETE <" + fromKey + ">");
			db.delete(bytes(fromKey));
		}

		// delete object from dirtyChunkList
		SimbaChunkList.removeDirtyChunks(fromObj);
	}

	/* YGO: copy data from object data while truncating to length */
	public static int truncateStrong(long fromObj, long toObj, int length) {
		Log.d(TAG, "truncate for strong consistency from obj: " + fromObj
				+ " to obj: " + toObj + " with length: " + length);
		String key = Long.toString(fromObj) + ",0";
		DBIterator iterator = db.iterator();
		int chunk_num = length / CHUNKSIZE;
		int off = length % CHUNKSIZE;
		int index = 0;

		try {
			for (iterator.seek(bytes(key)); iterator.hasNext(); iterator.next()) {
				// read fromObj's chunk
				byte[] buf = db.get(iterator.peekNext().getKey());
				String toKey = Long.toString(toObj) + ","
						+ Integer.toString(index);
				if (chunk_num == index) {
					if (buf.length > off) {
						Log.d(TAG, "RESIZE <" + toKey + "> from " + buf.length
								+ " -> " + off);
						byte[] new_buf = new byte[off];
						System.arraycopy(buf, 0, new_buf, 0, off);

						// write to toObj's chunk
						Log.d(TAG, "PUT <" + toKey + ">, buffer["
								+ new_buf.length + "]");
						db.put(bytes(toKey), new_buf);
						SimbaChunkList.addDirtyChunk(toObj, index);
					}
					break;
				} else {
					// write to toObj's chunk
					Log.d(TAG, "PUT <" + toKey + ">, buffer[" + buf.length
							+ "]");
					db.put(bytes(toKey), buf);
					SimbaChunkList.addDirtyChunk(toObj, index);

					index++;
				}
			}
			iterator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return length;
	}

	public static void deleteDirtyObject(long obj_id) {
		BitSet dirtyChunks = SimbaChunkList.getDirtyChunks(obj_id);
		if (dirtyChunks != null) {
			for (int i = 0; i < dirtyChunks.size(); i++) {
				if (dirtyChunks.get(i) == false) {
					continue;
				}
				int chunk_num = i;
				String key = Long.toString(obj_id) + ","
						+ Integer.toString(chunk_num);

				// delete chunk from leveldb
				Log.d(TAG, "DELETE <" + key + ">");
				db.delete(bytes(key));
			}
		}

		// delete object from dirtyChunkList
		SimbaChunkList.removeDirtyChunks(obj_id);
	}

	public static void deleteObject(long obj_id, int startChunk) {
		String key = Long.toString(obj_id) + "," + Integer.toString(startChunk);
		DBIterator iterator = db.iterator();

		try {
			for (iterator.seek(bytes(key)); iterator.hasNext(); iterator.next()) {
				if (!(asString(iterator.peekNext().getKey()).startsWith(Long
						.toString(obj_id) + ",")))
					break;
				Log.d(TAG, "DELETE <" + asString(iterator.peekNext().getKey())
						+ ">");
				db.delete(iterator.peekNext().getKey());
			}
			iterator.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// delete object from dirtyChunkList
		SimbaChunkList.removeDirtyChunks(obj_id);
	}

	public static void crash() {
		System.exit(0);
	}
}
