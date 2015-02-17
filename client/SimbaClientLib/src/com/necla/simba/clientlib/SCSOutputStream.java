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
package com.necla.simba.clientlib;

import android.content.ContentValues;
import android.os.RemoteException;
import android.util.Log;

import com.necla.simba.client.SimbaContentServiceAPI;
import com.necla.simba.client.SharedConstants;

/**
 * @author Younghwan Go
 * @created Jul 16, 2013 10:07:54 AM
 * 
 */
public class SCSOutputStream {
	private final String TAG = "SCSOutputStream";
	// binder limits max message size to be 1MB
	private final static int BINDERSIZE = 128 * 1024;
	private final static int CHUNKSIZE = SharedConstants.CHUNKSIZE;
	private byte[] chunk = new byte[CHUNKSIZE];
	private int chunk_num;
	private boolean isDirty = false;

	private SimbaContentServiceAPI svc;
	private String tid;
	private String tbl;
	protected String row_id;
	private long obj_id;
	private int offset;

	private int confidenceLevel;

	public SCSOutputStream(SimbaContentServiceAPI svc, String tid, String tbl,
			String row_id, long obj_id, int offset, int lvl) {
		this.svc = svc;
		this.tid = tid;
		this.tbl = tbl;
		this.row_id = row_id;
		this.obj_id = obj_id;
		chunk_num = offset / CHUNKSIZE;
		this.offset = offset - (chunk_num * CHUNKSIZE);
		this.confidenceLevel = lvl;
	}

	public int writeStream(SCSClientAPI adapter, byte[] buffer) {
		int length = buffer.length;
		int off = 0;

		try {
			while (offset + length >= CHUNKSIZE) {
				System.arraycopy(buffer, off, chunk, offset, CHUNKSIZE - offset);

				// if the chunk size is bigger than binder size, send in parts
				if (CHUNKSIZE > BINDERSIZE) {
					int binder_offset = 0;
					while (binder_offset < CHUNKSIZE) {
						byte[] buf = new byte[BINDERSIZE];
						System.arraycopy(chunk, binder_offset, buf, 0,
								BINDERSIZE);
						int res = 0;
						if ((res = svc.writeStream(tid, tbl, obj_id, chunk_num,
								buf, binder_offset, CHUNKSIZE)) == -1) {
							// if object is stale, close it
							Log.d(TAG, "Object: " + obj_id
									+ " is stale! closing it");
							adapter.closeObject(obj_id);
							return -1;
						}
						binder_offset += res;
					}
				} else {
					if (svc.writeStream(tid, tbl, obj_id, chunk_num, chunk, 0,
							CHUNKSIZE) == -1) {
						// if object is stale, close it
						Log.d(TAG, "Object: " + obj_id
								+ " is stale! closing it");
						adapter.closeObject(obj_id);
						return -1;
					}
				}
				chunk_num++;
				length -= CHUNKSIZE - offset;
				off += CHUNKSIZE - offset;
				offset = 0;

				// YGO: don't update if strong consistency
				if (!isDirty && confidenceLevel != 1) {
					// set _dirtyObj flag
					ContentValues values = new ContentValues();
					values.put("_dirtyObj", Boolean.TRUE);
					svc.update(tid, tbl, values, "_id = ?",
							new String[] { row_id }, null);
					isDirty = true;
				}
			}
			if (length > 0) {
				System.arraycopy(buffer, off, chunk, offset, length);
				offset += length;
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return length;
	}

	public int truncate(SCSClientAPI adapter, int len) {
		try {
			// YGO: don't update if strong consistency
			if (!isDirty && confidenceLevel != 1) {
				// set _dirtyObj flag
				ContentValues values = new ContentValues();
				values.put("_dirtyObj", Boolean.TRUE);
				svc.update(tid, tbl, values, "_id = ?",
						new String[] { row_id }, null);
				isDirty = true;
			}
			return svc.truncate(tid, tbl, row_id, obj_id, len);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return -1;
	}

	public int flush(SCSClientAPI adapter) {
		try {
			if (offset > 0) {
				// if the remain size is bigger than binder size, send in parts
				if (offset > BINDERSIZE) {
					int binder_offset = 0;
					while (binder_offset < offset) {
						byte[] buf = new byte[BINDERSIZE];
						System.arraycopy(chunk, binder_offset, buf, 0,
								BINDERSIZE);
						int res = 0;
						if ((res = svc.writeStream(tid, tbl, obj_id, chunk_num,
								buf, binder_offset, offset)) == -1) {
							// if object is stale, close it
							Log.d(TAG, "Object: " + obj_id
									+ " is stale! closing it");
							adapter.closeObject(obj_id);
							return -1;
						}
						binder_offset += res;
					}
				} else {
					byte[] buf = new byte[offset];
					System.arraycopy(chunk, 0, buf, 0, offset);
					if (svc.writeStream(tid, tbl, obj_id, chunk_num, buf, 0,
							offset) == -1) {
						// if object is stale, close it
						Log.d(TAG, "Object: " + obj_id
								+ " is stale! closing it");
						adapter.closeObject(obj_id);
						return -1;
					}
				}
				offset = 0;
				// YGO: don't update if strong consistency
				if (!isDirty && confidenceLevel != 1) {
					// set _dirtyObj flag
					ContentValues values = new ContentValues();
					values.put("_dirtyObj", Boolean.TRUE);
					svc.update(tid, tbl, values, "_id = ?",
							new String[] { row_id }, null);
					isDirty = true;
				}
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return 0;
	}

	public void close(SCSClientAPI adapter) {
		try {
			if (flush(adapter) == 0) {
				adapter.closeObject(obj_id);
				svc.decrementObjCounter(tid, tbl, obj_id);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}
}
