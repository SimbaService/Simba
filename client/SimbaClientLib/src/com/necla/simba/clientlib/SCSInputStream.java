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

import android.os.RemoteException;
import android.util.Log;

import com.necla.simba.client.SimbaContentServiceAPI;

/**
 * @author Younghwan Go
 * @created Jul 17, 2013 2:21:04 PM
 * 
 */
public class SCSInputStream {
	private final static int BINDERSIZE = 128 * 1024;

	private SimbaContentServiceAPI svc;
	protected String row_id;
	private long obj_id;
	private int offset;

	public SCSInputStream(SimbaContentServiceAPI svc, String row_id,
			long obj_id) {
		this.svc = svc;
		this.row_id = row_id;
		this.obj_id = obj_id;
		this.offset = 0;
	}

	public int read(byte[] buffer) {
		int res = -1;
		try {
			// if buffer size is bigger than BINDERSIZE, read by parts and merge
			if (buffer.length > BINDERSIZE) {
				int binder_offset = 0;
				while (binder_offset < buffer.length) {
					int size = buffer.length - binder_offset < BINDERSIZE ? buffer.length
							- binder_offset
							: BINDERSIZE;
					byte[] buf = new byte[size];
					res = svc.readStream(obj_id, buf, binder_offset, offset,
							buffer.length);
					if (res > 0) {
						System.arraycopy(buf, 0, buffer, binder_offset, res);
						binder_offset += res;
						offset += res;
					}
				}
				// full buffer is complete
				assert (binder_offset == buffer.length);
				return binder_offset;
			} else {
				res = svc.readStream(obj_id, buffer, 0, offset, buffer.length);
				if (res > 0) {
					offset += res;
				}
			}
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return res;
	}
}
