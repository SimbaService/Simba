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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.ContentValues;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.ServiceConnection;
import android.database.Cursor;
import android.database.CursorWindow;
import android.database.MergeCursor;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;

import com.necla.simba.client.ISCSClient;
import com.necla.simba.client.CRChoice;
import com.necla.simba.client.ConnState;
import com.necla.simba.client.DataObjectRow;
import com.necla.simba.client.InternalDataRow;
import com.necla.simba.client.SimbaBroadcastReceiver;
import com.necla.simba.client.SimbaContentServiceAPI;
import com.necla.simba.client.SimbaCursorWindow;
import com.necla.simba.client.SimbaCursorWindowAllocationException;
import com.necla.simba.client.RowObject;
import com.necla.simba.client.TableProperties;

/***
 * Adapter class that implements Simba Client API.
 * 
 * @file SCSClientAdapter.java
 * @author Younghwan Go
 * @created 6:37:19 PM, Jul 9, 2012
 * @modified 2:19:13 PM, Feb 10, 2015
 */
public class SCSClientAdapter implements SCSClientAPI {
	private static final String TAG = "SCSClientAdapter";
	private static final String OBJTAG = "obj_";

	private Context app_context;
	private ISCSClient app_notify_callback;
	private ISCSClientAdapter app_ready_callback;
	private String app_name;
	private String tid;
	private int confidenceLevel;

	// list of opened objects Map<tbl, ArrayList<RowObject>>
	private Map<String, List<RowObject>> openObjects = new HashMap<String, List<RowObject>>();

	private SimbaContentServiceAPI svc = null;
	private IntentFilter intentFilter = new IntentFilter(
			SimbaBroadcastReceiver.CONNECTION_STATE_CHANGED);

	private BroadcastReceiver broadcastReceiver = new SimbaBroadcastReceiver() {
		@Override
		public void onReceive(Context arg0, Intent intent) {
			boolean networkState = intent.getBooleanExtra(
					SimbaBroadcastReceiver.EXTRA_CONNECTION_STATE, false);

			if (app_ready_callback != null)
				app_ready_callback.networkState(networkState);
		}
	};

	private ServiceConnection conn = new ServiceConnection() {
		public void onServiceDisconnected(ComponentName name) {
			app_ready_callback.disconnected();
			Log.v(TAG, "Service Disconnected");
		}

		public void onServiceConnected(ComponentName name, IBinder service) {
			Log.v(TAG, "Service Connected: " + service.getClass().getName());
			svc = SimbaContentServiceAPI.Stub.asInterface(service);

			try {
				tid = svc.registerApp(app_name, app_notify_callback);
				Log.v(TAG, "Registered " + app_name + ", tid = " + tid);

				// erase any open object list
				if (!openObjects.isEmpty()) {
					openObjects.clear();
				}

				// now SCSAPI is ready to use, notify user application
				app_ready_callback.ready();
				app_ready_callback.networkState(svc.isNetworkConnected());

			} catch (RemoteException e) {
				e.printStackTrace();
			}
		}
	};

	public void plug(Context context, final ISCSClientApp notify_callback,
			ISCSClientAdapter ready_callback) {
		app_context = context;

		app_notify_callback = new ISCSClient.Stub() {
			@Override
			public void newData(String table, int rows, int numDeletedRows)
					throws RemoteException {
				notify_callback.newData(table, rows, numDeletedRows);
			}

			@Override
			public void syncConflict(String table, int rows)
					throws RemoteException {
				notify_callback.syncConflict(table, rows);
			}

			@Override
			public void subscribeDone() throws RemoteException {
				notify_callback.subscribeDone();
			}

			@Override
			public void ping() throws RemoteException {
				// Log.d(TAG, "Received ping. I'm alive!");
			}
		};

		// app_notify_callback = notify_callback;
		app_ready_callback = ready_callback;
		String[] temp = context.getPackageName().split("\\.");
		app_name = temp[temp.length - 1];
		// app_name = context.getPackageName();
		context.registerReceiver(broadcastReceiver, intentFilter);
		app_context.bindService(
				new Intent(SimbaContentServiceAPI.class.getName()), conn,
				Context.BIND_AUTO_CREATE);
	}

	public void unplug() {
		try {
			boolean status = svc.unregisterApp(tid);
			Log.v(TAG, "SCS unregister: " + status);
			app_context.unregisterReceiver(broadcastReceiver);
			app_context.unbindService(conn);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		Log.v(TAG, "Unregistered " + app_name);
	}

	public boolean subscribeTable(String name, int period, int delay,
			ConnState syncpref) {
		boolean ret = false;

		try {
			ret = svc.subscribeTable(tid, name, period, delay, syncpref);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return ret;
	}

	public boolean createTable(String name, String cmd, int lvl,
			TableProperties props) {
		boolean ret = false;

		try {
			confidenceLevel = lvl;
			Log.d(TAG, "tid="+tid);
			Log.d(TAG, "name="+name);
			Log.d(TAG, "cmd="+cmd);
			Log.d(TAG, "lvl="+lvl);
			Log.d(TAG, "props="+props);
			ret = svc.createTable(tid, name, cmd, lvl, props);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return ret;
	}

	public void closeObject(long obj_id) {
		openObjects.remove(obj_id);
	}

	public SCSCursor readData(String tbl, String[] projection,
			String selection, String[] selectionArgs, String sortOrder) {
		List<SCSInputStream> mis = new ArrayList<SCSInputStream>();
		SimbaCursorWindow cw = null;
		Cursor cursor = null;
		boolean hasObj = false;
		int columnCount = 0;
		try {
			Log.d(TAG, "tid="+tid);
			Log.d(TAG, "tbl="+tbl);

			String schema = svc.getSchemaSQL(tid, tbl);
			String[] columns = schema.split("\\s+");

			// check if any column contains object
			if (projection == null) {
				for (String c : columns) {
					if (c.endsWith(","))
						continue;
					if (c.startsWith(OBJTAG)) {
						hasObj = true;
						break;
					} else {
						columnCount++;
					}
				}
			} else {
				for (String proj : projection) {
					if (proj.startsWith(OBJTAG)) {
						hasObj = true;
					} else {
						columnCount++;
					}
				}
			}
			cw = svc.read(tid, tbl, projection, selection, selectionArgs,
					sortOrder);
			cursor = new SimbaWindowedCursor(cw.getWindow(), cw.getColumns());
		} catch (RemoteException e) {
			if (e instanceof SimbaCursorWindowAllocationException) {
				// We assume this is due to
				// android/database/CursorWindowAllocationException
				// now we need to use the LIMIT/OFFSET trick to get partial the
				// result set iteratively e.g. binary search
				// then use MergeCursor to combine the list of Cursors into a
				// single one
				List<Cursor> cursors = new ArrayList<Cursor>();
				int OFFSET = 0;
				boolean goon = true;

				do {
					cw = read_data(tbl, projection, selection, selectionArgs,
							sortOrder, OFFSET);
					if (cw != null && cw.getWindow().getNumRows() > 0) {
						cursors.add(new SimbaWindowedCursor(cw.getWindow(), cw
								.getColumns()));
						OFFSET += cw.getWindow().getNumRows();
					} else { // stop when there is no more data to read
						goon = false;
					}
				} while (goon);

				cursor = new MergeCursor(new Cursor[cursors.size()]);
			} else {
				e.printStackTrace();
			}
		}

		if (cursor.getCount() > 0) {
			if (hasObj) {
				CursorWindow obj_cw = new CursorWindow("obj_cw");
				int row = 0, col = 0;

				// increase columnCount by 1 for row_id
				obj_cw.setNumColumns(columnCount + 1);
				cursor.moveToFirst();
				do {
					String id = cursor.getString(0);
					obj_cw.allocRow();

					while (col < cursor.getColumnCount()) {
						if (cursor.getColumnName(col).startsWith(OBJTAG)) {
							mis.add(new SCSInputStream(svc, id, cursor
									.getLong(col)));
						} else {
							obj_cw.putString(cursor.getString(col), row, col);
						}
						col++;
					}
					col = 0;
					row++;
				} while (cursor.moveToNext());

				cursor.close();

				cursor = new SimbaWindowedCursor(obj_cw, Arrays.copyOfRange(
						cw.getColumns(), 0, columnCount));
			}
		}

		return new SCSCursor(cursor, mis);
	}

	private SimbaCursorWindow read_data(String tbl, String[] projection,
			String selection, String[] selectionArgs, String sortOrder,
			int OFFSET) {
		int LIMIT = 65536; // starting value of binary search
		boolean goon = true;
		SimbaCursorWindow cw = null;

		do {
			try {
				cw = svc.read(tid, tbl, projection, selection, selectionArgs,
						sortOrder + " LIMIT " + OFFSET + "," + LIMIT);
				goon = false;
			} catch (RemoteException re) {
				if (re instanceof SimbaCursorWindowAllocationException) {
					goon = true;
					LIMIT = LIMIT >> 1;
				}
			}
		} while (goon);

		return cw;
	}

	public List<SCSOutputStream> writeData(String tbl, ContentValues data,
			String[] objectOrdering) {
		List<SCSOutputStream> mos = new ArrayList<SCSOutputStream>(1);

		try {
			List<RowObject> ro_list = svc.write(tid, tbl, data, objectOrdering);
			// list of open objects
			List<RowObject> ro = openObjects.get(tbl);
			if (ro == null) {
				ro = new ArrayList<RowObject>();
			}
			for (RowObject obj : ro_list) {
				mos.add(new SCSOutputStream(svc, tid, tbl, obj.GetRowID(), obj
						.GetObjectID(), obj.GetOffset(), confidenceLevel));
				ro.add(obj);
			}
			openObjects.put(tbl, ro);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
		return mos;
	}

	public List<SCSOutputStream> updateData(String tbl, ContentValues data,
			String selection, String[] selectionArgs, String[] objectOrdering) {
		List<SCSOutputStream> mos = new ArrayList<SCSOutputStream>(1);

		try {
			List<RowObject> ro_list = svc.update(tid, tbl, data, selection,
					selectionArgs, objectOrdering);
			// list of open objects
			List<RowObject> ro = openObjects.get(tbl);
			if (ro == null) {
				ro = new ArrayList<RowObject>();
			}
			for (RowObject obj : ro_list) {
				mos.add(new SCSOutputStream(svc, tid, tbl, obj.GetRowID(), obj
						.GetObjectID(), obj.GetOffset(), confidenceLevel));
				ro.add(obj);
			}
			openObjects.put(tbl, ro);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return mos;
	}

	public int deleteData(String tbl, String selection, String[] selectionArgs) {
		int numRows = 0;
		try {
			numRows = svc.delete(tid, tbl, selection, selectionArgs);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return numRows;
	}

	public void writeSyncOneshot(String tbl, int delay) {
		try {
			svc.writeSyncOneshot(tid, tbl, delay);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	public void registerPeriodicWriteSync(String tbl, int period, int delay,
			ConnState syncpref) {
		try {
			svc.registerPeriodicWriteSync(tid, tbl, period, delay, syncpref);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	/*
	 * Not Used public void registerPeriodicReadSync(String tbl, int period, int
	 * delay, ConnState syncpref) { try { svc.registerPeriodicWriteSync(tid,
	 * tbl, period, delay, syncpref); } catch (RemoteException e) {
	 * e.printStackTrace(); } }
	 */

	public void unregisterPeriodicWriteSync(String tbl) {
		try {
			svc.unregisterPeriodicWriteSync(tid, tbl);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public void beginCR(String tbl) {
		try {
			svc.beginCR(tid, tbl);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public List<DataObjectRow> getConflictedRows(String tbl) {
		List<DataObjectRow> ret = new ArrayList<DataObjectRow>();
		// return structure (id, column data, SCSInputStream)
		try {
			List<InternalDataRow> rows = svc.getConflictedRows(tid, tbl);
			for (InternalDataRow row : rows) {
				List<SCSInputStream> mis_list = new ArrayList<SCSInputStream>();
				for (int i = 0; i < row.getObjectData().size(); i++) {
					SCSInputStream mis = new SCSInputStream(svc, row.getId(),
							row.getObjectData().get(i));
					mis_list.add(mis);
				}
				DataObjectRow dor = new DataObjectRow(row.getId(),
						row.getColumnData(), mis_list, row.isDeleted());
				ret.add(dor);
			}
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		return ret;
	}

	public void resolveConflict(String tbl, DataObjectRow row, CRChoice choice) {
		try {
			// InternalDataRow dr = (InternalDataRow) row;
			svc.resolveConflict(tid, tbl, row.getId(), choice);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public void endCR(String tbl) {
		try {
			svc.endCR(tid, tbl);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public void readSyncOneshot(String tbl) {
		try {
			svc.readSyncOneshot(tid, tbl);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	public void subscribePeriodicReadSync(String tbl, int period, int delay,
			ConnState syncpref) {
		try {
			svc.subscribePeriodicReadSync(tid, tbl, period, delay, syncpref);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

	}

	public void unsubscribePeriodicReadSync(String tbl) {
		try {
			svc.unsubscribePeriodicReadSync(tid, tbl);
		} catch (RemoteException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean isNetworkConnected() {
		try {
			return svc.isNetworkConnected();
		} catch (RemoteException e) {
			e.printStackTrace();
			return false;
		}
	}
}
