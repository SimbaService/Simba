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

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.iq80.leveldb.ReadOptions;

import android.content.ContentValues;
import android.database.CrossProcessCursorWrapper;
import android.database.Cursor;
import android.database.CursorWindow;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.os.RemoteException;
import android.util.Log;

import com.necla.simba.protocol.ColumnData;
import com.necla.simba.protocol.DataRow;
import com.necla.simba.protocol.ObjectHeader;
import com.necla.simba.protocol.SimbaConsistency;
import com.necla.simba.protocol.SyncHeader;
import com.necla.simba.protocol.TornRowRequest;

/***
 * This class provides the application table abstraction: any CRUD operations on
 * the table have to go through its API.
 * 
 * @file SimbaTable.java
 * @author Younghwan Go
 * @created 10:36:00 AM, Jul 11, 2012
 * @modified 2:19:13 PM, Feb 10, 2015
 */
public class SimbaTable {
	private final String TAG = "SimbaTable";
	private static final String OBJTAG = "obj_";

	private SQLiteDatabase db;
	private String app, tbl, schema_sql;
	private boolean schema_sync_pending;
	private int writeperiod = -1, readperiod = -1;
	private int writedt = -1, readdt = -1;
	private final int DEFAULT_VERSION = -1;
	private final String TBL_DEL, TBL_CONFLICT, TBL_DIRTY_CHUNK_LIST;
	private List<String> pk_cols;
	private String[] user_cols;
	private ConnState writeconnPref = ConnState.TG,
			readconnPref = ConnState.TG;
	private TableProperties props;
	private Metadata metadata;
	private int tbl_rev = -1;
	private boolean inCR = false;
	private boolean isRecovering = false;

	private boolean stopPullData = false;
	private boolean sendNotificationPull = false;

	private int consistencyLevel;
	private SyncSet syncSet = null;

	private Map<Long, String> objRow = new HashMap<Long, String>();
	private static final Set<String> meta_fields;
	
	private SimpleDateFormat dateFormatter = new SimpleDateFormat(
			"HH:mm:ss.SSS");

	static {
		meta_fields = new HashSet<String>();
		String[] fields = new String[] { "_id", "_rev", "_torn", "_dirty",
				"_dirtyObj", "_openObj", "_sync", "_conflict" };
		for (String f : fields)
			meta_fields.add(f);

	}

	// create auxiliary tables: tbl_deleted and tbl_server
	private void create_aux_table(String cmd) {
		db.execSQL("CREATE TABLE IF NOT EXISTS "
				+ TBL_DEL
				+ "("
				+ cmd
				+ ",_id VARCHAR, _rev INT, _sync BIT DEFAULT 0, _conflict BIT, _dirty BIT, _dirtyObj BIT);");

		// add columns "countobj_x" for each object column
		String cmd_conflict = "";
		String[] cols = cmd.split("\\s+");
		for (int i = 0; i < cols.length; i += 2) {
			if (!cols[i].startsWith(OBJTAG)) {
				// e.g. "title VARCHAR, "
				cmd_conflict += cols[i] + " " + cols[i + 1] + " ";
			} else {
				// e.g. "countobj_1 INT, obj_1 BIGINT, "
				cmd_conflict += "count" + cols[i] + " INT, " + cols[i] + " "
						+ cols[i + 1] + " ";
			}
		}
		db.execSQL("CREATE TABLE IF NOT EXISTS " + TBL_CONFLICT + "("
				+ cmd_conflict + ", _id VARCHAR UNIQUE, _rev INT, _sync BIT DEFAULT 0);");
		// tbl_dirty_chunk_list
		db.execSQL("CREATE TABLE IF NOT EXISTS " + TBL_DIRTY_CHUNK_LIST
				+ "(obj LONG, chunklist VARCHAR);");
	}

	/**
	 * 
	 * @param uid
	 * @param app_db
	 * @param tbl
	 * @param cmd
	 * @param recovered
	 *            true if this is a recovered table, we don't need to create aux
	 *            tables or add extra meta fields
	 */
	public SimbaTable(String uid, SQLiteDatabase app_db, String tbl,
			String cmd, int lvl, TableProperties props, Metadata metadata,
			boolean recovered) {
		this.db = app_db;
		this.app = uid;
		this.tbl = tbl;
		this.schema_sql = cmd;
		this.schema_sync_pending = false;

		this.TBL_DEL = tbl + "_deleted";
		this.TBL_CONFLICT = tbl + "_server";
		this.TBL_DIRTY_CHUNK_LIST = tbl + "_chunk";
		this.pk_cols = this.get_primary_keys();
		this.user_cols = this.getUserColumns();
		this.props = props;
		this.metadata = metadata;

		this.consistencyLevel = lvl;

		if (!recovered)
			create_aux_table(cmd);
		else
			clearSyncFlagOnRecovery();
	}

	public boolean isPartial() {
		return this.props.isPartial();
	}

	private void clearSyncFlagOnRecovery() {
		ContentValues cv = new ContentValues();
		cv.put("_sync", Boolean.FALSE);
		cv.put("_dirty", Boolean.TRUE);
		db.update(tbl, cv, "_dirty = ?", new String[] { "1" });
	}

	public static String schemaToColumnSchema(String schema) {
		int startIndex = schema.indexOf('(') + 1;
		int endIndex = schema.indexOf(')', startIndex);
		System.out.println("schema=" + schema);
		schema = schema.substring(startIndex, endIndex);
		String[] cols = schema.split(",");
		System.out.println("cols=" + cols);

		StringBuilder sb = new StringBuilder();
		boolean first = true;
		for (String s : cols) {
			String cname = s.replaceAll("^\\s+", "").split(" ")[0];
			System.out.println("cname='" + cname + "'");

			if (!meta_fields.contains(cname)) {
				if (!first)
					sb.append(",");
				first = false;
				sb.append(s);

			}

		}

		System.out.println("cmd='" + sb.toString() + "'");
		return sb.toString();
	}

	public boolean schemaSyncPending() {
		return this.schema_sync_pending;
	}

	public void setSchemaSyncPending(boolean status) {
		this.schema_sync_pending = status;
	}

	public String getSchemaSQL() {
		return this.schema_sql;
	}

	public SQLiteDatabase getDatabase() {
		return this.db;
	}

	public boolean isRecovering() {
		return this.isRecovering;
	}

	public void doneRecovering() {
		this.isRecovering = false;
	}

	public boolean isStopPullData() {
		return this.stopPullData;
	}

	public boolean isSendNotificationPull() {
		return this.sendNotificationPull;
	}

	public void setSendNotificationPull(boolean flag) {
		this.sendNotificationPull = flag;
	}

	public void resetStopPullData() {
		assert stopPullData == true;
		Log.d(TAG, "resetting stopPullData to FALSE!");
		stopPullData = false;
	}

	public void incrementObjCounter(long obj_id, String row_id) {
		// assert (objRow.containsKey(obj_id) == false);
		Cursor cursor = db.query(tbl, new String[] { "_openObj" }, "_id = ?",
				new String[] { row_id }, null, null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			ContentValues cv = new ContentValues();
			int count = cursor.getInt(0);
			cv.put("_openObj", count + 1);
			updateWithoutDirty(cv, "_id = ?", new String[] { row_id });
			objRow.put(obj_id, row_id);

			Log.d(TAG, "Incrementing _openObj value! obj_id: " + obj_id
					+ ", count: " + count + " -> " + (count + 1));
		}
	}

	public int decrementObjCounter(long obj_id) {
		assert (objRow.containsKey(obj_id) == true);
		boolean isStrongConsistency = (consistencyLevel == SimbaConsistency.Type.STRONG) ? true
				: false;

		Cursor cursor = db.query(tbl, new String[] { "_openObj" }, "_id = ?",
				new String[] { objRow.get(obj_id) }, null, null, null);
		if (cursor.getCount() > 0 && !isStrongConsistency) {
			cursor.moveToFirst();
			ContentValues cv = new ContentValues();
			int count = cursor.getInt(0);
			assert (count > 1);
			cv.put("_openObj", count - 1);
			updateWithoutDirty(cv, "_id = ?",
					new String[] { objRow.get(obj_id) });
			// remove from objRow at row or object delete!
			// objRow.remove(obj_id);

			Log.d(TAG, "Decrementing _openObj value! obj_id: " + obj_id
					+ ", count: " + count + " -> " + (count - 1));

			return (count - 1);
		} else if (isStrongConsistency) {
			if (cursor.getCount() > 0) {
				Log.d(TAG, "Closing object for update with strong consistency");
			} else {
				// YGO: close object for first row write
				Log.d(TAG,
						"Closing object for first write with strong consistency");
			}
			ContentValues cv_syncSet = syncSet.getContentValues();
			int openObj = cv_syncSet.getAsInteger("_openObj");
			cv_syncSet.put("_openObj", openObj - 1);
			syncSet.setContentValues(cv_syncSet);

			
			
			return (openObj - 1);
		}
		/* this should not happen! */
		return -1;
	}

	public String getRowID(long obj_id) {
		return objRow.get(obj_id);
	}

	public SyncSet getSyncSet() {
		return syncSet;
	}

	public void setTornRows(List<Long> torn_objs) {
		String[] projs = new String[user_cols.length + 2];
		projs[0] = "_id";
		projs[1] = "_dirtyObj";
		System.arraycopy(user_cols, 0, projs, 2, user_cols.length);

		// find rows with opened objects
		db.beginTransaction();
		Cursor cursor = db.query(tbl, projs, "_openObj > ?",
				new String[] { "0" }, null, null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			do {
				String id = cursor.getString(0);
				boolean client_dirtyObj = (cursor.getInt(1) == 1);

				ContentValues cv = new ContentValues();
				// torn if either dirty object or dirty column and not
				// conflicted
				if (client_dirtyObj) {
					Log.d(TAG, "Setting torn row: " + id);
					cv.put("_torn", Boolean.TRUE);
					cv.put("_dirty", Boolean.FALSE);
					cv.put("_dirtyObj", Boolean.FALSE);
					cv.put("_openObj", 0);
					cv.put("_sync", Boolean.FALSE);
					cv.put("_conflict", Boolean.FALSE);

					// store obj_ids that are in a torn row
					for (int i = 2; i < projs.length; i++) {
						if (cursor.getColumnName(i).startsWith(OBJTAG)) {
							torn_objs.add(cursor.getLong(i));
						}
					}
				} else {
					Log.d(TAG,
							"Setting open object count to 0 for non-torn row: "
									+ id);
					// since row is not dirty, reset object count to 0
					cv.put("_openObj", 0);
				}
				updateWithoutDirty(cv, "_id = ?", new String[] { id });
			} while (cursor.moveToNext());
		}
		db.setTransactionSuccessful();
		db.endTransaction();
	}

	public void recoverDirtyRows() {
		String[] projs = new String[user_cols.length + 2];
		projs[0] = "_id";
		projs[1] = "_dirtyObj";
		System.arraycopy(user_cols, 0, projs, 2, user_cols.length);

		db.beginTransaction();

		// TBL - find rows set as _sync
		Cursor cursor = db.query(tbl, projs, "_sync = ?", new String[] { "1" },
				null, null, null);
		if (cursor.getCount() > 0) {
			assert (SimbaChunkList.isDirtyChunkListEmpty() == false);
			cursor.moveToFirst();
			do {
				String id = cursor.getString(0);
				boolean client_dirtyObj = (cursor.getInt(1) == 1);

				ContentValues cv = new ContentValues();
				cv.put("_sync", Boolean.FALSE);
				cv.put("_dirty", Boolean.TRUE);

				// set object's dirty chunks dirty
				for (int i = 2; i < cursor.getColumnCount(); i++) {
					if (cursor.getColumnName(i).startsWith(OBJTAG)) {
						long obj_id = cursor.getLong(i);
						// check if any object is dirty via dirtyChunkList
						if (SimbaChunkList.getDirtyChunks(obj_id) != null) {
							// if dirtyObj is already set, send all objects
							if (client_dirtyObj) {
								Log.d(TAG, "Recovering dirty row: " + id
										+ " for all chunks of object: "
										+ obj_id);
								// remove object from dirty chunk list
								// then it will send entire object at sync time
								SimbaChunkList.removeDirtyChunks(obj_id);
							} else {
								Log.d(TAG, "Recovering dirty row: " + id
										+ " for some chunks of object: "
										+ obj_id);
								cv.put("_dirtyObj", Boolean.TRUE);
								break;
							}
						}
					}
				}
				updateWithoutDirty(cv, "_id = ?", new String[] { id });
			} while (cursor.moveToNext());
		}

		db.setTransactionSuccessful();
		db.endTransaction();
	}

	public int recoverConflictTable() {
		int numConflictedRows = 0;

		String[] projs = new String[user_cols.length + 3];
		projs[0] = "_id";
		projs[1] = "_rev";
		projs[2] = "_sync";
		System.arraycopy(user_cols, 0, projs, 3, user_cols.length);

		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(TBL_CONFLICT);
		Cursor cursor = qb.query(db, projs, null, null, null, null, null);
		if (cursor.getCount() > 0) {
			Log.d(TAG, "Recovering CONFLICT TABLE");
			String id = cursor.getString(0);
			int rev = cursor.getInt(1);
			boolean conflictRowInSync = (cursor.getInt(2) == 1);
			cursor.moveToFirst();
			do {
				Cursor local_cursor = db.query(tbl, new String[] { "_conflict",
						"_torn" }, "_id = ?",
						new String[] { cursor.getString(0) }, null, null, null);
				if (local_cursor.getCount() > 0) {
					local_cursor.moveToFirst();
					if (local_cursor.getInt(0) == 0
							|| local_cursor.getInt(1) == 1 || conflictRowInSync) {
						// 1) delete torn row
						// 2) delete non-conflict row since it may be incomplete
						// 3) delete row in TBL_CONFLICT if it was in the middle
						// of update
						Log.d(TAG, "Deleting Row ID: " + id + " with _rev: "
								+ rev + ", tbl_rev: " + getRev()
								+ " from CONFLICT TABLE");
						db.delete(TBL_CONFLICT, "_id = ?", new String[] { id });

						// if conflicted row was being updated before crash,
						// clear _conflict flag in local table
						if (conflictRowInSync) {
							ContentValues cv = new ContentValues();
							cv.put("_conflict", Boolean.FALSE);
							db.update(tbl, cv, "_id = ?", new String[] { id });
						}
					} else {
						// keep the row since this is normal conflict row
						Log.d(TAG, "Keeping Row ID: " + cursor.getString(0)
								+ " with _rev: " + rev + ", tbl_rev: "
								+ getRev() + " in CONFLICT TABLE");
						numConflictedRows++;
					}
				}
				// find row in delete table
				else {
					local_cursor = db.query(TBL_DEL,
							new String[] { "_conflict" }, "_id = ?",
							new String[] { cursor.getString(0) }, null, null,
							null);
					if (local_cursor.getCount() > 0) {
						local_cursor.moveToFirst();
						if (local_cursor.getInt(0) == 0) {
							// delete non-conflict row since it may be
							// incomplete
							Log.d(TAG, "Deleting Row ID: " + id
									+ " with _rev: " + rev + ", tbl_rev: "
									+ getRev() + " from CONFLICT TABLE");
							db.delete(TBL_CONFLICT, "_id = ?",
									new String[] { id });
						} else {
							// keep the row since this is normal conflict row
							Log.d(TAG, "Keeping Row ID: " + id + " with _rev: "
									+ rev + ", tbl_rev: " + getRev()
									+ " in CONFLICT TABLE");
							numConflictedRows++;
						}
					}
				}
			} while (cursor.moveToNext());
		}
		return numConflictedRows;
	}

	public int recoverDeleteTable() {
		// delete row if the same row exists in local table
		// this can happen for resolveConflict where row from delete table is
		// moved back to local table then crashes before deleting the row
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(TBL_DEL);
		Cursor cursor = qb.query(db, new String[] { "_id" }, null, null, null,
				null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			do {
				Cursor local_cursor = db.query(tbl, null, "_id=?",
						new String[] { cursor.getString(0) }, null, null, null);
				if (local_cursor.getCount() > 0) {
					db.delete(TBL_DEL, "_id=?",
							new String[] { cursor.getString(0) });
				}
			} while (cursor.moveToNext());
		}

		// set sync pending rows to be false
		ContentValues cv = new ContentValues();
		cv.put("_sync", Boolean.FALSE);
		updateDelete(cv, "_sync=?", new String[] { "1" });

		// get number of conflict rows
		cursor = db.rawQuery("SELECT COUNT(*) FROM " + TBL_DEL
				+ " WHERE _conflict=1", null);
		cursor.moveToFirst();

		return cursor.getInt(0);
	}

	public List<RowObject> write(ContentValues values, String[] objectOrdering)
			throws RemoteException {
		if (inCR)
			throw new RemoteException("In CR");
		return writeInternal(values, objectOrdering, false, true);
	}

	public List<RowObject> writeInternal(ContentValues values,
			String[] objectOrdering, boolean isInsertObject, boolean localWrite) {
		List<RowObject> ro_list = new ArrayList<RowObject>();
		String _id, _obj;
		long obj_id;

		if (!values.containsKey("_id")) {
			if (pk_cols.isEmpty()) {
				_id = UUID.randomUUID().toString();
			} else {
				/*
				 * TODO: when primary keys span multiple columns, what do we
				 * store? hash of the combined string?
				 */
				_id = UUID.randomUUID().toString();
			}
			values.put("_id", _id);
		} else {
			_id = values.getAsString("_id");
		}

		/* check for object write */
		if (!isInsertObject) {
			int openObj = 0;
			// set obj_id in the order the app defined
			if (objectOrdering != null) {
				for (int i = 0; i < objectOrdering.length; i++) {
					for (Map.Entry<String, Object> s : values.valueSet()) {
						_obj = s.getKey();
						if (_obj.startsWith(OBJTAG)
								&& _obj.equals(objectOrdering[i])) {
							obj_id = SimbaLevelDB.getMaxObjID();
							Log.d(TAG, "Inserting obj_id to column! [col: "
									+ _obj + ", obj_id: " + obj_id + "]");

							ro_list.add(new RowObject(tbl, _id, obj_id, 0));

							// increment open counter for obj_id
							objRow.put(obj_id, _id);
							openObj++;

							// insert object id into value
							values.put(_obj, obj_id);
							SimbaLevelDB.setMaxObjID(obj_id + 1);
						}
					}
				}
			}
			// set obj_id in any order
			else {
				for (Map.Entry<String, Object> s : values.valueSet()) {
					_obj = s.getKey();
					if (_obj.startsWith(OBJTAG)) {
						obj_id = SimbaLevelDB.getMaxObjID();
						Log.d(TAG, "Inserting obj_id to column! [col: " + _obj
								+ ", obj_id: " + obj_id + "]");

						ro_list.add(new RowObject(tbl, _id, obj_id, 0));

						// increment open counter for obj_id
						objRow.put(obj_id, _id);
						openObj++;

						// insert object id into value
						values.put(_obj, obj_id);
						SimbaLevelDB.setMaxObjID(obj_id + 1);
					}
				}
			}
			if (openObj > 0) {
				Log.d(TAG, "Incrementing _openObj value for first row write!"
						+ " count: 0 -> " + openObj);
				values.put("_openObj", openObj);
			}
		}

		if (consistencyLevel == SimbaConsistency.Type.STRONG && localWrite) {
			// YGO: temporarily write values into memory
			syncSet = new SyncSet(_id, values, Boolean.FALSE);
		} else {
			// write to local table only if not a strong consistency
			db.insert(tbl, null, values);
		}

		return ro_list;
	}

	public void writeConflictCopy(ContentValues values) {
        Boolean b = values.getAsBoolean("_sync");
        Log.d(TAG, "writeConflictCopy b=" + b);
		db.insertWithOnConflict(TBL_CONFLICT, null, values,
				SQLiteDatabase.CONFLICT_REPLACE);
	}

	public SimbaCursorWindow read(String[] projs, String sels,
			String[] selArgs, String sortOrder)
			throws SimbaCursorWindowAllocationException, RemoteException {

		if (inCR)
			throw new RemoteException("In CR");
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(tbl);

		// need to pre-process the projection list so that meta-fields will
		// not be returned to user in the result set
		boolean sel_all = false;
		if (projs == null || projs.length == 0) {
			sel_all = true;
		} else {
			for (String proj : projs) {
				if (proj.equals("*")) {
					sel_all = true;
					break;
				}
			}
		}

		String[] _projs;
		if (sel_all) {
			_projs = new String[1 + user_cols.length];
			_projs[0] = "_id";
			System.arraycopy(user_cols, 0, _projs, 1, user_cols.length);
		} else {
			_projs = new String[1 + projs.length];
			_projs[0] = "_id";
			System.arraycopy(projs, 0, _projs, 1, projs.length);
		}

		Cursor cursor = null;
		try {
			cursor = qb.query(db, _projs, sels, selArgs, null, null, sortOrder);
		} catch (RuntimeException rte) {
			// we are assuming rte is CursorWindowAllocationException, since
			// it's NOT
			// public and cannot be accessed outside android.database pacage.
			throw new SimbaCursorWindowAllocationException(rte.getMessage());
		}

		CursorWindow cw = new CursorWindow(null);
		CrossProcessCursorWrapper cpcw = new CrossProcessCursorWrapper(cursor);
		cpcw.fillWindow(0, cw);

		cursor.close();
		cpcw.close();

		return new SimbaCursorWindow(_projs, cw);
	}

	public List<RowObject> update(ContentValues values, String sels,
			String[] selArgs, String[] objectOrdering) throws RemoteException {
		if (inCR)
			throw new RemoteException("In CR");

		List<RowObject> ro_list = new ArrayList<RowObject>();
		String _id, _obj;
		long obj_id;
		int openObj;

		String[] projs = new String[user_cols.length + 2];
		projs[0] = "_id";
		projs[1] = "_openObj";
		System.arraycopy(user_cols, 0, projs, 2, user_cols.length);

		Cursor cursor = db.query(tbl, projs, sels, selArgs, null, null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			_id = cursor.getString(0);
			openObj = cursor.getInt(1);
			int newOpen = 0;
			// set obj_id in the order the app defined
			if (objectOrdering != null) {
				for (int j = 0; j < objectOrdering.length; j++) {
					for (Map.Entry<String, Object> s : values.valueSet()) {
						_obj = s.getKey();
						if (_obj.startsWith(OBJTAG)
								&& _obj.equals(objectOrdering[j])) {
							// get obj_id of the object column
							for (int i = 2; i < cursor.getColumnCount(); i++) {
								if (_obj.equals(cursor.getColumnName(i))) {
									// YGO: get new obj_id if there is no object
									// value or is a strong consistency
									if (cursor.isNull(i)
											|| consistencyLevel == SimbaConsistency.Type.STRONG) {
										obj_id = SimbaLevelDB.getMaxObjID();
										SimbaLevelDB.setMaxObjID(obj_id + 1);
									} else {
										obj_id = cursor.getLong(i);
									}
									values.put(_obj, obj_id);
									ro_list.add(new RowObject(tbl, _id, obj_id,
											0));

									// increment open counter for obj_id
									objRow.put(obj_id, _id);
									newOpen++;
									break;
								}
							}
						}
					}
				}
			}
			// set obj_id in any order
			else {
				for (Map.Entry<String, Object> s : values.valueSet()) {
					_obj = s.getKey();
					if (_obj.startsWith(OBJTAG)) {
						// get obj_id of the object column
						for (int i = 2; i < cursor.getColumnCount(); i++) {
							if (_obj.equals(cursor.getColumnName(i))) {
								// YGO: get new obj_id if there is no object
								// value or is a strong consistency
								if (cursor.isNull(i)
										|| consistencyLevel == SimbaConsistency.Type.STRONG) {
									obj_id = SimbaLevelDB.getMaxObjID();
									SimbaLevelDB.setMaxObjID(obj_id + 1);
								} else {
									obj_id = cursor.getLong(i);
								}
								values.put(_obj, obj_id);
								ro_list.add(new RowObject(tbl, _id, obj_id, 0));

								// increment open counter for obj_id
								objRow.put(obj_id, _id);
								newOpen++;
								break;
							}
						}
					}
				}
			}
			if (newOpen > 0) {
				Log.d(TAG, "Incrementing _openObj value for update row!"
						+ " count: " + openObj + " -> " + (openObj + newOpen));
				values.put("_openObj", openObj + newOpen);
			}
			if (consistencyLevel != SimbaConsistency.Type.STRONG) {
				// update to local table only if not a strong consistency
				updateInternal(values, sels, selArgs);
			} else {
				// YGO: temporarily write values into memory
				Log.d(TAG, "Storing values temporarily into memory!");
				syncSet = new SyncSet(_id, values, Boolean.FALSE);
			}
		}

		return ro_list;
	}

	// this is public but it is only intended to be used from within Simba, not
	// from the client-facing API
	public int updateInternal(ContentValues values, String sels,
			String[] selArgs) {
		// mark _dirty field for target rows if it's not for _dirtyObj field
		if (!values.containsKey("_dirtyObj")) {
			values.put("_dirty", Boolean.TRUE);
		}

		return db.update(tbl, values, sels, selArgs);
	}

	public int updateWithoutDirty(ContentValues values, String sels,
			String[] selArgs) {
		return db.update(tbl, values, sels, selArgs);
	}

	public int updateDelete(ContentValues values, String sels, String[] selArgs) {
		return db.update(TBL_DEL, values, sels, selArgs);
	}

	/* YGO: store obj_id, length mapping to SyncSet */
	public void truncate(long obj_id, int length) {
		String row_id = syncSet.getRowId();
		ContentValues cv_syncSet = syncSet.getContentValues();

		Cursor cursor = db.query(tbl, user_cols, "_id = ?",
				new String[] { row_id }, null, null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			for (int i = 0; i < user_cols.length; i++) {
				// find column that SyncSet's obj_id corresponds to
				if (cursor.getColumnName(i).startsWith(OBJTAG)
						&& cv_syncSet.getAsLong(cursor.getColumnName(i)) == obj_id) {
					// get original obj_id
					long fromObj = cursor.getLong(i);

					// copy
					SimbaLevelDB.truncateStrong(fromObj, obj_id, length);
					break;
				}
			}

		}

		syncSet.setObjLen(obj_id, length);
	}

	public int delete(String sels, String[] selArgs) throws RemoteException {
		if (inCR)
			throw new RemoteException("In CR");
		// String s = "";
		// for (String b: selArgs)
		// s += b + " " ;
		// Log.v(TAG, "got delete sels=" + sels + "args=" + s);

		String[] projs = new String[user_cols.length + 6];
		projs[0] = "_id";
		projs[1] = "_rev";
		projs[2] = "_sync";
		projs[3] = "_conflict";
		projs[4] = "_dirty";
		projs[5] = "_dirtyObj";
		System.arraycopy(user_cols, 0, projs, 6, user_cols.length);

		// 0. lock db
		db.beginTransaction();

		// 1. before query: do a select first to get target rows
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(tbl);
		Cursor cursor = qb.query(db, projs, sels, selArgs, null, null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			do {
				String id = cursor.getString(0);
				int rev = cursor.getInt(1);
				boolean sync = (cursor.getInt(2) == 1);
				// Log.v(TAG, "Found row to del rev=" + rev);

				if ((rev > DEFAULT_VERSION) || sync) {
					ContentValues cv = new ContentValues();
					cv.put("_id", id);
					cv.put("_rev", rev);
					cv.put("_sync", sync);
					cv.put("_conflict", cursor.getInt(3));
					cv.put("_dirty", cursor.getInt(4));
					cv.put("_dirtyObj", cursor.getInt(5));
					for (int i = 6; i < cursor.getColumnCount(); i++) {
						// column data
						if (!cursor.getColumnName(i).startsWith(OBJTAG)) {
							cv.put(cursor.getColumnName(i), cursor.getString(i));
						}
						// object
						else {
							cv.put(cursor.getColumnName(i), cursor.getLong(i));
						}
					}
					Log.d(TAG, "Moving row: " + id + " to TBL_DEL!");
					if (consistencyLevel != SimbaConsistency.Type.STRONG) {
						// write to delete table only if not a strong
						// consistency
						db.insert(TBL_DEL, null, cv);
					} else {
						// temporarily write values into memory
						syncSet = new SyncSet(id, cv, Boolean.TRUE);
					}
				}
			} while (cursor.moveToNext());
		}
		cursor.close();

		// 2. actual query
		int num_rows = 0;
		if (consistencyLevel != SimbaConsistency.Type.STRONG) {
			// delete from local table only if not a strong consistency
			num_rows = db.delete(tbl, sels, selArgs);
		} else {
			// YGO: operate with only one row at a time for strong consistency!
			num_rows = 1;
		}

		// 3. unlock db
		db.setTransactionSuccessful();
		db.endTransaction();

		return num_rows;
	}

	// remove deleted rows permanently
	public int purge(String sels, String[] selArgs, boolean fromTblDel) {
		String[] projs = new String[user_cols.length + 1];
		projs[0] = "_id";
		System.arraycopy(user_cols, 0, projs, 1, user_cols.length);

		Cursor cursor = null;
		if (fromTblDel) {
			cursor = db.query(TBL_DEL, projs, sels, selArgs, null, null, null);
		} else {
			// YGO: get row in local table
			cursor = db.query(tbl, projs, sels, selArgs, null, null, null);
		}

		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			do {
				for (int i = 1; i < cursor.getColumnCount(); i++) {
					// delete objects
					if (cursor.getColumnName(i).startsWith(OBJTAG)
							&& !cursor.isNull(i)) {
						SimbaChunkList.removeDirtyChunks(cursor.getLong(i));
						SimbaLevelDB.deleteObject(cursor.getLong(i), 0);
					}
				}
				Log.d(TAG, "DELETE row: " + cursor.getString(0));
			} while (cursor.moveToNext());
		}
		if (fromTblDel) {
			return db.delete(TBL_DEL, sels, selArgs);
		} else {
			// YGO: delete row in local table
			return db.delete(tbl, sels, selArgs);
		}
	}

	// insert a row to dirtyChunkList table
	public long insertDirtyChunkList(ContentValues values) {
		return db.insert(TBL_DIRTY_CHUNK_LIST, null, values);
	}

	// get all rows from dirtyChunkList table
	public Cursor getDirtyChunkList() {
		Cursor cursor = db.rawQuery("SELECT * FROM " + TBL_DIRTY_CHUNK_LIST,
				null);
		return cursor;
	}

	// delete all rows in dirtyChunkList table
	public void deleteDirtyChunkList() {
		db.delete(TBL_DIRTY_CHUNK_LIST, null, null);
	}

	/**
	 * Find out columns that function as primary key.
	 * 
	 * @return list of column names
	 */
	private List<String> get_primary_keys() {

		db.beginTransaction();
		Cursor cursor = db.rawQuery("PRAGMA table_info(" + tbl + ");", null);
		// Log.d("SimbaTable", "PK: PRAGMA table_info with column count " +
		// cursor.getColumnCount() + " row count : " + cursor.getCount() +
		// " Tbl:" + tbl);
		db.endTransaction();
		List<String> ret = new ArrayList<String>();

		assert cursor.moveToFirst() : "cursor.movetofirst empty";
		cursor.moveToFirst();
		do {
			String column = cursor.getString(1); /* 1 is "column name" */
			int flag = cursor.getInt(5); /* 5 is "primary_key" */
			if (flag == 1) {
				ret.add(column);
			}
			// Log.d("SimbaTable", "get_primary_keys flags: "+ flag);

		} while (cursor.moveToNext());
		cursor.close();

		return ret;
	}

	/**
	 * Find out all user-defined columns. i.e. except _id, _rev, _dirty
	 * 
	 * @param db
	 *            input database
	 * @param tbl
	 *            input table name
	 * @return String array of column names
	 */
	public String[] getUserColumns() {
		String[] projs;

		if (this.user_cols != null && this.user_cols.length > 0) {
			projs = this.user_cols;
		} else {
			db.beginTransaction();
			// db.beginTransactionNonExclusive();
			Cursor cursor = db
					.rawQuery("PRAGMA table_info(" + tbl + ");", null);
			Log.d("SimbaTable",
					"UserColumns: PRAGMA table_info with column count "
							+ cursor.getColumnCount() + " row count : "
							+ cursor.getCount() + " Tbl:" + tbl);
			db.endTransaction();

			assert cursor.getCount() > 0 : "getUserColumns Cursor size < 0";
			projs = new String[cursor.getCount() - meta_fields.size()];
			String column;
			int i = 0;

			cursor.moveToFirst();
			do {
				column = cursor.getString(1); /* 1 is "column name" */
				if (!meta_fields.contains(column)) {
					projs[i++] = column;
				}
			} while (cursor.moveToNext());
			cursor.close();
		}
		return projs;
	}

	/**
	 * Grab dirty rows for only user-defined fields. This operation may change
	 * the state of database and should be called only once. After this
	 * operation, dirty rows will be returned as a Cursor and there will be no
	 * dirty rows in the database.
	 * 
	 * @param db
	 *            input database
	 * @param tbl
	 *            input table name
	 * @return Cursor for result set
	 */
	public Cursor getDirtyRows() {
		String[] projs = new String[user_cols.length + 4];
		projs[0] = "_id";
		projs[1] = "_rev";
		projs[2] = "_dirty";
		projs[3] = "_dirtyObj";
		System.arraycopy(user_cols, 0, projs, 4, user_cols.length);

		Cursor cursor = null;
		// if any row is in sync pending, block next sync
		cursor = db.query(tbl, projs, "_sync = ?", new String[] { "1" }, null,
				null, null);
		if (cursor.getCount() > 0) {
			return null;
		}

		db.beginTransaction();
		try {
			// get dirty rows with no object opened and not torn row
			cursor = db.query(tbl, projs,
					"_torn = ? AND (_dirty = ? OR _dirtyObj = ?) AND "
							+ "	_sync = ? AND _conflict = ? AND _openObj = ?",
					new String[] { "0", "1", "1", "0", "0", "0" }, null, null,
					null);
			if (cursor.getCount() > 0) {
				// store dirtyChunkList in a persistent storage before
				// setting flags
				deleteDirtyChunkList();
				SimbaChunkList.storeDirtyChunkList(app, tbl);
				cursor.moveToFirst();
				do {
					String id = cursor.getString(0);
					ContentValues values = new ContentValues();
					if (cursor.getInt(2) == 1) {
						values.put("_dirty", Boolean.FALSE);
					}
					if (cursor.getInt(3) == 1) {
						values.put("_dirtyObj", Boolean.FALSE);
					}
					values.put("_sync", Boolean.TRUE);
					db.update(tbl, values, "_id = ?", new String[] { id });
				} while (cursor.moveToNext());
			}
			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}

		return cursor;
	}

	// can only be called once to get deleted rows
	public Vector<DataRow> getDeletedRows() {
		Vector<DataRow> ret = new Vector<DataRow>();
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(TBL_DEL);
		ContentValues values = new ContentValues();
		values.put("_sync", Boolean.TRUE);

		db.beginTransaction();

		try {
			Cursor cursor = qb.query(db, new String[] { "_id", "_rev" },
					"_sync=? AND _conflict=?", new String[] { "0", "0" }, null,
					null, null);
			if (cursor.getCount() > 0) {
				cursor.moveToFirst();
				do {
					String id = cursor.getString(0);
					int rev = cursor.getInt(1);
					Log.v(TAG, "Adding deleted row=" + id + ", rev=" + rev);
					ret.add(DataRow.newBuilder().setId(id).setRev(rev).build());
				} while (cursor.moveToNext());
				db.update(TBL_DEL, values, "_sync = ? AND _conflict = ?",
						new String[] { "0", "0" });
			}
			db.setTransactionSuccessful();
		} finally {
			db.endTransaction();
		}

		return ret;
	}

	public TornRowRequest buildDataForRecover() {
		TornRowRequest.Builder trr = TornRowRequest.newBuilder()
				.setApp(getAppId()).setTbl(getTblId());

		Cursor cursor = db.query(tbl, new String[] { "_id" }, "_torn = ?",
				new String[] { "1" }, null, null, null);
		if (cursor.getCount() > 0) {
			this.isRecovering = true;
			cursor.moveToFirst();
			do {
				String id = cursor.getString(0);
				trr.addElementId(id);
			} while (cursor.moveToNext());
		}
		cursor.close();

		return trr.build();
	}

	public SyncHeader buildDataForSyncing(Map<Integer, Long> obj_list) {
		int trans_id = SeqNumManager.getSeq();

		SyncHeader.Builder sds = SyncHeader.newBuilder().setApp(getAppId())
				.setTbl(getTblId()).setTrans_id(trans_id);

		Vector<DataRow> del_rows;

		/*
		 * TODO: one shot sync doesn't wait for a response, so we don't know
		 * whether the other side knows about the table or not Right now, the
		 * first one shot sync will just fail and set the schema sync pending
		 * flag
		 */
		/*
		 * if (schemaSyncPending()) { sds.setSchemaSql(getSchemaSQL());
		 * setSchemaSyncPending(false); }
		 */

		Cursor cursor = getDirtyRows();
		if (cursor != null && cursor.getCount() > 0) {
			cursor.moveToFirst();
			int oid = 0;
			do {
				// 0 = _id, 1 = _rev, 2 = _dirty, 3 = _dirtyObj
				String id = cursor.getString(0);
				int rev = cursor.getInt(1);
				boolean isRowDirty = (cursor.getInt(2) == 1);
				boolean isObjDirty = (cursor.getInt(3) == 1);
				boolean foundDirtyObject = false;

				DataRow.Builder row = DataRow.newBuilder().setId(id)
						.setRev(rev);
				String[] cols = cursor.getColumnNames();

				for (int i = 4; i < cursor.getColumnCount(); i++) {
					if (!cols[i].startsWith(OBJTAG)) { // column data
						// if column data is dirty or rev is -1, send all
						// columns
						if (isRowDirty || rev == -1) {
							ColumnData.Builder c = ColumnData.newBuilder();
							if (cursor.getType(i) == 3) { // string
								String str = "'" + cursor.getString(i) + "'";
								c.setColumn(cols[i]).setValue(str);
							} else if (cursor.getType(i) != 0
									&& cursor.getType(i) != 4) { // int/float/blob
								String str = cursor.getString(i);
								c.setColumn(cols[i]).setValue(str);
							} else {
								Log.d(TAG, "Unsupported type!");
								assert false;
							}
							row.addElementData(c.build());
						}
					} else { // object
						if (isObjDirty) {
							// find whether the object has dirty chunks
							if (SimbaChunkList.getDirtyChunks(cursor
									.getLong(i)) != null) {
								Log.d(TAG,
										"Adding object into ObjectHeader! [column: "
												+ cols[i] + ", object: "
												+ cursor.getLong(i) + "]");
								foundDirtyObject = true;

								ObjectHeader.Builder oh = ObjectHeader
										.newBuilder();
								oh.setColumn(cols[i])
										.setOid(oid)
										.setNum_chunks(
												SimbaLevelDB
														.getNumChunks(cursor
																.getLong(i)));
								row.addElementObj(oh.build());

								/* insert "oid & object id" into object list */
								obj_list.put(oid, cursor.getLong(i));
								oid++;
							}
						}
					}
				}
				// if _dirtyObj is true but cannot find any dirty chunk,
				// send all objects
				if (isObjDirty && !foundDirtyObject) {
					for (int i = 4; i < cursor.getColumnCount(); i++) {
						if (cols[i].startsWith(OBJTAG)) {
							Log.d(TAG,
									"Adding object into ObjectHeader! [column: "
											+ cols[i]
											+ ", object: "
											+ cursor.getLong(i)
											+ ", num_chunk: "
											+ SimbaLevelDB.getNumChunks(cursor
													.getLong(i)) + "]");

							ObjectHeader.Builder oh = ObjectHeader.newBuilder();
							oh.setColumn(cols[i])
									.setOid(oid)
									.setNum_chunks(
											SimbaLevelDB.getNumChunks(cursor
													.getLong(i)));
							row.addElementObj(oh.build());

							/* insert "oid & object id" into object list */
							obj_list.put(oid, cursor.getLong(i));
							oid++;
						}
					}
				}

				sds.addElementDirtyRows(row.build());
			} while (cursor.moveToNext());
			cursor.close();
		} else {
			/* cursor is empty */
		}

		del_rows = getDeletedRows();
		if (!del_rows.isEmpty()) {
			// Log.v(TAG, "del_rows size=" + del_rows.size());
			sds.setDeletedRows(del_rows);
		} else {
			/* no deleted rows */
		}

		return sds.build();
	}

	/* YGO: build SyncHeader for strong consistency */
	public SyncHeader buildDataForSyncingStrong(Map<Integer, Long> obj_list) {
		// there was update while the client was writing
		if (syncSet.isUpdated()) {
			// delete objects in SyncSet
			ContentValues cv = syncSet.getContentValues();
			for (int i = 0; i < user_cols.length; i++) {
				if (cv.containsKey(user_cols[i])
						&& user_cols[i].startsWith(OBJTAG)) {
					SimbaLevelDB.deleteObject(cv.getAsLong(user_cols[i]), 0);
				}
			}

			// reset SyncSet to null
			syncSet = null;
			return null;
		}

		int trans_id = SeqNumManager.getSeq();

		SyncHeader.Builder sds = SyncHeader.newBuilder().setApp(getAppId())
				.setTbl(getTblId()).setTrans_id(trans_id);

		if (!syncSet.isDelete()) {
			// syncing for update
			int oid = 0;
			ContentValues cv = syncSet.getContentValues();
			//Log.d(TAG, "syncset cv=" + cv);

			String[] projs = new String[user_cols.length + 1];
			projs[0] = "_rev";
			System.arraycopy(user_cols, 0, projs, 1, user_cols.length);

			String row_id = syncSet.getRowId();
			Cursor cursor = db.query(tbl, projs, "_id = ?",
					new String[] { row_id }, null, null, null);
			if (cursor.getCount() == 0) {
				// write! rev set to -1 as default
				DataRow.Builder row = DataRow.newBuilder().setId(row_id)
						.setRev(-1);
				String[] columns = schema_sql.split(("\\s+"));
				for (int i = 0; i < columns.length; i += 2) {
					if (cv.containsKey(columns[i])) {
						// column data
						if (!columns[i].startsWith(OBJTAG)) {
							ColumnData.Builder c = ColumnData.newBuilder();
							if (columns[i + 1].startsWith("VARCHAR")) {
								String str = "'" + cv.getAsString(columns[i])
										+ "'";
								c.setColumn(columns[i]).setValue(str);
							} else if (columns[i + 1].startsWith("INT")) {
								String str = cv.getAsString(columns[i]);
								c.setColumn(columns[i]).setValue(str);
							} else {
								Log.d(TAG, "Unsupported type!");
								assert false;
							}

							row.addElementData(c.build());
						}
						// object data
						else {
							// if not truncated, obj_len is not set (-1)
							Long obj_id = cv.getAsLong(columns[i]);
							int obj_len = syncSet.getObjLen(obj_id);
							if (obj_len == -1) {
								obj_len = SimbaLevelDB.getNumChunks(obj_id);
							}

							ObjectHeader.Builder oh = ObjectHeader.newBuilder();
							oh.setColumn(columns[i]).setOid(oid)
									.setNum_chunks(obj_len);
							row.addElementObj(oh.build());
							obj_list.put(oid, cv.getAsLong(columns[i]));
							oid++;
						}
					}
				}
				sds.addElementDirtyRows(row.build());
			} else {
				// update! get rev from local row
				cursor.moveToFirst();
				int rev = cursor.getInt(0);
				DataRow.Builder row = DataRow.newBuilder().setId(row_id)
						.setRev(rev);
				String[] columns = cursor.getColumnNames();

				for (int i = 1; i < cursor.getColumnCount(); i++) {
					//Log.d(TAG, "column[i]=" + columns[i]);
					if (cv.containsKey(columns[i])) {
						// column data
						if (!columns[i].startsWith(OBJTAG)) {
							ColumnData.Builder c = ColumnData.newBuilder();
							if (cursor.getType(i) == 3) { // string
								String str = "'" + cv.getAsString(columns[i])
										+ "'";
								c.setColumn(columns[i]).setValue(str);
							} else if (cursor.getType(i) != 0
									&& cursor.getType(i) != 4) { // int/float/blob
								String str = cv.getAsString(columns[i]);
								c.setColumn(columns[i]).setValue(str);
							} else {
								Log.d(TAG, "Unsupported type!");
								assert false;
							}
							row.addElementData(c.build());
						}
						// object data
						else {
							// if not truncated, obj_len is not set! (-1)
							Long obj_id = cv.getAsLong(columns[i]);
							int obj_len = syncSet.getObjLen(obj_id);
							if (obj_len == -1) {
								// update: find largest obj_len in case update
								// increased length
								if (SimbaLevelDB.getNumChunks(cursor
										.getLong(i)) >= SimbaLevelDB
										.getNumChunks(obj_id)) {
									obj_len = SimbaLevelDB.getNumChunks(cursor
											.getLong(i));
								}
							}

							ObjectHeader.Builder oh = ObjectHeader.newBuilder();
							oh.setColumn(columns[i]).setOid(oid)
									.setNum_chunks(obj_len);
							row.addElementObj(oh.build());
							obj_list.put(oid, cv.getAsLong(columns[i]));
							oid++;
						}
					}
				}
				// set _sync flag
				ContentValues values = new ContentValues();
				values.put("_sync", Boolean.TRUE);
				db.update(tbl, values, "_id = ?", new String[] { row_id });

				sds.addElementDirtyRows(row.build());
			}
			cursor.close();
		} else {
			// syncing for delete
			String row_id = syncSet.getRowId();
			Cursor cursor = db.query(tbl, new String[] { "_rev" }, "_id = ?",
					new String[] { row_id }, null, null, null);
			if (cursor.getCount() > 0) {
				cursor.moveToFirst();
				int rev = cursor.getInt(0);
				Vector<DataRow> del_row = new Vector<DataRow>();
				del_row.add(DataRow.newBuilder().setId(row_id).setRev(rev)
						.build());
				sds.setDeletedRows(del_row);
			}
			cursor.close();
		}

		return sds.build();
	}

	public boolean getCR() {
		return inCR;
	}

	public void beginCR() throws RemoteException {
		if (inCR)
			throw new RemoteException("Already in CR");
		inCR = true;
	}

	public void endCR() throws RemoteException {
		if (!inCR)
			throw new RemoteException("Not in CR");
		inCR = false;
	}

	public List<InternalDataRow> getConflictedRows() throws RemoteException {
		if (!inCR)
			throw new RemoteException("Not in CR");
		List<InternalDataRow> ret = new ArrayList<InternalDataRow>();

		// 1. go through normal table
		String[] projs = new String[user_cols.length + 1];
		projs[0] = "_id";
		System.arraycopy(user_cols, 0, projs, 1, user_cols.length);

		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(tbl);
		Cursor cursor = qb.query(db, projs, "_conflict = ?",
				new String[] { "1" }, null, null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			do {
				String id = cursor.getString(0);

				// check if row in TBL_CONFLICT is not in the middle of update
				InternalDataRow server_copy = get_server_copy(id);
				if (server_copy != null) {
					Log.d(TAG, "Get conflicted row from local table! row=" + id);
					List<String> columnData = new ArrayList<String>();
					List<Long> objectData = new ArrayList<Long>();
					for (int i = 1; i < projs.length; i++) {
						if (!cursor.getColumnName(i).startsWith(OBJTAG)) {
							columnData.add(cursor.getString(i));
						} else {
							objectData.add(cursor.getLong(i));
						}
					}
					ret.add(new InternalDataRow(id, columnData, objectData,
							false));
					ret.add(server_copy);
				} else {
					Log.d(TAG, "Row is currently in update! row=" + id);
				}
			} while (cursor.moveToNext());
		}
		cursor.close();

		// 2. go through deleted table
		qb.setTables(TBL_DEL);
		cursor = qb.query(db, projs, "_conflict = ?", new String[] { "1" },
				null, null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			do {
				String id = cursor.getString(0);

				// check if row in TBL_CONFLICT is not in the middle of update
				InternalDataRow server_copy = get_server_copy(id);
				if (server_copy != null) {
					Log.d(TAG, "Get conflicted row from delete table! row="
							+ id);
					List<String> columnData = new ArrayList<String>();
					List<Long> objectData = new ArrayList<Long>();
					for (int i = 1; i < cursor.getColumnCount(); i++) {
						if (!cursor.getColumnName(i).startsWith(OBJTAG)) {
							columnData.add(cursor.getString(i));
						} else {
							objectData.add(cursor.getLong(i));
						}
					}
					ret.add(new InternalDataRow(id, columnData, objectData,
							true));
					ret.add(server_copy);
				} else {
					Log.d(TAG, "Row is currently in update! row=" + id);
				}
			} while (cursor.moveToNext());
		}
		cursor.close();

		// 3. return
		return ret;
	}

	private InternalDataRow get_server_copy(String id) {
		InternalDataRow ret = null;

		String[] projs = new String[user_cols.length + 1];
		projs[0] = "_rev";
		System.arraycopy(user_cols, 0, projs, 1, user_cols.length);

		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(TBL_CONFLICT);
		Cursor cursor = qb.query(db, projs, "_id = ? AND _sync = ?",
				new String[] { id, "0" }, null, null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			int rev = cursor.getInt(0);

			List<String> columnData = new ArrayList<String>();
			List<Long> objectData = new ArrayList<Long>();
			if (rev >= 0) {
				for (int i = 1; i < projs.length; i++) {
					if (!cursor.getColumnName(i).startsWith(OBJTAG)) {
						columnData.add(cursor.getString(i));
					} else {
						objectData.add(cursor.getLong(i));
					}
				}
			} else {
				/* server copy exists but it is NULL */
			}
			ret = new InternalDataRow(id, columnData, objectData, false);
		}
		cursor.close();

		return ret;
	}

	private int get_server_rev(String id) {
		int ret = this.DEFAULT_VERSION;
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(TBL_CONFLICT);
		Cursor cursor = qb.query(db, new String[] { "_rev" }, "_id=?",
				new String[] { id }, null, null, null);

		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			ret = cursor.getInt(0);
		}
		cursor.close();

		return ret;
	}

	private void delete_server_copy(String id) {
		Log.d(TAG, "Deleting server copy");

		// delete objects that the server sent
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(TBL_CONFLICT);
		Cursor cursor = qb.query(db, user_cols, "_id=?", new String[] { id },
				null, null, null);
		if (cursor.getCount() > 0) {
			cursor.moveToFirst();
			do {
				for (int i = 2; i < cursor.getColumnCount(); i++) {
					if (cursor.getColumnName(i).startsWith(OBJTAG)
							&& !cursor.isNull(i)) {
						SimbaLevelDB.deleteDirtyObject(cursor.getLong(i));
					}
				}
			} while (cursor.moveToNext());
		}

		db.delete(TBL_CONFLICT, "_id=?", new String[] { id });
	}

	public void resolveConflict(String id, CRChoice choice)
			throws RemoteException {
		if (!inCR)
			throw new RemoteException("Not in CR");
		int svr_rev = this.get_server_rev(id);
		int num_rows = 0;
		ContentValues cv = new ContentValues();

		switch (choice) {
		case MINE:
			// update local revision, delete server copy
			this.delete_server_copy(id);

			cv.put("_rev", svr_rev);
			cv.put("_conflict", Boolean.FALSE);

			// if row was deleted in server-side, set entire row to be dirty
			if (svr_rev == -1) {
				cv.put("_dirty", Boolean.TRUE);
				cv.put("_dirtyObj", Boolean.TRUE);
				Cursor cursor = db.query(tbl, null, "_id = ?",
						new String[] { id }, null, null, null);
				if (cursor.getCount() > 0) {
					cursor.moveToFirst();
					for (int i = 0; i < cursor.getColumnCount(); i++) {
						if (cursor.getColumnName(i).startsWith(OBJTAG)) {
							SimbaLevelDB.setObjectDirty(cursor.getLong(i));
						}
					}
				}
			}
			num_rows = db.update(tbl, cv, "_id=?", new String[] { id });

			// local row is in deleted table
			if (num_rows == 0) {
				if (svr_rev >= 0) {
					Log.d(TAG, "Set deleted row ver to " + svr_rev);
					db.update(TBL_DEL, cv, "_id=?", new String[] { id });
				} else {
					purge("_id=?", new String[] { id }, Boolean.TRUE);
				}
			}
			break;
		case SERVER:
			if (svr_rev >= 0) {
				// delete obsolete objects in leveldb
				SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
				qb.setTables(tbl);
				Cursor cursor = qb.query(db, user_cols, "_id=?",
						new String[] { id }, null, null, null);
				if (cursor.getCount() == 0) {
					// row is in TBL_DEL
					Log.d(TAG,
							"Row: "
									+ id
									+ " is in delete table! Moving it back to local table");

					String[] projs = new String[user_cols.length + 6];
					projs[0] = "_id";
					projs[1] = "_rev";
					projs[2] = "_sync";
					projs[3] = "_conflict";
					projs[4] = "_dirty";
					projs[5] = "_dirtyObj";
					System.arraycopy(user_cols, 0, projs, 6, user_cols.length);

					qb.setTables(TBL_DEL);
					cursor = qb.query(db, projs, "_id=?", new String[] { id },
							null, null, null);
					assert cursor.getCount() > 0;
					if (cursor.getCount() > 0) {
						cursor.moveToFirst();
						cv.put("_id", id);
						cv.put("_rev", cursor.getInt(1));
						cv.put("_sync", cursor.getInt(2));
						cv.put("_conflict", cursor.getInt(3));
						cv.put("_dirty", cursor.getInt(4));
						cv.put("_dirtyObj", cursor.getInt(5));
						for (int i = 6; i < cursor.getColumnCount(); i++) {
							// column data
							if (!cursor.getColumnName(i).startsWith(OBJTAG)) {
								cv.put(cursor.getColumnName(i),
										cursor.getString(i));
							}
							// object
							else {
								cv.put(cursor.getColumnName(i),
										cursor.getLong(i));
							}
						}
						db.insert(tbl, null, cv);
						db.delete(TBL_DEL, "_id=?", new String[] { id });
						cv.clear();
					}
					// search for the row again
					qb.setTables(tbl);
					cursor = qb.query(db, user_cols, "_id=?",
							new String[] { id }, null, null, null);
				}
				if (cursor.getCount() > 0) {
					cursor.moveToFirst();
					do {
						for (int i = 0; i < cursor.getColumnCount(); i++) {
							if (cursor.getColumnName(i).startsWith(OBJTAG)) {
								qb = new SQLiteQueryBuilder();
								qb.setTables(TBL_CONFLICT);
								Cursor srv_cursor = qb.query(db, new String[] {
										cursor.getColumnName(i),
										"count" + cursor.getColumnName(i) },
										"_id = ?", new String[] { id }, null,
										null, null);
								if (srv_cursor.getCount() > 0) {
									srv_cursor.moveToFirst();
									if (!srv_cursor.isNull(0)) {

										if (!srv_cursor.isNull(1)) {
											int num_chunks = srv_cursor
													.getInt(1);
											// truncate the size accordingly
											ReadOptions ro = SimbaLevelDB
													.takeSnapshot();
											SimbaLevelDB
													.truncate(
															cursor.getLong(i),
															(num_chunks == 0 ? 0
																	: num_chunks - 1)
																	* SimbaLevelDB
																			.getChunkSize()
																	+ SimbaLevelDB
																			.getChunk(
																					ro,
																					srv_cursor
																							.getLong(0),
																					num_chunks - 1).length,
															false);
											SimbaLevelDB.closeSnapshot(ro);
										}
										SimbaLevelDB.updateObject(
												srv_cursor.getLong(0),
												cursor.getLong(i));
									}
								}
							}
						}
					} while (cursor.moveToNext());
				}
			}

			// delete row
			if (svr_rev < 0) {
				SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
				qb.setTables(tbl);
				Cursor cursor = qb.query(db, user_cols, "_id=?",
						new String[] { id }, null, null, null);
				if (cursor.getCount() > 0) {
					cursor.moveToFirst();
					do {
						for (int i = 0; i < cursor.getColumnCount(); i++) {
							// delete objects
							if (cursor.getColumnName(i).startsWith(OBJTAG)
									&& !cursor.isNull(i)) {
								SimbaLevelDB
										.deleteObject(cursor.getLong(i), 0);
							}
						}
					} while (cursor.moveToNext());
				}
				num_rows = db.delete(tbl, "_id=?", new String[] { id });
				if (num_rows == 0) {
					purge("_id=?", new String[] { id }, Boolean.TRUE);
				}
			} else {
				// insert server copy into local table
				List<String> svr_data = this.get_server_copy(id)
						.getColumnData();

				for (int i = 0; i < svr_data.size(); i++) {
					if (svr_data.get(i) != null) {
						cv.put(user_cols[i], svr_data.get(i));
					}
				}

				cv.put("_rev", svr_rev);
				cv.put("_dirty", Boolean.FALSE);
				cv.put("_dirtyObj", Boolean.FALSE);
				cv.put("_conflict", Boolean.FALSE);

				num_rows = db.update(tbl, cv, "_id=?", new String[] { id });
				if (num_rows == 0) {
					// NOTE: Moving to local table is already done above!
					assert false;
					purge("_id=?", new String[] { id }, Boolean.TRUE);
					cv.put("_id", id);
					db.insert(tbl, null, cv);
				}
			}

			// delete server copy
			this.delete_server_copy(id);
			break;
		case IGNORE:
			break;
		}
	}

	public int processSyncResponse(List<DataRow> dirtyRows,
			List<DataRow> syncedRows, List<DataRow> conflictRows,
			Map<Integer, Long> obj_list) {
		int conflictedObjRows = 0;
		boolean isStrongConsistency = consistencyLevel == SimbaConsistency.Type.STRONG ? true
				: false;

		// 1. synced rows are easy to handle, just merge with local db
		for (DataRow row : syncedRows) {
			String id = row.getId();
			int server_rev = row.getRev();

			if (server_rev < 0) {
				if (isStrongConsistency) {
					/* YGO: purge from local table's row */
					purge("_id=?", new String[] { id }, Boolean.FALSE);
					Log.d(TAG, "Delete Reponse: "
							+ dateFormatter.format(System.currentTimeMillis()));
				} else {
					/* deleted row: purge from delete_list */
					purge("_id=?", new String[] { id }, Boolean.TRUE);
				}
			} else {
				/* dirty row: update _rev, clear _sync */
				ContentValues cv = new ContentValues();
				cv.put("_rev", server_rev);
				cv.put("_sync", Boolean.FALSE);

				int affected_rows = updateWithoutDirty(cv, "_id=?",
						new String[] { id });

				if (affected_rows < 1 && !isStrongConsistency) {
					cv = new ContentValues();
					cv.put("_rev", server_rev);
					cv.put("_sync", Boolean.FALSE);
					/* user might have deleted that row */
					updateDelete(cv, "_id=?", new String[] { id });
				}

				// find object ids to remove from dirtyChunkList
				Cursor cursor = db.query(tbl, user_cols, "_id = ?",
						new String[] { id }, null, null, null);
				// synced for update
				if (cursor.getCount() > 0) {
					cursor.moveToFirst();
					for (int i = 0; i < user_cols.length; i++) {
						/* YGO: move all data from SyncSet to local table */
						if (isStrongConsistency) {
							Log.d(TAG,
									"Strong consistency! Updating object to local obj_id");
							ContentValues cv_syncSet = syncSet
									.getContentValues();
							if (cv_syncSet.containsKey(cursor.getColumnName(i))
									&& cursor.getColumnName(i).startsWith(
											OBJTAG)) {
								if (syncSet.getObjLen(cv_syncSet
										.getAsLong(cursor.getColumnName(i))) > 0) {
									Log.d(TAG,
											"Replace obj_id to truncated object");
									// YGO: if truncated, switch the obj_id to
									// truncated object and delete data of
									// original obj_id
									SimbaLevelDB.deleteObject(
											cursor.getLong(i), 0);
								} else {
									SimbaLevelDB.updateObject(
											cv_syncSet.getAsLong(cursor
													.getColumnName(i)), cursor
													.getLong(i));
									// set obj_ids in cv_syncSet's value back to
									// original obj_id (we don't need to do
									// anything for column!)
									cv_syncSet.put(cursor.getColumnName(i),
											cursor.getLong(i));
								}
							}
							// set new content values into SyncSet
							syncSet.setContentValues(cv_syncSet);
						} else {
							// remove this object's dirty chunk list
							if (cursor.getColumnName(i).startsWith(OBJTAG)) {
								SimbaChunkList.removeDirtyChunks(cursor
										.getLong(i));
							}
						}
					}
					// YGO: update local table with new data for strong
					// consistency
					if (isStrongConsistency) {
						Log.d(TAG, "Synced for update with strong consistency!");
						ContentValues cv_syncSet = syncSet.getContentValues();
						updateWithoutDirty(cv_syncSet, "_id=?",
								new String[] { id });
						
						Log.d(TAG, "Update Reponse: "
								+ dateFormatter.format(System.currentTimeMillis()));
					}
				}
				// YGO: synced for first write with strong consistency
				else if (isStrongConsistency) {
					Log.d(TAG,
							"Synced for first write with strong consistency!");
					ContentValues cv_syncSet = syncSet.getContentValues();
					cv_syncSet.put("_rev", server_rev);
					cv_syncSet.put("_sync", Boolean.FALSE);
					cv_syncSet.put("_dirty", Boolean.FALSE);
					db.insert(tbl, null, cv_syncSet);

					for (int i = 0; i < user_cols.length; i++) {
						// YGO: remove dirty chunks for synced objects
						if (cv_syncSet.containsKey(user_cols[i])
								&& user_cols[i].startsWith(OBJTAG)) {
							SimbaChunkList.removeDirtyChunks(cv_syncSet
									.getAsLong(user_cols[i]));
						}
					}
					
					Log.d(TAG, "Sync Reponse: "
					+ dateFormatter.format(System.currentTimeMillis()));
				}
			}
		}

		for (DataRow mdr : conflictRows) {
			String id = mdr.getId();

			// 1. mark _conflict flag in local table
			// check which flag to mark dirty: row data or object
			ContentValues cv = new ContentValues();
			for (DataRow dr : dirtyRows) {
				if (mdr.getId().equals(dr.getId())) {
					if (isStrongConsistency) {
						cv.put("_sync", Boolean.FALSE);
					} else {
						if (mdr.getObj().size() > 0) {
							conflictedObjRows++;
						} else {
							// set flags only if there is no more message to
							// come
							cv.put("_conflict", Boolean.TRUE);
							cv.put("_sync", Boolean.FALSE);
						}

						// set dirty flags back
						if (dr.getData().size() > 0) {
							cv.put("_dirty", Boolean.TRUE);
						}
						if (dr.getObj().size() > 0) {
							cv.put("_dirtyObj", Boolean.TRUE);
						}
					}
					break;
				}
			}
			int affected_rows = 0;
			if (cv.size() > 0) {
				affected_rows = updateWithoutDirty(cv, "_id=?",
						new String[] { id });
			}
			if (affected_rows < 1) {
				cv = new ContentValues();
				cv.put("_conflict", Boolean.TRUE);
				cv.put("_sync", Boolean.FALSE);
				updateDelete(cv, "_id=?", new String[] { id });
			}
			// 2. store server rows in TBL_CONFLICT
			cv = new ContentValues();
			cv.put("_id", id);
			cv.put("_rev", mdr.getRev());
			Log.d(TAG,
					"Writing to TBL_CONFLICT! row=" + id + ", rev="
							+ mdr.getRev());

			// row data
			List<ColumnData> server_data = mdr.getData();
			for (int i = 0; i < server_data.size(); i++) {
				cv.put(server_data.get(i).getColumn(), server_data.get(i)
						.getValue());
			}

			// object data. set value to be new object id for leveldb
			List<ObjectHeader> server_obj = mdr.getObj();
			for (int i = 0; i < server_obj.size(); i++) {
				ObjectHeader oh = server_obj.get(i);
				// check if object size has been changed
				if (oh.hasNum_chunks()) {
					Log.d(TAG, "chunk_num: " + oh.getNum_chunks());
					cv.put("count" + oh.getColumn(), oh.getNum_chunks());
				}

				long obj_id = SimbaLevelDB.getMaxObjID();
				cv.put(oh.getColumn(), obj_id);
				obj_list.put(oh.getOid(), obj_id);
				SimbaLevelDB.setMaxObjID(obj_id + 1);
			}

			// YGO: if end of sync for strong consistency, merge to local row
			if (isStrongConsistency && server_obj.size() == 0) {
				updateWithoutDirty(cv, "_id=?", new String[] { id });
				Log.d(TAG, "Sync Reponse (end): "
						+ dateFormatter.format(System.currentTimeMillis()));
			} else {
				writeConflictCopy(cv);
			}
		}

		// YGO: reset SyncSet in case it is used for strong consistency
		syncSet = null;
		
		if (isStrongConsistency)
			kickSyncWaiter();

		return conflictedObjRows;
	}

	final Lock syncLock = new ReentrantLock();
	final Condition waitingForSyncResponse = syncLock.newCondition();
	
	private void kickSyncWaiter() {
		assert consistencyLevel == SimbaConsistency.Type.STRONG;
		syncLock.lock();
		Log.d(TAG, "kicking sync waiter");
		waitingForSyncResponse.signal();
		syncLock.unlock();
		
	}


	public void waitForSyncResponse() {
		Log.d(TAG, "blocking for sync response");
		assert consistencyLevel == SimbaConsistency.Type.STRONG;

		syncLock.lock();
		try {
			waitingForSyncResponse.await();
		} catch (InterruptedException e) {
			Log.d(TAG, "Interrupted while waiting for sync response");
		}finally {
			syncLock.unlock();
		}
		Log.d(TAG, "woken up after sync response");

	}

	private void handleConflict(String id, int rev,
			Vector<ColumnData> serverData,
			Vector<ObjectHeader> serverDataObject, Map<Integer, Long> obj_list,
			boolean conflictOnDeletedTable, boolean isDelete,
			boolean client_conflict) {
		ContentValues cv;

		// set _conflict in local table only if there is no object in the row
		if (serverDataObject.size() == 0) {
			cv = new ContentValues();
			cv.put("_conflict", Boolean.TRUE);
			cv.put("_sync", Boolean.FALSE);
			int affected_rows = conflictOnDeletedTable ? updateDelete(cv,
					"_id=?", new String[] { id }) : updateInternal(cv, "_id=?",
					new String[] { id });
			assert affected_rows == 1 : "Must be found";
		}

		// store server row in TBL_CONFLICT
		cv = new ContentValues();
		cv.put("_id", id);
		if (isDelete) {
			// set _rev to -1 if the row is deleted in server-side
			cv.put("_rev", -1);
		} else {
			cv.put("_rev", rev);
		}
		if (client_conflict) {
			cv.put("_sync", Boolean.TRUE);
		}

		if (serverData.size() > 0) {
			for (int i = 0; i < serverData.size(); i++) {
				ColumnData cd = serverData.get(i);
				cv.put(cd.getColumn(), cd.getValue());
			}
		}
		if (serverDataObject.size() > 0) {
			for (int i = 0; i < serverDataObject.size(); i++) {
				ObjectHeader oh = serverDataObject.get(i);
				// check if object size has been changed
				if (oh.hasNum_chunks()) {
					cv.put("count" + oh.getColumn(), oh.getNum_chunks());
				}

				long obj_id = SimbaLevelDB.getMaxObjID();
				cv.put(oh.getColumn(), obj_id);
				obj_list.put(oh.getOid(), obj_id);
				SimbaLevelDB.setMaxObjID(obj_id + 1);
			}
		}
		writeConflictCopy(cv);
	}

	/**
	 * This function in called when SCS receives data for a read sync. the
	 * return value indicates the number of new rows that were incorporated
	 * 
	 * @param dirtyRows
	 * @param deletedRows
	 * 
	 * @return array with new row and conflicted row count
	 */
	public int[] processSyncData(List<DataRow> dirtyRows,
			List<DataRow> deletedRows, Map<Integer, Long> obj_list,
			boolean fromNotificationPull) {
		// YGO: get row_id to check for conflict if SyncSet is currently being
		// used
		String row_id = "";
		if (syncSet != null) {
			row_id = syncSet.getRowId();
		}
		boolean isStrongConsistency = (consistencyLevel == SimbaConsistency.Type.STRONG) ? true
				: false;

		int newRows = 0;
		int conflictedRows = 0;
		int numDeletedRows = 0;

		// list of torn row ids
		List<String> torn_rows = new ArrayList<String>();
		if (!fromNotificationPull) {
			SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
			qb.setTables(tbl);
			Cursor cursor = qb.query(db, new String[] { "_id" }, "_torn=?",
					new String[] { "1" }, null, null, null);
			if (cursor.getCount() > 0) {
				cursor.moveToFirst();
				do {
					torn_rows.add(cursor.getString(0));
				} while (cursor.moveToNext());
			}
		}

		// 1. handle dirty rows
		for (DataRow mdr : dirtyRows) {
			String id = mdr.getId();
			int server_rev = mdr.getRev();
			int client_rev = -1;
			boolean client_torn = false;
			boolean client_dirty = false;
			boolean client_dirtyObj = false;
			boolean client_syncing = false;
			boolean client_conflict = false;
			int found = 0;

			SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
			qb.setTables(tbl);
			Cursor cursor = qb
					.query(db,
							new String[] { "_rev, _torn, _dirty, _dirtyObj, _sync, _conflict" },
							"_id=?", new String[] { id }, null, null, null);
			if (cursor.getCount() > 0) {
				cursor.moveToFirst();
				client_rev = cursor.getInt(0);
				client_torn = (cursor.getInt(1) == 1);
				client_dirty = (cursor.getInt(2) == 1);
				client_dirtyObj = (cursor.getInt(3) == 1);
				client_syncing = (cursor.getInt(4) == 1);
				client_conflict = (cursor.getInt(5) == 1);
				found = 1;
			}
			cursor.close();

			if (client_syncing) {
				Log.d(TAG, "Client is syncing this row right now");
				// ignore this row, it is an update for a row that is currently
				// being synced
				// we will handle this row when the sync completed
				continue;
			}

			// YGO: if updating for row currently being written with strong
			// consistency, set isUpdated = true
			if (syncSet != null && id.equals(row_id)) {
				syncSet.setUpdated();
			}

			if (found == 0) {
				assert (!isStrongConsistency);
				qb = new SQLiteQueryBuilder();
				qb.setTables(TBL_DEL);
				cursor = qb.query(db, new String[] { "_rev", "_sync" },
						"_id=? ", new String[] { id }, null, null, null);
				if (cursor.getCount() > 0) {
					cursor.moveToFirst();
					if (fromNotificationPull && cursor.getInt(1) == 1) {
						/* DELETE ROW. Ignore this row */
						Log.d(TAG, "DELETE ROW in sync. Client: " + client_rev
								+ ", Server: " + server_rev);
						List<ObjectHeader> oh_list = mdr.getObj();
						for (ObjectHeader oh : oh_list) {
							Log.d(TAG, "Object oid[" + oh.getOid()
									+ "] will be dropped");
						}
						continue;
					}
					client_rev = cursor.getInt(0);
					client_dirty = true;
					found = 2;
				}
				cursor.close();
			}

			if (found != 0) {
				// if this is a torn row, ignore it
				if (client_torn && fromNotificationPull) {
					Log.d(TAG, "TORN ROW. Client: " + client_rev + ", Server: "
							+ server_rev);
					List<ObjectHeader> oh_list = mdr.getObj();
					for (ObjectHeader oh : oh_list) {
						Log.d(TAG, "Object oid[" + oh.getOid()
								+ "] will be dropped");
					}
					if (oh_list.size() == 0) {
						// set tbl_rev to server's rev
						setTblRev(server_rev);
					}
					continue;
				}
				if (fromNotificationPull && client_rev < server_rev) {
					if (client_dirty || client_dirtyObj) {
						/* CONFLICT */
						assert (fromNotificationPull == true);
						assert (!isStrongConsistency);

						// if a row to update is used at conflict resolution,
						// ignore all PullData messages from this row
						if (client_conflict && inCR) {
							Log.d(TAG, "This row is in CR! row=" + id);
							stopPullData = true;

							return new int[] { newRows, conflictedRows,
									numDeletedRows };
						}

						handleConflict(id, server_rev, mdr.getData(),
								mdr.getObj(), obj_list, found == 2, false,
								client_conflict);
						if (mdr.getObj().size() == 0) {
							Log.d(TAG, "CONFLICT row without objects");
							conflictedRows++;

							// set tbl_rev to server's rev
							setTblRev(server_rev);
						} else {
							Log.d(TAG, "CONFLICT row with objects");
						}
					} else {
						// update only if there is no object in the server row
						if (mdr.getObj().size() == 0) {
							Log.d(TAG, "UPDATE row without objects");
							/* UPDATE */
							ContentValues cv = new ContentValues();
							cv.put("_id", id);
							cv.put("_rev", mdr.getRev());
							cv.put("_dirty", Boolean.FALSE);
							if (client_torn) {
								cv.put("_torn", Boolean.FALSE);
							}

							List<ColumnData> server_data = mdr.getData();
							for (int i = 0; i < server_data.size(); i++) {
								if (server_data.get(i).hasValue()) {
									if (server_data.get(i).getColumn()
											.equals("key")
											|| server_data.get(i).getColumn()
													.equals("deleted")
											|| server_data.get(i).getColumn()
													.equals("version")) {
										continue;
									}
									cv.put(server_data.get(i).getColumn(),
											server_data.get(i).getValue());
								}
							}

							// YGO: if update for row in local update with
							// strong consistency, set isUpdated to true
							if (syncSet != null) {
								if (syncSet.getRowId().equals(id)) {
									syncSet.setUpdated();
								}
							}

							updateWithoutDirty(cv, "_id=?", new String[] { id });
							newRows++;

							// set tbl_rev to server's rev
							setTblRev(server_rev);
						}
						// put server row into TBL_CONFLICT to be updated later
						else {
							Log.d(TAG, "UPDATE row with objects");
							ContentValues cv = new ContentValues();
							cv.put("_id", id);
							cv.put("_rev", mdr.getRev());

							List<ColumnData> server_data = mdr.getData();
							// row data
							for (int i = 0; i < server_data.size(); i++) {
								if (server_data.get(i).hasValue()) {
									if (server_data.get(i).getColumn()
											.equals("key")
											|| server_data.get(i).getColumn()
													.equals("deleted")
											|| server_data.get(i).getColumn()
													.equals("version")) {
										continue;
									}
									cv.put(server_data.get(i).getColumn(),
											server_data.get(i).getValue());
								}
							}
							// object data. set value to be new object id for
							// leveldb
							List<ObjectHeader> server_obj = mdr.getObj();
							for (int i = 0; i < server_obj.size(); i++) {
								ObjectHeader oh = server_obj.get(i);
								// check if object size has been changed
								if (oh.hasNum_chunks()) {
									cv.put("count" + oh.getColumn(),
											oh.getNum_chunks());
								}

								long obj_id = SimbaLevelDB.getMaxObjID();
								cv.put(oh.getColumn(), obj_id);
								obj_list.put(oh.getOid(), obj_id);
								SimbaLevelDB.setMaxObjID(obj_id + 1);
							}

							// YGO: if update for row in local update with
							// strong consistency, set isUpdated to true
							if (syncSet != null) {
								if (syncSet.getRowId().equals(id)) {
									syncSet.setUpdated();
								}
							}

							writeConflictCopy(cv);
						}
					}
				} else {
					if (fromNotificationPull) {
						/* DUPLICATE ROW. Ignore this row */
						Log.d(TAG, "DUPLICATE ROW. Client: " + client_rev
								+ ", Server: " + server_rev);
						List<ObjectHeader> oh_list = mdr.getObj();
						for (ObjectHeader oh : oh_list) {
							Log.d(TAG, "Object oid[" + oh.getOid()
									+ "] will be dropped");
						}
						if (oh_list.size() == 0) {
							// set tbl_rev to server's rev
							setTblRev(server_rev);
						}
					} else {
						// torn row is handled
						torn_rows.remove(id);

						/* RECOVER ROW. Overwrite the existing row */
						if (mdr.getObj().size() == 0) {
							Log.d(TAG, "RECOVER torn row without objects");
							ContentValues cv = new ContentValues();
							cv.put("_id", id);
							cv.put("_rev", mdr.getRev());
							assert (client_torn == true);
							cv.put("_torn", Boolean.FALSE);

							List<ColumnData> server_data = mdr.getData();
							for (int i = 0; i < server_data.size(); i++) {
								if (server_data.get(i).hasValue()) {
									if (server_data.get(i).getColumn()
											.equals("key")
											|| server_data.get(i).getColumn()
													.equals("deleted")
											|| server_data.get(i).getColumn()
													.equals("version")) {
										continue;
									}
									cv.put(server_data.get(i).getColumn(),
											server_data.get(i).getValue());
								}
							}
							updateWithoutDirty(cv, "_id=?", new String[] { id });

							// set tbl_rev to server's rev
							setTblRev(server_rev);
						} else {
							Log.d(TAG, "RECOVER torn row with objects");
							ContentValues cv = new ContentValues();
							cv.put("_id", id);
							cv.put("_rev", mdr.getRev());

							List<ColumnData> server_data = mdr.getData();
							// row data
							for (int i = 0; i < server_data.size(); i++) {
								if (server_data.get(i).hasValue()) {
									if (server_data.get(i).getColumn()
											.equals("key")
											|| server_data.get(i).getColumn()
													.equals("deleted")
											|| server_data.get(i).getColumn()
													.equals("version")) {
										continue;
									}
									cv.put(server_data.get(i).getColumn(),
											server_data.get(i).getValue());
								}
							}
							// object data. set value to be new object id for
							// leveldb
							List<ObjectHeader> server_obj = mdr.getObj();
							for (int i = 0; i < server_obj.size(); i++) {
								ObjectHeader oh = server_obj.get(i);
								// check if object size has been changed
								if (oh.hasNum_chunks()) {
									cv.put("count" + oh.getColumn(),
											oh.getNum_chunks());
								}

								long obj_id = SimbaLevelDB.getMaxObjID();
								cv.put(oh.getColumn(), obj_id);
								obj_list.put(oh.getOid(), obj_id);
								SimbaLevelDB.setMaxObjID(obj_id + 1);
							}
							writeConflictCopy(cv);
						}
					}
				}
			} else {
				ContentValues cv = new ContentValues();
				cv.put("_id", id);
				cv.put("_rev", mdr.getRev());
				// cv.put("_dirty", Boolean.FALSE);

				List<ColumnData> server_data = mdr.getData();
				for (int i = 0; i < server_data.size(); i++) {
					if (server_data.get(i).hasValue()) {
						if (server_data.get(i).getColumn().equals("key")
								|| server_data.get(i).getColumn()
										.equals("deleted")
								|| server_data.get(i).getColumn()
										.equals("version")) {
							continue;
						}
						cv.put(server_data.get(i).getColumn(),
								server_data.get(i).getValue());
					}
				}
				// object data. set value to be new object id for
				// leveldb
				List<ObjectHeader> server_obj = mdr.getObj();
				for (int i = 0; i < server_obj.size(); i++) {
					ObjectHeader oh = server_obj.get(i);
					// check if object size has been changed
					if (oh.hasNum_chunks()) {
						cv.put("count" + oh.getColumn(), oh.getNum_chunks());
					}

					long obj_id = SimbaLevelDB.getMaxObjID();
					cv.put(oh.getColumn(), obj_id);
					obj_list.put(oh.getOid(), obj_id);
					SimbaLevelDB.setMaxObjID(obj_id + 1);
				}
				if (mdr.getObj().size() == 0) {
					Log.d(TAG, "INSERT row without objects");
					cv.put("_dirty", Boolean.FALSE);
					writeInternal(cv, null, false, false);
					newRows++;

					// set tbl_rev to server's rev
					setTblRev(server_rev);
				} else {
					Log.d(TAG, "INSERT row with objects");
					writeConflictCopy(cv);
				}
			}
		}

		// 2. handle deleted rows
		for (DataRow del_row : deletedRows) {
			boolean found = false;
			String id = del_row.getId();
			int server_rev = del_row.getRev();

			String[] projs = new String[user_cols.length + 4];
			projs[0] = "_torn";
			projs[1] = "_dirty";
			projs[2] = "_dirtyObj";
			projs[3] = "_conflict";
			System.arraycopy(user_cols, 0, projs, 4, user_cols.length);

			SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
			qb.setTables(tbl);
			Cursor cursor = qb.query(db, projs, "_id = ? AND _sync = ?",
					new String[] { id, "0" }, null, null, null);
			if (cursor.getCount() > 0) {
				cursor.moveToFirst();
				boolean client_torn = (cursor.getInt(0) == 1);
				boolean client_dirty = (cursor.getInt(1) == 1);
				boolean client_dirtyObj = (cursor.getInt(2) == 1);
				boolean client_conflict = (cursor.getInt(3) == 1);
				if ((client_dirty || client_dirtyObj) && fromNotificationPull) {
					// if a row to update is used at conflict resolution,
					// ignore all PullData messages from this row
					if (client_conflict && inCR) {
						Log.d(TAG, "This row is in CR! row=" + id);
						stopPullData = true;

						return new int[] { newRows, conflictedRows,
								numDeletedRows };
					}

					handleConflict(id, del_row.getRev(), del_row.getData(),
							del_row.getObj(), obj_list, false, true,
							client_conflict);
					Log.d(TAG, "CONFLICT on delete row");
					conflictedRows++;
				} else {
					// torn row is handled
					if (client_torn) {
						torn_rows.remove(id);
					}

					// delete objects
					for (int i = 4; i < cursor.getColumnCount(); i++) {
						if (cursor.getColumnName(i).startsWith(OBJTAG)
								&& !cursor.isNull(i)) {
							SimbaLevelDB.deleteObject(cursor.getLong(i), 0);
						}
					}
					Log.d(TAG, "DELETE row: " + id);
					int nDeleted = db.delete(tbl, "_id=?", new String[] { id });
					numDeletedRows += nDeleted;
				}
				found = true;
			}
			if (!found) {
				// not found in main table, delete from deleted table if it's
				// there
				purge("_id=?", new String[] { id }, Boolean.TRUE);
				// don't need to update the numDeleted count over here, because
				// the user couldn't have seen this row anyway
			}

			// YGO: if update for row in local update with
			// strong consistency, set isUpdated to true
			if (syncSet != null) {
				if (syncSet.getRowId().equals(id)) {
					syncSet.setUpdated();
				}
			}

			// set tbl_rev to server's rev
			setTblRev(server_rev);
		}

		// 3. delete any torn rows that weren't handled
		for (String row : torn_rows) {
			SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
			qb.setTables(tbl);
			Cursor cursor = qb.query(db, user_cols, "_id = ?",
					new String[] { row }, null, null, null);
			if (cursor.getCount() > 0) {
				cursor.moveToFirst();
				for (int i = 0; i < cursor.getColumnCount(); i++) {
					if (cursor.getColumnName(i).startsWith(OBJTAG)
							&& !cursor.isNull(i)) {
						Log.d(TAG, "DELETE object: " + cursor.getLong(i));
						SimbaLevelDB.deleteObject(cursor.getLong(i), 0);
					}
				}
			}
			Log.d(TAG, "DELETE torn row that was never synced: " + row);
			int nDeleted = db.delete(tbl, "_id=?", new String[] { row });
			numDeletedRows += nDeleted;
		}

		return new int[] { newRows, conflictedRows, numDeletedRows };
	}

	public int getRev() {
		if (tbl_rev == -1) {
			tbl_rev = metadata.getInteger(app, tbl, "tbl_rev", -1);
		}
		return tbl_rev;
	}

	public int[] processObjectFragmentEof(SimbaSyncObject mso, DataRow row,
			boolean isSyncResponse) {
		int newRows = 0;
		int conflictedRows = 0;
		int numDeletedRows = 0;
		boolean isStrongConsistency = (consistencyLevel == SimbaConsistency.Type.STRONG) ? true
				: false;

		/* handle eof for SyncResponse */
		if (isSyncResponse) {
			if (isStrongConsistency) {
				String id = row.getId();
				int svr_rev = row.getRev();

				// YGO: merge server's data from TBL_CONFLICT
				ContentValues cv = new ContentValues();
				SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
				qb.setTables(tbl);
				Cursor cursor = qb.query(db, user_cols, "_id=?",
						new String[] { id }, null, null, null);
				if (cursor.getCount() > 0) {
					cursor.moveToFirst();
					for (int i = 0; i < cursor.getColumnCount(); i++) {
						if (cursor.getColumnName(i).startsWith(OBJTAG)) {
							qb = new SQLiteQueryBuilder();
							qb.setTables(TBL_CONFLICT);
							Cursor srv_cursor = qb
									.query(db,
											new String[] {
													cursor.getColumnName(i),
													"count"
															+ cursor.getColumnName(i) },
											"_id = ?", new String[] { row
													.getId() }, null, null,
											null);
							if (srv_cursor.getCount() > 0) {
								srv_cursor.moveToFirst();
								if (!srv_cursor.isNull(0)) {
									if (!srv_cursor.isNull(1)) {
										int num_chunks = srv_cursor.getInt(1);
										// truncate the size accordingly
										ReadOptions ro = SimbaLevelDB
												.takeSnapshot();
										SimbaLevelDB
												.truncate(
														cursor.getLong(i),
														(num_chunks == 0 ? 0
																: num_chunks - 1)
																* SimbaLevelDB
																		.getChunkSize()
																+ SimbaLevelDB
																		.getChunk(
																				ro,
																				srv_cursor
																						.getLong(0),
																				num_chunks - 1).length,
														false);
										SimbaLevelDB.closeSnapshot(ro);
									}
									SimbaLevelDB.updateObject(
											srv_cursor.getLong(0),
											cursor.getLong(i));
								}
							}
						}
					}
					// insert server copy into local table
					List<String> svr_data = this.get_server_copy(id)
							.getColumnData();

					for (int i = 0; i < svr_data.size(); i++) {
						if (svr_data.get(i) != null) {
							cv.put(user_cols[i], svr_data.get(i));
						}
					}

					cv.put("_rev", svr_rev);
					cv.put("_sync", Boolean.FALSE);
					cv.put("_dirty", Boolean.FALSE);
					cv.put("_dirtyObj", Boolean.FALSE);
					cv.put("_conflict", Boolean.FALSE);

					int num_rows = db.update(tbl, cv, "_id=?",
							new String[] { id });
					if (num_rows == 0) {
						// NOTE: Moving to local table is already done above!
						assert false;
						purge("_id=?", new String[] { id }, Boolean.TRUE);
						cv.put("_id", id);
						db.insert(tbl, null, cv);
					}
				}
			} else {
				SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
				qb.setTables(TBL_CONFLICT);
				Cursor cursor = qb.query(db, null, "_id=?",
						new String[] { row.getId() }, null, null, null);

				if (cursor.getCount() > 0) {
					cursor.moveToFirst();

					// mark _conflict flag in local table
					ContentValues cv = new ContentValues();
					cv.put("_sync", Boolean.FALSE);
					cv.put("_conflict", Boolean.TRUE);

					int affected_rows = updateWithoutDirty(cv, "_id=?",
							new String[] { row.getId() });
					if (affected_rows < 1) {
						cv = new ContentValues();
						cv.put("_conflict", Boolean.TRUE);
						cv.put("_sync", Boolean.FALSE);
						updateDelete(cv, "_id=?", new String[] { row.getId() });
					}
					conflictedRows++;
				}
			}
		}
		/* handle eof for PullData & TornRowResponse */
		else {
			String id = row.getId();
			int server_rev = row.getRev();
			int client_rev = -1;
			boolean client_torn = false;
			boolean client_dirty = false;
			boolean client_dirtyObj = false;
			boolean client_syncing = false;
			boolean client_conflict = false;
			int found = 0;

			SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
			qb.setTables(tbl);
			Cursor cursor = qb
					.query(db,
							new String[] { "_rev, _torn, _dirty, _dirtyObj, _sync, _conflict" },
							"_id=?", new String[] { id }, null, null, null);
			if (cursor.getCount() > 0) {
				cursor.moveToFirst();
				client_rev = cursor.getInt(0);
				client_torn = (cursor.getInt(1) == 1);
				client_dirty = (cursor.getInt(2) == 1);
				client_dirtyObj = (cursor.getInt(3) == 1);
				client_syncing = (cursor.getInt(4) == 1);
				client_conflict = (cursor.getInt(5) == 1);
				found = 1;
			}
			cursor.close();

			if (client_syncing) {
				Log.d(TAG, "This row is currently being synced");
				// ignore this row, it is an update for a row that is
				// currently
				// being synced
				// we will handle this row when the sync completed
				return new int[] { newRows, conflictedRows, numDeletedRows };
			}

			if (found == 0) {
				qb = new SQLiteQueryBuilder();
				qb.setTables(TBL_DEL);
				cursor = qb.query(db, new String[] { "_rev, _conflict" },
						"_id = ?", new String[] { id }, null, null, null);
				if (cursor.getCount() > 0) {
					cursor.moveToFirst();
					client_rev = cursor.getInt(0);
					client_dirty = true;
					client_conflict = (cursor.getInt(1) == 1);
					found = 2;
				}
				cursor.close();
			}

			if (found != 0) {
				if (client_rev < server_rev) {
					if (client_dirty || client_dirtyObj) {
						/* CONFLICT */
						Log.d(TAG, "CONFLICT row with objects");
						ContentValues cv = new ContentValues();
						cv.put("_conflict", Boolean.TRUE);
						if (client_dirty)
							cv.put("_dirty", Boolean.TRUE);
						if (client_dirtyObj)
							cv.put("_dirtyObj", Boolean.TRUE);

						int affected_rows = found == 2 ? updateDelete(cv,
								"_id=?", new String[] { id }) : updateInternal(
								cv, "_id=?", new String[] { id });
						assert affected_rows == 1 : "Must be found";
						conflictedRows++;

						// if client was already in conflict, clear _sync flag
						// in TBL_CONFLICT
						if (client_conflict) {
							cv.clear();
							Log.d(TAG,
									"Clear _sync flag for a row in TBL_CONFLICT");
							cv.put("_sync", Boolean.FALSE);
							db.update(TBL_CONFLICT, cv, "_id=?",
									new String[] { id });
						}
					} else {
						/* UPDATE */
						ContentValues cv = new ContentValues();
						cv.put("_id", id);
						cv.put("_rev", row.getRev());
						if (client_torn) {
							Log.d(TAG, "RECOVER torn row with objects");
							cv.put("_torn", Boolean.FALSE);
						} else {
							Log.d(TAG, "UPDATE row with objects");
						}

						// column data
						List<ColumnData> server_data = row.getData();
						for (int i = 0; i < server_data.size(); i++) {
							if (server_data.get(i).hasValue()) {
								if (server_data.get(i).getColumn()
										.equals("key")
										|| server_data.get(i).getColumn()
												.equals("deleted")
										|| server_data.get(i).getColumn()
												.equals("version")) {
									continue;
								}
								cv.put(server_data.get(i).getColumn(),
										server_data.get(i).getValue());
							}
						}

						// object data
						List<ObjectHeader> server_obj = row.getObj();
						for (int i = 0; i < server_obj.size(); i++) {
							qb = new SQLiteQueryBuilder();
							qb.setTables(TBL_CONFLICT);
							cursor = qb.query(db, new String[] {
									server_obj.get(i).getColumn(),
									"count" + server_obj.get(i).getColumn() },
									"_id=?", new String[] { id }, null, null,
									null);
							if (cursor.getCount() > 0) {
								cursor.moveToFirst();
								// object to update from
								long fromObj = cursor.getLong(0);

								// object to update to
								qb = new SQLiteQueryBuilder();
								qb.setTables(tbl);
								Cursor local_cursor = qb.query(db,
										new String[] { server_obj.get(i)
												.getColumn() }, "_id=?",
										new String[] { id }, null, null, null);
								if (local_cursor.getCount() > 0) {
									local_cursor.moveToFirst();
									long toObj = local_cursor.getLong(0);

									// for recovery, replace the obj_id
									if (client_torn) {
										SimbaLevelDB.deleteObject(toObj, 0);
										cv.put(local_cursor.getColumnName(0),
												fromObj);
										SimbaChunkList
												.removeDirtyChunks(fromObj);
									} else {
										// object size has been changed!
										if (!cursor.isNull(1)) {
											int num_chunks = cursor.getInt(1);
											Log.d(TAG, "num_chunks: "
													+ num_chunks);

											// truncate the size accordingly
											ReadOptions ro = SimbaLevelDB
													.takeSnapshot();
											SimbaLevelDB
													.truncate(
															toObj,
															SimbaLevelDB
																	.getChunk(
																			ro,
																			fromObj,
																			num_chunks - 1).length,
															false);
											SimbaLevelDB.closeSnapshot(ro);
										}
										SimbaLevelDB.updateObject(fromObj,
												toObj);
									}
								}
							}
						}
						updateWithoutDirty(cv, "_id=?", new String[] { id });
						newRows++;

						// delete row from TBL_CONFLICT
						db.delete(TBL_CONFLICT, "_id=?", new String[] { id });
					}
				} else {
					/* RECOVER ROW. Overwrite the existing row */
					Log.d(TAG, "RECOVER torn row with objects");
					ContentValues cv = new ContentValues();
					cv.put("_id", id);
					cv.put("_rev", row.getRev());
					assert (client_torn == true);
					cv.put("_torn", Boolean.FALSE);

					// column data
					List<ColumnData> server_data = row.getData();
					for (int i = 0; i < server_data.size(); i++) {
						if (server_data.get(i).hasValue()) {
							if (server_data.get(i).getColumn().equals("key")
									|| server_data.get(i).getColumn()
											.equals("deleted")
									|| server_data.get(i).getColumn()
											.equals("version")) {
								continue;
							}
							cv.put(server_data.get(i).getColumn(), server_data
									.get(i).getValue());
						}
					}

					// object data
					List<ObjectHeader> server_obj = row.getObj();
					for (int i = 0; i < server_obj.size(); i++) {
						qb = new SQLiteQueryBuilder();
						qb.setTables(TBL_CONFLICT);
						cursor = qb.query(db, new String[] { server_obj.get(i)
								.getColumn() }, "_id=?", new String[] { id },
								null, null, null);

						if (cursor.getCount() > 0) {
							cursor.moveToFirst();
							// object to update to
							long newObj = cursor.getLong(0);

							// object to delete
							qb = new SQLiteQueryBuilder();
							qb.setTables(tbl);
							cursor = qb.query(db, new String[] { server_obj
									.get(i).getColumn() }, "_id=?",
									new String[] { id }, null, null, null);
							if (cursor.getCount() > 0) {
								cursor.moveToFirst();
								long oldObj = cursor.getLong(0);

								// replace old obj_id with new obj_id
								SimbaLevelDB.deleteObject(oldObj, 0);
								cv.put(cursor.getColumnName(0), newObj);
								SimbaChunkList.removeDirtyChunks(newObj);
							}
						}
					}
					updateWithoutDirty(cv, "_id=?", new String[] { id });

					// delete row from TBL_CONFLICT
					db.delete(TBL_CONFLICT, "_id=?", new String[] { id });
				}
			} else {
				/* INSERT */
				Log.d(TAG, "INSERT row with objects");
				ContentValues cv = new ContentValues();
				cv.put("_id", id);
				cv.put("_rev", row.getRev());
				cv.put("_dirty", Boolean.FALSE);
				cv.put("_dirtyObj", Boolean.FALSE);

				// column data
				List<ColumnData> server_data = row.getData();
				for (int i = 0; i < server_data.size(); i++) {
					if (server_data.get(i).hasValue()) {
						if (server_data.get(i).getColumn().equals("key")
								|| server_data.get(i).getColumn()
										.equals("deleted")
								|| server_data.get(i).getColumn()
										.equals("version")) {
							continue;
						}
						cv.put(server_data.get(i).getColumn(),
								server_data.get(i).getValue());
					}
				}

				// object data
				List<ObjectHeader> server_obj = row.getObj();
				for (int i = 0; i < server_obj.size(); i++) {
					qb = new SQLiteQueryBuilder();
					qb.setTables(TBL_CONFLICT);
					cursor = qb.query(db, new String[] { server_obj.get(i)
							.getColumn() }, "_id=?", new String[] { id }, null,
							null, null);
					if (cursor.getCount() > 0) {
						cursor.moveToFirst();
						// insert new object id set by server
						long obj_id = cursor.getLong(0);
						cv.put(server_obj.get(i).getColumn(), obj_id);
						SimbaChunkList.removeDirtyChunks(obj_id);
					}
				}
				writeInternal(cv, null, true, false);

				// delete row from TBL_CONFLICT
				db.delete(TBL_CONFLICT, "_id=?", new String[] { id });
				newRows++;
			}
			// set tbl_rev to server's rev
			setTblRev(server_rev);
		}
		return new int[] { newRows, conflictedRows, numDeletedRows };
	}

	public int getConsistencyLevel() {
		return consistencyLevel;
	}

	public void setTblRev(int server_rev) {
		if (tbl_rev < server_rev) {
			tbl_rev = server_rev;
			metadata.put(app, tbl, "tbl_rev", tbl_rev);
			Log.d(TAG, "Set tbl_rev to " + tbl_rev);
		}
	}

	public int getSyncPeriod(boolean write) {
		if (write)
			return writeperiod;
		else
			return readperiod;
	}

	public boolean isSyncEnabled(boolean write) {
		if (write)
			return (writeperiod != -1);
		else
			return (readperiod != -1);
	}

	public void disableSync(boolean write) {
		if (write)
			this.writeperiod = -1;
		else
			this.readperiod = -1;
	}

	public void enableSync(int period, boolean write) {
		assert period != -1;
		if (write)
			this.writeperiod = period;
		else
			this.readperiod = period;
	}

	public int getSyncDT(boolean write) {
		if (write)
			return writedt;
		else
			return readdt;
	}

	public void setSyncDT(int dt, boolean write) {
		if (write)
			this.writedt = dt;
		else
			this.readdt = dt;
	}

	public String getAppId() {
		return this.app;
	}

	public String getTblId() {
		return this.tbl;
	}

	public String toString() {
		return "SimbaTable: {" + db.getPath() + "/" + tbl + ", " + writeperiod
				+ "," + writedt + ", " + readperiod + "," + readdt + "}";
	}

	/***
	 * This function returns the preference of network connection on which sync
	 * is enabled
	 * 
	 * @return
	 */
	public ConnState getSyncNWpref(boolean write) {
		if (write)
			return this.writeconnPref;
		else
			return this.readconnPref;
	}

	public void setSyncNWpref(ConnState connPref, boolean write) {
		if (write)
			this.writeconnPref = connPref;
		else
			this.readconnPref = connPref;
	}

	public void trim() {
		// Expensive operation!
		SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
		qb.setTables(tbl);
		db.beginTransaction();
		Cursor c = db.rawQuery("SELECT MIN(_rev), MAX(_rev), COUNT(*) FROM "
				+ tbl
				+ " WHERE _dirty=0 AND _sync=0 AND _conflict=0 AND _rev<>-1",
				null);
		db.endTransaction();
		if (c.getCount() > 0) {
			c.moveToFirst();
			int numRemaining = c.getInt(2);
			int maxRev = c.getInt(1);
			int uptoRev = c.getInt(0);
			int numLoops = 0;
			// heuristically start dropping entries

			while (numRemaining > Preferences.MAX_ENTRIES
					&& numLoops < Preferences.MAX_TRIM_LOOPS) {
				int toDelete = numRemaining - Preferences.MAX_ENTRIES;
				uptoRev += toDelete;
				int deleted = db.delete(tbl,
						"_rev<=? AND _dirty=0 AND _sync=0 AND _conflict=0",
						new String[] { Integer.toString(uptoRev) });
				uptoRev++;
				numRemaining -= deleted;
				numLoops++;
			}

		}

	}
}
