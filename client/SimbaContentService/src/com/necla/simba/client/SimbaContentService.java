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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import net.jarlehansen.protobuf.javame.ByteString;

import org.iq80.leveldb.ReadOptions;

import android.app.Service;
import android.content.ContentValues;
import android.content.Intent;
import android.content.SharedPreferences;
import android.content.SharedPreferences.OnSharedPreferenceChangeListener;
import android.database.Cursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteException;
import android.os.Handler;
import android.os.IBinder;
import android.os.Message;
import android.os.RemoteException;
import android.util.Log;

import com.necla.simba.protocol.ActivePull;
import com.necla.simba.protocol.Column;
import com.necla.simba.protocol.CreateTable;
import com.necla.simba.protocol.SimbaMessage;
import com.necla.simba.protocol.NotificationPull;
import com.necla.simba.protocol.ObjectFragment;
import com.necla.simba.protocol.ObjectHeader;
import com.necla.simba.protocol.SimbaConsistency;
import com.necla.simba.protocol.SubscribeTable;
import com.necla.simba.protocol.SyncHeader;
import com.necla.simba.protocol.SyncRequest;
import com.necla.simba.protocol.UnsubscribeTable;

/***
 * This class is the core of the Simba content service.
 */
public class SimbaContentService extends Service {
	private static final String TAG = "SimbaContentService";
	private final ConcurrentHashMap<String, SQLiteDatabase> client_dbhandle = new ConcurrentHashMap<String, SQLiteDatabase>();
	private SimbaNetworkManager networkManager;
	private SyncScheduler syncScheduler;
	private WriteTimerManager writeTimerManager;
	private SharedPreferences settings;
	private ConnectionState cs;
	private SimbaLevelDB mldb;

	private static List<String> tblBitmap = new ArrayList<String>();
	private int bitmapId = 0;
	private Map<String, ArrayList<RowObject>> appToObj = new HashMap<String, ArrayList<RowObject>>();
	private Map<String, byte[]> writeChunks = new HashMap<String, byte[]>();
	private Map<String, byte[]> readChunks = new HashMap<String, byte[]>();

	private Handler handler;
	private static boolean isConnected = false;
	private static boolean isRunning = false;

	private static boolean createDirIfNotExists(String dir) {
		boolean ret = true;

		File file = new File(dir);
		if (!file.exists()) {
			if (!file.mkdirs()) {
				Log.e(TAG, "Could not create database directory: " + dir);
				ret = false;
			}
		}
		return ret;
	}

	@Override
	public void onCreate() {
		Log.v(TAG, "SCS onCreate");
		isRunning = true;
		createDirIfNotExists(Preferences.DBPATH);

		// configure logging
		ConfigLog4j.configure();

		settings = getSharedPreferences(Preferences.PREFS_NAME, 0);
		String hostname = settings.getString("hostname",
				Preferences.DEFAULT_HOST);
		int port = settings.getInt("port", Preferences.DEFAULT_PORT);
		settings.registerOnSharedPreferenceChangeListener(new OnSharedPreferenceChangeListener() {

			@Override
			public void onSharedPreferenceChanged(SharedPreferences prefs,
					String key) {
				if (key.equals("hostname") || key.equals("port"))
					networkManager.updateNetworkSettings(prefs.getString(
							"hostname", Preferences.DEFAULT_HOST), prefs
							.getInt("port", Preferences.DEFAULT_PORT));
			}
		});
		metadata = new Metadata(Preferences.DBPATH);
		handler = new Handler() {
			@Override
			public void handleMessage(Message msg) {
				switch (msg.what) {
				case InternalMessages.NETWORK_CONNECTED: {
					networkManager.tokenManager.networkConnected();

					isConnected = true;
					Intent intent = new Intent(
							SimbaBroadcastReceiver.CONNECTION_STATE_CHANGED);
					intent.putExtra(
							SimbaBroadcastReceiver.EXTRA_CONNECTION_STATE,
							true);
					sendBroadcast(intent);
				}
					break;
				case InternalMessages.NETWORK_DISCONNECTED: {
					isConnected = false;
					Intent intent = new Intent(
							SimbaBroadcastReceiver.CONNECTION_STATE_CHANGED);
					intent.putExtra(
							SimbaBroadcastReceiver.EXTRA_CONNECTION_STATE,
							false);
					sendBroadcast(intent);
				}
					break;
				case InternalMessages.CLIENT_LOST: {
					String app = (String) msg.obj;
					Log.v(TAG, "Client " + app
							+ " is no longer connected, dropping");

					boolean ret = unregisterAppInternal(app);
					if (ret)
						AppAuthenticationManager.unregisterByUID(app);
				}
					break;

				case InternalMessages.AUTHENTICATION_DONE: {
					networkManager.processPendingMessages();
				}
					break;

				}
			}

		};

		// get the connection state in SCS and pass it around
		cs = new ConnectionState(this.getApplicationContext());

		networkManager = new SimbaNetworkManager(this,
				this.getApplicationContext(), hostname, port, handler, cs);
		syncScheduler = new SyncScheduler(networkManager, cs);
		writeTimerManager = new WriteTimerManager(syncScheduler, cs);

		// create SimbaLevelDB
		mldb = new SimbaLevelDB();
	}

	private Metadata metadata;
	private SimbaChunkList chunklist;

	private final IBinder mBinder = new SimbaContentServiceAPI.Stub() {
		/**
		 * register the client, create its database if necessary, store its
		 * callback.
		 * 
		 * @param uid
		 *            user id
		 * @param callback
		 *            callback handle for client
		 * @return the ticket id to be used for subsequent communication
		 */
		public String registerApp(String uid, ISCSClient callback)
				throws RemoteException {
			String tid = null;
			SQLiteDatabase db;

			if (ClientCallbackManager.clientExists(uid)) {
				Log.w(TAG,
						"Multiple registers from same app are not supported, removing previous registration");
				boolean ret = unregisterAppInternal(uid);
				if (ret)
					AppAuthenticationManager.unregisterByUID(uid);
			}
			tid = AppAuthenticationManager.register(uid);

			ClientCallbackManager.addClient(uid, callback, handler);
			if (Preferences.WAL) {
				Log.v(TAG, "Callback added: " + callback
						+ "; creating app database with WAL for: " + uid);
				db = SQLiteDatabase.openDatabase(Preferences.DBPATH + uid
						+ ".db", null, SQLiteDatabase.CREATE_IF_NECESSARY
						| SQLiteDatabase.ENABLE_WRITE_AHEAD_LOGGING, null);

				// Log.d(TAG, "Checkpointing " + uid + "DB after create");
				// db.rawQuery("PRAGMA wal_checkpoint", null);
			} else {
				Log.v(TAG, "Callback added: " + callback
						+ "; creating app database in default mode for: " + uid);
				db = SQLiteDatabase.openOrCreateDatabase(Preferences.DBPATH
						+ uid + ".db", null);
			}

			client_dbhandle.put(uid, db);
			List<SimbaTable> mtbls = recoverTables(uid, db);
			SimbaTableManager.addRecoveredTables(uid, mtbls);

			// subscribe tables if they have read sync setting
			// TODO: this is very inefficient!
			for (SimbaTable t : mtbls) {
				int period = 0;
				if ((period = metadata.getInteger(uid, t.getTblId(),
						"readperiod", -1)) != -1) {
					sub_tbl(uid, t.getTblId(), period, t.getSyncDT(false));
				}
			}

			Log.v(TAG, db.getPath() + " opened");

			return tid;
		}

		/**
		 * unregister the client
		 */
		public boolean unregisterApp(String tid) throws RemoteException {

			String uid = AppAuthenticationManager.authenticate(tid);
			if (uid == null)
				return false;

			// TODO: close any objects that were opened by this app

			boolean ret = unregisterAppInternal(uid);
			if (ret)
				AppAuthenticationManager.unregister(tid);
			return ret;

		}

		public boolean isNetworkConnected() {
			return isConnected;
		}

		private SimbaTable mtblSyncSetting(SimbaTable mtbl, int period,
				int delay, ConnState syncpref, boolean rw) {

			mtbl.enableSync(period, rw);
			mtbl.setSyncDT(delay, rw);
			mtbl.setSyncNWpref(syncpref, rw);
			return mtbl;
		}

		private List<SimbaTable> recoverTables(String uid, SQLiteDatabase db) {
			ArrayList<SimbaTable> mtbls = new ArrayList<SimbaTable>();
			Cursor cursor = db.query("sqlite_master", new String[] { "name",
					"sql" }, null, null, null, null, null, null);
			cursor.moveToFirst();

			do {
				String tbl_name = cursor.getString(0);

				if (tbl_name.endsWith("_deleted")
						|| tbl_name.endsWith("_server")
						|| tbl_name.endsWith("_chunk")
						|| tbl_name.equals("android_metadata")
						|| tbl_name.equals("sqlite_sequence"))
					continue;
				String schema = cursor.getString(1);
				if (schema == null)
					continue;

				schema = SimbaTable.schemaToColumnSchema(schema);
				Log.v(TAG, "Recovered table " + tbl_name + " schema=" + schema);

				int lvl = restoreConsistencyLevel(uid, tbl_name);
				TableProperties props = restoreTableProperties(uid, tbl_name);

				SimbaTable mtbl = new SimbaTable(uid, db, tbl_name, schema,
						lvl, props, metadata, true);

				int period = 0, dt = 0;
				ConnState syncpref = ConnState.TG;

				// create table message
				CreateTable.Builder t = CreateTable
						.newBuilder()
						.setApp(uid)
						.setConsistencyLevel(
								SimbaConsistency.newBuilder()
										.setType(SimbaConsistency.Type.CAUSAL)
										.build()).setTbl(tbl_name);
				String[] columns = mtbl.getSchemaSQL().split("\\s+");
				for (int i = 0; i < columns.length; i += 2) {
					int type = Column.Type.VARCHAR;
					if (columns[i + 1].startsWith("VARCHAR")) {
						type = Column.Type.VARCHAR;
					} else if (columns[i + 1].startsWith("INT")) {
						type = Column.Type.INT;
					} else if (columns[i + 1].startsWith("BIGINT")) {
						type = Column.Type.OBJECT;
					}
					t.addElementColumns(Column.newBuilder().setName(columns[i])
							.setType(type).build());
					t.setConsistencyLevel(SimbaConsistency.newBuilder()
							.setType(lvl).build());
				}
				SimbaMessage.Builder b = SimbaMessage.newBuilder()
						.setType(SimbaMessage.Type.CREATE_TABLE)
						.setCreateTable(t.build());
				networkManager.sendTokenedMessage(b);

				// get prefs from metadata table and assign to above variables
				period = metadata.getInteger(uid, tbl_name, "writeperiod", -1);
				if (period != -1) {
					dt = metadata.getInteger(uid, tbl_name, "writedt", -1);
					syncpref.setValue(metadata.getInteger(uid, tbl_name,
							"writesyncpref", -1));
					mtblSyncSetting(mtbl, period, dt, syncpref, true);
					Log.v(TAG,
							"Recovery: Table " + uid + "/" + tbl_name
									+ " set to write sync with a period of "
									+ mtbl.getSyncPeriod(true));
					writeTimerManager.addTask(mtbl);
				}

				period = metadata.getInteger(uid, tbl_name, "readperiod", -1);
				if (period != -1) {
					dt = metadata.getInteger(uid, tbl_name, "readdt", -1);
					syncpref.setValue(metadata.getInteger(uid, tbl_name,
							"readsyncpref", -1));
					mtblSyncSetting(mtbl, period, dt, syncpref, false);
					Log.v(TAG,
							"Recovery: Table " + uid + "/" + tbl_name
									+ " set to read sync with a period of "
									+ mtbl.getSyncPeriod(false));
					// cannot subscribe before adding to TableManager yet!
					// sub_tbl(uid, tbl_name, period, mtbl.getSyncDT(false));
				}

				// set torn rows for rows with open objects
				List<Long> torn_objs = new ArrayList<Long>();
				mtbl.setTornRows(torn_objs);

				// recover dirtyChunkList from the DIRTYCHUNKLIST table
				if (SimbaChunkList.recoverDirtyChunkList(mtbl, torn_objs)) {
					// recover dirty rows with dirtyChunkList
					mtbl.recoverDirtyRows();
				}

				int conflictedRows = 0;
				// remove any obsolete rows in TBL_CONFLICT
				conflictedRows += mtbl.recoverConflictTable();
				conflictedRows += mtbl.recoverDeleteTable();

				mtbls.add(mtbl);

				// if there is any conflict rows, alert the client app
				if (conflictedRows > 0) {
					try {
						ISCSClient client = ClientCallbackManager
								.getCallback(uid);
						if (client != null) {
							client.syncConflict(tbl_name, conflictedRows);
						}
					} catch (RemoteException e) {
						// TODO: handle app crash while recovering
						// e.printStackTrace();
					}
				}
			} while (cursor.moveToNext());
			cursor.close();

			return mtbls;
		}

		private void saveConsistencyLevel(String uid, String tbl, int lvl) {
			metadata.put(uid, tbl, "consistency_level", lvl);
		}

		private void saveTableProperties(String uid, String tbl,
				TableProperties props) {
			metadata.put(uid, tbl, "partial", props.isPartial());
		}

		private int restoreConsistencyLevel(String uid, String tbl) {
			return metadata.getInteger(uid, tbl, "consistency_level", -1);
		}

		private TableProperties restoreTableProperties(String uid, String tbl) {
			boolean partial = metadata.getBoolean(uid, tbl, "partial", false);
			return new TableProperties(partial);
		}

		public boolean subscribeTable(String tid, String tbl, int period,
				int delay, ConnState syncpref) throws RemoteException {
			String uid = AppAuthenticationManager.authenticate(tid);
			if (uid == null)
				return false;

			// insert to table bitmap
			Log.d(TAG, "Inserting <" + uid + ", " + tbl + "> to table bitmap");
			tblBitmap.add(uid + "," + tbl);

			// subscribe to table with _rev -1
			SubscribeTable t = SubscribeTable
					.newBuilder()
					.setApp(uid)
					.setTbl(tbl)
					.setDelayTolerance(
							(int) (delay * Preferences.READ_DT_SERVER_RATIO))
					.setPeriod(period).setRev(-1).build();

			SimbaMessage.Builder b = SimbaMessage.newBuilder()
					.setType(SimbaMessage.Type.SUB_TBL).setSubscribeTable(t);
			// SimbaLogger.log("up, " + b.toString() + ", " +
			// b.build().toString() + ", " + b.build().computeSize());

			networkManager.sendTokenedMessage(b);

			return true;
		}

		public boolean createTable(String tid, String tbl, String cmd, int lvl,
				TableProperties props) throws RemoteException {
			boolean ret = false;
			String uid = AppAuthenticationManager.authenticate(tid);
			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				if (mtbl != null) {
					Log.v(TAG, "table " + uid + "/" + tbl + " already created");
					ret = true;
				} else {
					SQLiteDatabase db = client_dbhandle.get(uid);

					String sql = "CREATE TABLE IF NOT EXISTS " + tbl + " ("
							+ cmd + ");";
					db.execSQL(sql);

					/*
					 * add extra fields: row id, revision number, dirty flag
					 * (row, obj)
					 */
					try {
						db.execSQL("ALTER TABLE " + tbl
								+ " ADD COLUMN _id VARCHAR;");
						db.execSQL("ALTER TABLE " + tbl
								+ " ADD COLUMN _rev INT DEFAULT -1;");
						db.execSQL("ALTER TABLE " + tbl
								+ " ADD COLUMN _torn BIT DEFAULT 0;");
						db.execSQL("ALTER TABLE " + tbl
								+ " ADD COLUMN _dirty BIT DEFAULT 1;");
						db.execSQL("ALTER TABLE " + tbl
								+ " ADD COLUMN _dirtyObj BIT DEFAULT 0;");
						db.execSQL("ALTER TABLE " + tbl
								+ " ADD COLUMN _openObj INT DEFAULT 0;");
						db.execSQL("ALTER TABLE " + tbl
								+ " ADD COLUMN _sync BIT DEFAULT 0;");
						db.execSQL("ALTER TABLE " + tbl
								+ " ADD COLUMN _conflict BIT DEFAULT 0;");
					} catch (SQLiteException e) {
						/* do nothing: the column already exists */
					}

					ret = SimbaTableManager.addTable(uid, tbl,
							new SimbaTable(uid, db, tbl, cmd, lvl, props,
									metadata, false));
					saveConsistencyLevel(uid, tbl, lvl);
					saveTableProperties(uid, tbl, props);

					/* send CreateTable message to server */
					CreateTable.Builder t = CreateTable.newBuilder()
							.setApp(uid).setTbl(tbl);
					String[] columns = cmd.split("\\s+");
					for (int i = 0; i < columns.length; i += 2) {
						int type = Column.Type.VARCHAR;
						if (columns[i + 1].startsWith("VARCHAR")) {
							type = Column.Type.VARCHAR;
						} else if (columns[i + 1].startsWith("INT")) {
							type = Column.Type.INT;
						} else if (columns[i + 1].startsWith("BIGINT")) {
							type = Column.Type.OBJECT;
						}
						t.addElementColumns(Column.newBuilder()
								.setName(columns[i]).setType(type).build());
						t.setConsistencyLevel(SimbaConsistency.newBuilder()
								.setType(lvl).build());
					}

					SimbaMessage.Builder b = SimbaMessage.newBuilder()
							.setType(SimbaMessage.Type.CREATE_TABLE)
							.setCreateTable(t.build());
					// SimbaLogger.log("up, " + b.toString() + ", " +
					// b.build().toString() + ", " + b.build().computeSize());

					networkManager.sendTokenedMessage(b);
				}
			}

			Log.v(TAG, SimbaTableManager.dump());

			return ret;
		}

		public List<RowObject> write(String tid, String tbl,
				ContentValues values, String[] objectOrdering)
				throws RemoteException {
			List<RowObject> ro_list = new ArrayList<RowObject>();
			String uid = AppAuthenticationManager.authenticate(tid);
			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				ro_list = mtbl.write(values, objectOrdering);

				// set mapping between application and opened objects
				// mapping between object & row is handled in SimbaTable!
				ArrayList<RowObject> objects = appToObj.get(uid);
				if (objects == null) {
					objects = new ArrayList<RowObject>();
				}
				for (RowObject ro : ro_list) {
					objects.add(ro);
					Log.d(TAG, "Setting mapping between app: " + uid
							+ ", obj_id: " + ro.GetObjectID());
				}
				appToObj.put(uid, objects);

				/* YGO: sync immediately if strong consistency without object */
				if (ro_list.size() == 0
						&& mtbl.getConsistencyLevel() == SimbaConsistency.Type.STRONG) {
					syncStrongConsistency(mtbl);
				}
			} else {
				/* client not registered */
			}

			return ro_list;
		}

		public SimbaCursorWindow read(String tid, String tbl, String[] projs,
				String sels, String[] selArgs, String sortOrder)
				throws RemoteException {
			SimbaCursorWindow ret = null;
			String uid = AppAuthenticationManager.authenticate(tid);

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				ret = mtbl.read(projs, sels, selArgs, sortOrder);
			} else {
				/* client not registered */
			}

			return ret;
		}

		public List<RowObject> update(String tid, String tbl,
				ContentValues values, String sels, String[] selArgs,
				String[] objectOrdering) throws RemoteException {
			List<RowObject> ro_list = new ArrayList<RowObject>();
			String uid = AppAuthenticationManager.authenticate(tid);
			String[] newSelArgs = null;

			// remove torn rows from selection
			if (sels != null) {
				if (selArgs == null) {
					sels += " AND _torn = 0";
				} else {
					sels += " AND _torn = ?";
					newSelArgs = new String[selArgs.length + 1];
					int i = 0;
					for (i = 0; i < selArgs.length; i++) {
						newSelArgs[i] = selArgs[i];
					}
					newSelArgs[i] = "0";
				}
			}

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				if (newSelArgs == null) {
					ro_list = mtbl
							.update(values, sels, selArgs, objectOrdering);
				} else {
					ro_list = mtbl.update(values, sels, newSelArgs,
							objectOrdering);
				}

				// set mapping between application and opened objects
				// mapping between object & row is handled in SimbaTable!
				ArrayList<RowObject> objects = appToObj.get(uid);
				if (objects == null) {
					objects = new ArrayList<RowObject>();
				}
				for (RowObject ro : ro_list) {
					objects.add(ro);
					Log.d(TAG, "Setting mapping between app: " + uid
							+ ", obj_id: " + ro.GetObjectID());
				}
				appToObj.put(uid, objects);

				/* YGO: sync immediately if strong consistency without object */
				if (ro_list.size() == 0
						&& mtbl.getConsistencyLevel() == SimbaConsistency.Type.STRONG) {
					syncStrongConsistency(mtbl);
				}
			} else {
				/* client not registered */
			}

			return ro_list;
		}

		public int delete(String tid, String tbl, String sels, String[] selArgs)
				throws RemoteException {
			int ret = 0;
			String uid = AppAuthenticationManager.authenticate(tid);

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				ret = mtbl.delete(sels, selArgs);

				/* YGO: sync if strong consistency */
				if (mtbl.getConsistencyLevel() == SimbaConsistency.Type.STRONG) {
					syncStrongConsistency(mtbl);
				}
			} else {
				/* client not registered */
			}

			return ret;
		}

		public void writeSyncOneshot(String tid, String tbl, int delay)
				throws RemoteException {
			/* TODO: special case: client-initiated one-time sync */
			String uid = AppAuthenticationManager.authenticate(tid);

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				write_sync_oneshot(mtbl, delay);
			} else {
				/* client not registered */
			}
		}

		public void registerPeriodicWriteSync(String tid, String tbl,
				int period, int dt, ConnState syncpref) throws RemoteException {
			String uid = AppAuthenticationManager.authenticate(tid);
			boolean rw = true;

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				if (!mtbl.isSyncEnabled(rw)) {
					mtblSyncSetting(mtbl, period, dt, syncpref, rw);
					Log.v(TAG, "Table " + uid + "/" + tid
							+ " being set to write sync with a period of "
							+ mtbl.getSyncPeriod(rw));

					writeTimerManager.addTask(mtbl);
				} else {
					Log.v(TAG,
							"Table "
									+ uid
									+ "/"
									+ tid
									+ " curently already set to write sync with a period of "
									+ mtbl.getSyncPeriod(rw));
					Log.v(TAG,
							"Updating WriteSync preferences with new values; period:"
									+ mtbl.getSyncPeriod(rw) + ", new Period: "
									+ period + ", DT:" + mtbl.getSyncDT(rw)
									+ ", syncpref:" + mtbl.getSyncNWpref(rw));

					// TODO: replace by writeTimerManager.updateTask(mtbl);
					writeTimerManager.removeTask(mtbl);
					mtblSyncSetting(mtbl, period, dt, syncpref, rw);
					writeTimerManager.addTask(mtbl);

				}

				/* store period and DT to Simba meta-data table */
				metadata.put(uid, tbl, "writeperiod", period);
				metadata.put(uid, tbl, "writedt", dt);
				metadata.put(uid, tbl, "writesyncpref", syncpref.getValue());
			} else {
				Log.v(TAG,
						"registerPeriodicWriteSync client not registered tid="
								+ tid);
			}
		}

		public void beginCR(String tid, String tbl) throws RemoteException {
			String uid = AppAuthenticationManager.authenticate(tid);

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				mtbl.beginCR();
			} else {
				/* client not registered */
			}
		}

		public void endCR(String tid, String tbl) throws RemoteException {
			String uid = AppAuthenticationManager.authenticate(tid);

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);

				// if PullData was stopped in the past, send NotificationPull
				if (mtbl.isSendNotificationPull()) {
					mtbl.setSendNotificationPull(false);
					mtbl.endCR();

					Log.d(TAG,
							"Sending NOTIFICATION_PULL after endCR! table: "
									+ mtbl.getTblId() + ", fromVersion: "
									+ mtbl.getRev());
					NotificationPull.Builder np = NotificationPull.newBuilder()
							.setApp(mtbl.getAppId()).setTbl(mtbl.getTblId())
							.setFromVersion(mtbl.getRev());
					int seq = SeqNumManager.getSeq();
					SimbaMessage.Builder m = SimbaMessage.newBuilder()
							.setSeq(seq)
							.setType(SimbaMessage.Type.NOTIFICATION_PULL)
							.setNotificationPull(np.build());
					networkManager.sendTokenedMessage(m);
					SeqNumManager.addPendingSeq(seq, m.build());
				} else {
					mtbl.endCR();
				}
			} else {
				/* client not registered */
			}
		}

		public List<InternalDataRow> getConflictedRows(String tid, String tbl)
				throws RemoteException {
			String uid = AppAuthenticationManager.authenticate(tid);
			List<InternalDataRow> ret = new ArrayList<InternalDataRow>();

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				ret = mtbl.getConflictedRows();
			} else {
				/* client not registered */
			}
			return ret;
		}

		public void resolveConflict(String tid, String tbl, String id,
				CRChoice choice) throws RemoteException {
			String uid = AppAuthenticationManager.authenticate(tid);

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				mtbl.resolveConflict(id, choice);
			} else {
				/* client not registered */
			}
		}

		public void readSyncOneshot(String tid, String tbl)
				throws RemoteException {
			String uid = AppAuthenticationManager.authenticate(tid);
			String token = networkManager.tokenManager.getToken();

			if (uid != null) {
				if (token == null)
					throw new RemoteException("Authentication not complete yet");
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				List<String> payload = new ArrayList<String>();
				payload.add(mtbl.getAppId());
				payload.add(mtbl.getTblId());

				int seq = SeqNumManager.getSeq();
				ActivePull ap = ActivePull.newBuilder().setApp(mtbl.getAppId())
						.setTbl(mtbl.getTblId()).build();
				SimbaMessage m = SimbaMessage.newBuilder()
						.setType(SimbaMessage.Type.ACTIVE_PULL).setSeq(seq)
						.setToken(networkManager.tokenManager.getToken())
						.setActivePull(ap).build();

				SeqNumManager.addPendingSeq(seq, m);

				networkManager.sendMessage(m);
			} else {
				/* client not registered */
			}
		}

		/*
		 * Redundant function? public void registerPeriodicReadSync(String tid,
		 * String tbl, int period, int delay, ConnState syncpref) throws
		 * RemoteException { boolean rw = false; String uid =
		 * AppAuthenticationManager.authenticate(tid); if (uid != null) {
		 * SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl); if
		 * (!mtbl.isSyncEnabled(rw)) { mtblSyncSetting(mtbl, period, delay,
		 * syncpref, rw); Log.v(TAG, "1--Subscribing read timer on " + tbl +
		 * " with p=" + period + " and n/w sync choice=" + syncpref); } else {
		 * Log.v(TAG, "Table " + uid + "/" + tid +
		 * " cuurently already set to read sync with a period of " +
		 * mtbl.getSyncPeriod(rw)); Log.v(TAG,
		 * "1--Updating ReadSync preferences with new values; period:" +
		 * mtbl.getSyncPeriod(rw) + ", syncpref:" + mtbl.getSyncNWpref(rw));
		 * mtblSyncSetting(mtbl, period, delay, syncpref, rw);
		 * 
		 * } metadata.put(uid, tbl, "readperiod", period); metadata.put(uid,
		 * tbl, "readdt", delay); metadata.put(uid, tbl, "readsyncpref",
		 * syncpref.getValue()); } else { // client not registered } }
		 */

		public void subscribePeriodicReadSync(String tid, String tbl,
				int period, int dt, ConnState syncpref) throws RemoteException {
			String uid = AppAuthenticationManager.authenticate(tid);
			boolean rw = false;

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				if (!mtbl.isSyncEnabled(rw)) {
					mtblSyncSetting(mtbl, period, dt, syncpref, rw);
					Log.v(TAG, "2--Subscribing read timer on " + tbl
							+ " with p=" + period + " and n/w sync choice="
							+ syncpref);
					// sub_tbl(uid, tbl, period, dt);
				} else {
					Log.v(TAG,
							"Table "
									+ uid
									+ "/"
									+ tid
									+ " cuurently already set to read sync with a period of "
									+ mtbl.getSyncPeriod(rw));
					Log.v(TAG,
							"2--Updating ReadSync preferences with new values; period:"
									+ mtbl.getSyncPeriod(rw) + ", new Period: "
									+ period + ", syncpref:"
									+ mtbl.getSyncNWpref(rw));
					mtblSyncSetting(mtbl, period, dt, syncpref, rw);

				}
				sub_tbl(uid, tbl, period, dt);

				metadata.put(uid, tbl, "readperiod", period);
				metadata.put(uid, tbl, "readdt", dt);
				metadata.put(uid, tbl, "readsyncpref", syncpref.getValue());
			} else {
				/* client not registered */
			}
		}

		public void unsubscribePeriodicReadSync(String tid, String tbl)
				throws RemoteException {
			String uid = AppAuthenticationManager.authenticate(tid);

			if (uid != null) {
				Log.v(TAG, "Unsubscribing read timer on " + tbl);
				unsub_tbl(uid, tbl);
			} else {
				/* client not registered */
			}
		}

		@Override
		public void unregisterPeriodicWriteSync(String tid, String tbl)
				throws RemoteException {
			Log.v(TAG, "unregister for tbl=" + tbl);
			boolean rw = true;
			String uid = AppAuthenticationManager.authenticate(tid);

			if (uid != null) {
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
				if (mtbl.isSyncEnabled(rw)) {
					// mtbl.disableSync();

					writeTimerManager.removeTask(mtbl);

					/* store period and DT to Simba meta-data table */
					metadata.remove(uid, tbl, new String[] { "writeperiod",
							"writedt", "writesyncpref" });

				} else {
					Log.v(TAG, "Table " + uid + "/" + tid
							+ "has syncing disabled already");
					/* table not registered for syncing */
				}
			} else {
				/* client not registered */
			}

		}

		/* get table schema */
		public String getSchemaSQL(String tid, String tbl) {
			String uid = AppAuthenticationManager.authenticate(tid);
			SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
			return mtbl.getSchemaSQL();
		}

		/* write to leveldb */
		public int writeStream(String tid, String tbl, long obj_id,
				int chunk_num, byte[] buffer, int offset, int length) {
			String uid = AppAuthenticationManager.authenticate(tid);
			SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);

			// TODO: handle update with strong consistency
			// 1) if partial chunk, check whether object already exists
			// 2) read from it, then overwrite it

			// if partial buffer is received, merge with full buffer
			if (buffer.length < length) {
				String key = Long.toString(obj_id) + ","
						+ Integer.toString(chunk_num);
				byte[] buf;
				if (writeChunks.containsKey(key)) {
					buf = writeChunks.get(key);
				} else {
					buf = new byte[length];
					Log.v(TAG, "Trying for Buf length: " + length
							+ " Allocated buf: " + buf.length);
				}

				try {
					System.arraycopy(buffer, 0, buf, offset, buffer.length);
				} catch (Exception e) {
					e.printStackTrace();
					Log.v(TAG, "***************Arraycopy failure**********");
				}

				// full buffer is complete
				if (buffer.length + offset == length) {
					ArrayList<RowObject> ro_list = appToObj.get(uid);
					for (RowObject ro : ro_list) {
						if (ro.GetObjectID() == obj_id) {
							writeChunks.remove(key);
							return SimbaLevelDB.write(obj_id, chunk_num, buf,
									length);
						}
					}
				} else {
					writeChunks.put(key, buf);
					return buffer.length;
				}
			} else {
				ArrayList<RowObject> ro_list = appToObj.get(uid);
				for (RowObject ro : ro_list) {
					if (ro.GetObjectID() == obj_id) {
						return SimbaLevelDB.write(obj_id, chunk_num, buffer,
								length);
					}
				}
			}
			return -1;
		}

		/* truncate the length of object */
		public int truncate(String tid, String tbl, String row_id, long obj_id,
				int length) {
			String uid = AppAuthenticationManager.authenticate(tid);
			SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);

			Log.d(TAG, "Truncate obj: " + obj_id + ", length: " + length);
			if (mtbl.getConsistencyLevel() == SimbaConsistency.Type.STRONG) {
				/* YGO: truncate object at strong consistency */
				mtbl.truncate(obj_id, length);

				return length;
			} else {
				// truncate only if not strong consistency
				return SimbaLevelDB.truncate(obj_id, length, true);
			}
		}

		/* read from leveldb */
		public int readStream(long obj_id, byte[] buffer, int buffer_off,
				int offset, int length) {
			int chunk_num = offset / SimbaLevelDB.getChunkSize();
			int len = 0;

			if (buffer.length < length) {
				byte[] buf;
				String key = Long.toString(obj_id) + ","
						+ Integer.toString(chunk_num);
				if (readChunks.containsKey(key)) {
					buf = readChunks.get(key);
				} else {
					buf = new byte[length];
					len = mldb.read(obj_id, buf, offset);
					readChunks.put(key, buf);
				}
				System.arraycopy(buf, buffer_off, buffer, 0, buffer.length);
				len = buffer.length;

				if (buffer_off + buffer.length == length) {
					readChunks.remove(key);
				}
			} else {
				len = mldb.read(obj_id, buffer, offset);
			}

			return len;
		}

		/* decrement object open counter */
		public void decrementObjCounter(String tid, String tbl, long obj_id) {
			String uid = AppAuthenticationManager.authenticate(tid);

			// allow decrement only if the objct is opened
			ArrayList<RowObject> ro_list = appToObj.get(uid);
			for (RowObject ro : ro_list) {
				if (ro.GetObjectID() == obj_id) {
					// remove mapping of the closed object
					Log.d(TAG, "Removing mapping between app: " + uid
							+ ", obj_id: " + obj_id);
					ArrayList<RowObject> objects = appToObj.get(uid);
					ArrayList<RowObject> new_objects = new ArrayList<RowObject>();
					assert (objects != null);
					for (int i = 0; i < objects.size(); i++) {
						if (objects.get(i).GetObjectID() != obj_id) {
							new_objects.add(objects.get(i));
						}
					}
					appToObj.put(uid, new_objects);

					SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);
					int count = mtbl.decrementObjCounter(obj_id);

					/*
					 * YGO: sync if strong consistency and all objects are
					 * closed
					 */
					if (count == 0
							&& mtbl.getConsistencyLevel() == SimbaConsistency.Type.STRONG) {
						syncStrongConsistency(mtbl);
					}
					break;
				}
			}
		}

		/* !!!NOT USED!!! set opened objects received from app */
		// Map<String, ArrayList<RowObject>>
		public void setOpenObjects(String tid, Map openObjects) {
			String uid = AppAuthenticationManager.authenticate(tid);

			ArrayList<RowObject> objects = appToObj.get(uid);
			if (objects == null) {
				objects = new ArrayList<RowObject>();
			}
			Map<String, List<RowObject>> ro_list = (Map<String, List<RowObject>>) openObjects;
			for (Map.Entry<String, List<RowObject>> entry : ro_list.entrySet()) {
				String tbl = entry.getKey();
				List<RowObject> ro = entry.getValue();
				SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);

				for (int i = 0; i < ro.size(); i++) {
					// store object_id in appToObj
					objects.add(ro.get(i));
				}
			}
			appToObj.put(uid, objects);
		}

		/* YGO: sync with SyncSet when using strong consistency */
		public void syncStrongConsistency(SimbaTable mtbl) {
			// if there was update while writing/updating, remove SyncSet

			// take snapshot before creating SyncRequest message
			ReadOptions ro = SimbaLevelDB.takeSnapshot();
			Map<Integer, Long> obj_list = new HashMap<Integer, Long>();

			SyncHeader h = mtbl.buildDataForSyncingStrong(obj_list);
			if (h == null) {
				Log.d(TAG, "There was update while writing! Do not sync!");
			} else if (h.getDirtyRows().isEmpty()
					&& h.getDeletedRows().isEmpty()) {
				Log.d(TAG, "Nothing to sync!");
				assert false;
			} else {
				Log.d(TAG, "sync header=" + h);
				SyncRequest r = SyncRequest.newBuilder().setData(h).build();
				SimbaMessage.Builder mb = SimbaMessage.newBuilder()
						.setType(SimbaMessage.Type.SYNC_REQUEST)
						.setSeq(r.getData().getTrans_id()).setSyncRequest(r);

				Log.d(TAG, "Sending SYNC_REQUEST! app: " + r.getData().getApp()
						+ ", tbl: " + r.getData().getTbl() + ", trans_id: "
						+ r.getData().getTrans_id());

				networkManager.sendMessageNow(mb, r.getData().getTrans_id());
				SeqNumManager.addPendingSeq(r.getData().getTrans_id(),
						mb.build());

				/* create ObjectFragments */
				for (Map.Entry<Integer, Long> entry : obj_list.entrySet()) {
					int oid = entry.getKey();
					long objID = entry.getValue();
					int numChunks = SimbaLevelDB.getNumChunks(objID);
					for (int i = 0; i < numChunks; i++) {
						byte[] buffer = SimbaLevelDB.getChunk(ro, objID, i);

						ObjectFragment of = ObjectFragment.newBuilder()
								.setTrans_id(r.getData().getTrans_id())
								.setOid(oid).setOffset(i)
								.setData(ByteString.copyFrom(buffer))
								.setEof(i + 1 == numChunks ? true : false)
								.build();
						Log.d(TAG, "Sending all OBJECT_FRAGMENTs! trans_id: "
								+ of.getTrans_id() + ", oid: " + of.getOid()
								+ ", offset: " + of.getOffset() + ", eof: " + of.getEof());

						SimbaMessage.Builder mb_of = SimbaMessage
								.newBuilder()
								.setType(SimbaMessage.Type.OBJECT_FRAGMENT)
								.setSeq(r.getData().getTrans_id())
								.setObjectFragment(of);
						networkManager.sendMessageNow(mb_of, r.getData()
								.getTrans_id());
					}
				}
			}
			// close snapshot after all sync operations are done
			SimbaLevelDB.closeSnapshot(ro);
			mtbl.waitForSyncResponse();
		}
	};

	public IBinder onBind(Intent arg0) {
		return mBinder;
	}

	@Override
	public void onDestroy() {
		// simba_db.execSQL("DROP TABLE metadata;");
		try {
			metadata.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		super.onDestroy();
		isRunning = false;
	}

	public static boolean isRunning() {
		return isRunning;
	}

	// we have made sure this service will only be called once
	@Override
	public int onStartCommand(Intent intent, int flags, int startid) {
		Log.d(TAG, "SCS started");

		return Service.START_NOT_STICKY;

	}

	public static boolean isNetworkConnected() {
		return isConnected;
	}

	public void createTable(String uid, String tbl, String cmd,
			TableProperties props) {
		// FIX: set consistency level from app
		int lvl = SimbaConsistency.Type.STRONG;

		SQLiteDatabase db = client_dbhandle.get(uid);
		String sql = "CREATE TABLE IF NOT EXISTS " + tbl + " (" + cmd + ");";
		db.execSQL(sql);

		/*
		 * add extra fields: row id, revision number, dirty flag (row, obj)
		 */
		try {
			db.execSQL("ALTER TABLE " + tbl + " ADD COLUMN _id VARCHAR;");
			db.execSQL("ALTER TABLE " + tbl
					+ " ADD COLUMN _rev INT DEFAULT -1;");
			db.execSQL("ALTER TABLE " + tbl
					+ " ADD COLUMN _torn BIT DEFAULT 0;");
			db.execSQL("ALTER TABLE " + tbl
					+ " ADD COLUMN _dirty BIT DEFAULT 0;");
			db.execSQL("ALTER TABLE " + tbl
					+ " ADD COLUMN _dirtyObj BIT DEFAULT 0;");
			db.execSQL("ALTER TABLE " + tbl
					+ " ADD COLUMN _openObj INT DEFAULT 0;");
			db.execSQL("ALTER TABLE " + tbl
					+ " ADD COLUMN _sync BIT DEFAULT 0;");
			db.execSQL("ALTER TABLE " + tbl
					+ " ADD COLUMN _conflict BIT DEFAULT 0;");
		} catch (SQLiteException e) {
			/* do nothing: the column already exists */
		}

		SimbaTableManager.addTable(uid, tbl, new SimbaTable(uid, db, tbl,
				cmd, lvl, props, metadata, false));
		metadata.put(uid, tbl, "partial", props.isPartial());
	}

	private void write_sync_oneshot(SimbaTable mtbl, int dt) {
		Map<Integer, Long> obj_list = new HashMap<Integer, Long>();
		SyncHeader d = mtbl.buildDataForSyncing(obj_list);

		if (!d.getDirtyRows().isEmpty() || !d.getDeletedRows().isEmpty()) {
			SyncRequest r = SyncRequest.newBuilder().setData(d).build();
			SimbaMessage.Builder mmsg = SimbaMessage.newBuilder()
					.setType(SimbaMessage.Type.SYNC_REQUEST).setSyncRequest(r);

			// SimbaLogger.log("up, " + mmsg.toString() + ", " +
			// mmsg.build().toString() + ", " + mmsg.build().computeSize());

			syncScheduler.schedule(mmsg, true, dt);

			if (!obj_list.isEmpty()) {

				/* object fragments */
				/* create ObjectFragments */
				ReadOptions ro = SimbaLevelDB.takeSnapshot();
				for (Map.Entry<Integer, Long> entry : obj_list.entrySet()) {
					int oid = entry.getKey();
					long objID = entry.getValue();

					BitSet dirtyChunkList = SimbaChunkList
							.getDirtyChunks(objID);
					for (int i = dirtyChunkList.nextSetBit(0); i >= 0;) {
						int chunk_num = i;
						byte[] buffer = SimbaLevelDB.getChunk(ro, objID,
								chunk_num);
						i = dirtyChunkList.nextSetBit(i + 1);

						ObjectFragment of = ObjectFragment
								.newBuilder()
								.setTrans_id(r.getData().getTrans_id())
								.setOid(oid)
								.setOffset(
										chunk_num
												* SimbaLevelDB.getChunkSize())
								.setData(ByteString.copyFrom(buffer))
								.setEof(i == -1 ? true : false).build();

						Log.d(TAG,
								"Sending OBJECT_FRAGMENT! trans_id: "
										+ of.getTrans_id() + ", oid: "
										+ of.getOid() + ", offset: "
										+ of.getOffset());

						SimbaMessage.Builder mb_of = SimbaMessage
								.newBuilder()
								.setType(SimbaMessage.Type.OBJECT_FRAGMENT)
								.setObjectFragment(of);
						// SimbaLogger.log("up, " + mb_of.toString() + ", " +
						// mb_of.build().toString() + ", " +
						// mb_of.build().computeSize());

						syncScheduler.schedule(mb_of, true, dt);
					}
				}
				SimbaLevelDB.closeSnapshot(ro);
			} else {
				Log.d(TAG, "write_sync_oneshot no objects to sync");
			}
		}
	}

	private boolean unregisterAppInternal(String uid) {

		boolean ret = false;
		if (ClientCallbackManager.clientExists(uid)) {
			ClientCallbackManager.removeClient(uid);

			ConcurrentHashMap<String, SimbaTable> app_tbls = SimbaTableManager
					.getAllTables(uid);
			for (Enumeration<String> enu = app_tbls.keys(); enu
					.hasMoreElements();) {
				String tbl = enu.nextElement();
				writeTimerManager.removeTask(app_tbls.get(tbl));
				// remove dirty chunk list
				SimbaChunkList.removeDirtyChunkList(uid, tbl);
			}
			Log.d(TAG, "Removing mapping for app: " + uid);
			appToObj.remove(uid);
			SimbaTableManager.dropTables(uid);

			// close database, otherwise Android will complain about
			// close() not called exception.
			client_dbhandle.get(uid).close();
			ret = true;
		}

		return ret;
	}

	/* Subscribe Read sync requests to Server */
	private void sub_tbl(String uid, String tbl, int period, int dt) {
		// insert to table bitmap
		Log.d(TAG, "Inserting <" + uid + ", " + tbl + "> to table bitmap");
		tblBitmap.add(uid + "," + tbl);

		SimbaTable mtbl = SimbaTableManager.getTable(uid, tbl);

		SubscribeTable t = SubscribeTable
				.newBuilder()
				.setApp(uid)
				.setTbl(tbl)
				.setDelayTolerance(
						(int) (dt * Preferences.READ_DT_SERVER_RATIO))
				.setPeriod(period).setRev(mtbl.getRev()).build();

		SimbaMessage.Builder b = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.SUB_TBL).setSubscribeTable(t);

		// SimbaLogger.log("up, " + b.toString()); // + ", " +
		// b.build().toString() + ", " + b.build().computeSize());

		networkManager.sendTokenedMessage(b);
	}

	private void unsub_tbl(String uid, String tbl) {
		// remove from table bitmap
		for (String bit : tblBitmap) {
			String[] uid_tid = bit.split("\\,");
			if (uid_tid[1] == tbl) {
				tblBitmap.remove(bit);
				break;
			}
		}

		UnsubscribeTable t = UnsubscribeTable.newBuilder().setApp(uid)
				.setTbl(tbl).build();

		SimbaMessage.Builder b = SimbaMessage.newBuilder()
				.setType(SimbaMessage.Type.UNSUB_TBL).setUnsubscribeTable(t);

		// SimbaLogger.log("up, " + b.toString() + ", " + b.build().toString()
		// + ", " + b.build().computeSize());
		networkManager.sendTokenedMessage(b);
	}

	/* return table name for bitmap id */
	public static String getUidTid(int bitmap_id) {
		return tblBitmap.get(bitmap_id);
	}
}
