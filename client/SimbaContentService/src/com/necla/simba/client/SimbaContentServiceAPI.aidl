package com.necla.simba.client;

import android.content.ContentValues;

import com.necla.simba.client.ISCSClient;
import com.necla.simba.client.InternalDataRow;
import com.necla.simba.client.CRChoice;
import com.necla.simba.client.ConnState;
import com.necla.simba.client.TableProperties;
import com.necla.simba.client.SimbaCursorWindow;
import com.necla.simba.client.RowObject;

interface SimbaContentServiceAPI {
    /* security related */
  	String registerApp(in String uid, in ISCSClient client);
  	boolean unregisterApp(in String tid);
  
    /* CUD operations for table schema */
    boolean subscribeTable(in String tid, in String tbl, in int period, in int delay, in ConnState syncpref);
  	boolean createTable(in String tid, in String tbl, in String cmd, in int lvl, in TableProperties props);
  	// boolean updateTable(in String tid, in String tbl, in String cmd);
  	// boolean dropTable(in String tid, in String tbl, in String cmd);
  
    /* CRUD operations for table entries */
  	List<RowObject> write(in String tid, in String tbl, in ContentValues values, in String[] objectOrdering);
  	SimbaCursorWindow read(in String tid, in String tbl, in String[] projection, in String selection, in String[] selectionArgs, in String sortOrder);
  	List<RowObject> update(in String tid, in String tbl, in ContentValues values, in String selection, in String[] selectionArgs, in String[] objectOrdering);
  	int delete(in String tid, in String tbl, in String selection, in String[] selectionArgs);
  	
  	/* Delay-Tolerant write sync API */
  	void writeSyncOneshot(in String tid, in String tbl, in int delay);
  	void registerPeriodicWriteSync(in String tid, in String tbl, in int period, in int delay, in ConnState syncpref);
  	// void updatePeriodicWriteSync(in String tid, in String tbl, in int period, in int delay);
  	void unregisterPeriodicWriteSync(in String tid, in String tbl);
  	
  	/* Subscribe read sync on server */
  	void readSyncOneshot(in String tid, in String tbl);
  	//void registerPeriodicReadSync(in String tid, in String tbl, in int period, in int delay, in ConnState syncpref);
  	void subscribePeriodicReadSync(in String tid, in String tbl, in int period, in int delay, in ConnState syncpref);
  	void unsubscribePeriodicReadSync(in String tid, in String tbl);
  	
  	/* Conflict Resolution */
  	void beginCR(in String tid, in String tbl);
  	List<InternalDataRow> getConflictedRows(in String tid, in String tbl);
  	void resolveConflict(in String tid, in String tbl, in String id, in CRChoice choice);
  	void endCR(in String tid, in String tbl);
  	
  	/* Get network state */
  	boolean isNetworkConnected();
  	
  	/* Get SQL Table Schema */
  	String getSchemaSQL(in String tid, in String tbl);
  	
  	/* LevelDB Object Handler */
  	int writeStream(in String tid, in String tbl, in long obj_id, in int chunk_num, in byte[] buffer, in int offset, in int length);
  	int truncate(in String tid, in String tbl, in String row_id, in long obj_id, in int length);
  	int readStream(in long obj_id, out byte[] buffer, in int buffer_off, in int offset, in int length);
  	void decrementObjCounter(in String tid, in String tbl, in long obj_id);
  	
  	/* send opened objects from app */
  	void setOpenObjects(in String tid, in Map openObjects);
}
