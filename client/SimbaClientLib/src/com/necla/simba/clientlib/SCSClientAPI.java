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

import java.util.List;

import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;

import com.necla.simba.client.ISCSClient;
import com.necla.simba.client.CRChoice;
import com.necla.simba.client.ConnState;
import com.necla.simba.client.CursorInputStream;
import com.necla.simba.client.DataObjectRow;
import com.necla.simba.client.DataRow;
import com.necla.simba.client.TableProperties;

/***
 * Interface between user app and Simba client runtime.
 */
public interface SCSClientAPI {
	public void plug(Context context, ISCSClientApp callback1,
			ISCSClientAdapter callback2);

	public void unplug();

	public boolean subscribeTable(String name, int period, int delay,
			ConnState syncpref);

	public boolean createTable(String name, String cmd, int lvl,
			TableProperties props);

	// public boolean updateTable(String name);

	// public boolean dropTable(String name);

	public void closeObject(long obj_id);

	public SCSCursor readData(String tbl, String[] projection,
			String selection, String[] selectionArgs, String sortOrder);

	public List<SCSOutputStream> writeData(String tbl, ContentValues data,
			String[] objectOrdering);

	public List<SCSOutputStream> updateData(String tbl, ContentValues data,
			String selection, String[] selectionArgs, String[] objectOrdering);

	public int deleteData(String tbl, String selection, String[] selectionArgs);

	public void writeSyncOneshot(String tbl, int delay);

	public void registerPeriodicWriteSync(String tbl, int period, int delay,
			ConnState syncpref);

	// not used
	// public void registerPeriodicReadSync(String tbl, int period, int delay,
	// ConnState syncpref);

	public void unregisterPeriodicWriteSync(String tbl);

	// public void updatePeriodicWriteSync(String tbl, int peroid, int delay);

	public void beginCR(String tbl);

	public List<DataObjectRow> getConflictedRows(String tbl);

	public void resolveConflict(String tbl, DataObjectRow row, CRChoice choice);

	public void endCR(String tbl);

	public void readSyncOneshot(String tbl);

	public void subscribePeriodicReadSync(String tbl, int period, int delay,
			ConnState syncpref);

	public void unsubscribePeriodicReadSync(String tbl);

	public boolean isNetworkConnected();
}
