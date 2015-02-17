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

import java.util.ArrayList;
import java.util.List;

import android.database.Cursor;

import com.necla.simba.clientlib.SCSInputStream;

/**
 * @author Younghwan Go
 * @created Jul 17, 2013 5:41:50 PM
 * 
 */
public class CursorInputStream {
	private Cursor cursor;
	private List<SCSInputStream> mis;

	public CursorInputStream() {
		this.cursor = null;
		this.mis = new ArrayList<SCSInputStream>();
	}

	public Cursor getCursor() {
		return cursor;
	}

	public List<SCSInputStream> getSCSInputStream() {
		return mis;
	}

	public void setCursor(Cursor cursor) {
		this.cursor = cursor;
	}

	public void addSCSInputStream(SCSInputStream mis) {
		this.mis.add(mis);
	}
}
