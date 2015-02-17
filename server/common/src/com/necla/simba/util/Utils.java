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
package com.necla.simba.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import com.necla.simba.protocol.Common.SyncHeader;
import com.necla.simba.protocol.Common.SyncResponse;

/**
 * 
 * Random crap of shared static functions
 * 
 * @author aranya
 * 
 */
public class Utils {
	// simpler, less verbose methods for logging some stuff
	public static String stringify(SyncHeader sd) {
		StringBuffer sb = new StringBuffer();
		sb.append(SyncHeader.class.getCanonicalName());
		sb.append("(app = ").append(sd.getApp()).append(" tbl = ")
				.append(sd.getTbl()).append(" dirtyRows.size = ")
				.append(sd.getDeletedRowsCount())
				.append(" deletedRows.size = ")
				.append(sd.getDeletedRowsCount()).append(")");

		return sb.toString();
	}

	public static String stringify(SyncResponse sr) {
		StringBuffer sb = new StringBuffer();
		sb.append(SyncResponse.class.getCanonicalName());
		sb.append("(result = ").append(sr.getResult())
				.append(" okRows.size = ").append(sr.getSyncedRowsCount())
				.append(" conflictedRows.size =")
				.append(sr.getConflictedRowsCount()).append(")");

		return sb.toString();
	}


	public static String chunkListToString(List<UUID> values) {
		StringBuilder list = new StringBuilder();

		list.append("[").append(values.get(0));

		for (int i = 1; i < values.size(); i++) {
			list.append(", ").append(values.get(i));
		}

		list.append("]");
		return list.toString();
	}
	
	public static Properties loadProperties(String propFile) throws IOException {
		Properties properties = new Properties();
		properties.load(Utils.class
					.getResourceAsStream("/" + propFile));
		// load properties from the same file existing in current directory
		File f = new File(propFile);
		if (f.exists())
			properties.load(new FileInputStream(f));
		
		return properties;
		
	}
}
