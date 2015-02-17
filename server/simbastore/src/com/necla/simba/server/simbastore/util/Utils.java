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
package com.necla.simba.server.simbastore.util;

import java.io.IOException;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;

import com.datastax.driver.core.Row;
import com.google.protobuf.ByteString;
import com.necla.simba.protocol.Common.Column;
import com.necla.simba.protocol.Common.ColumnData;
import com.necla.simba.protocol.Common.DataRow;
import com.necla.simba.protocol.Common.ObjectHeader;
import com.necla.simba.protocol.Common.SyncHeader;
import com.necla.simba.protocol.Common.SyncResponse;
import com.necla.simba.server.simbastore.util.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);

	/*
	 * Attempts to decode the correct value type given the schema
	 */
	public static ColumnData decodeColumn(String name, Column.Type type, Row row)
			throws IOException {
		ColumnData.Builder cdb = ColumnData.newBuilder();
		cdb.setColumn(name);

		LOG.debug("decode: name=" + name + ", type=" + type);

		// TODO: handle null values!
		switch (type.getNumber()) {
		case Column.Type.VARCHAR_VALUE: {
			String value = row.getString(name);
			if (value != null) {
				cdb.setValue(value);
			}
		}
			break;
		case Column.Type.INT_VALUE: {
			Integer value = row.getInt(name);
			if (value != null) {
				cdb.setValue(Integer.toString(value));
			}
		}
			break;
		case Column.Type.UUID_VALUE: {
			UUID value = row.getUUID(name);

			if (value != null) {
				cdb.setValue(value.toString());
			}
		}
			break;
		case Column.Type.BOOLEAN_VALUE: {
			Boolean value = row.getBool(name);
			if (value != null) {
				cdb.setValue(value.toString());
			}
		}
			break;
		case Column.Type.BIGINT_VALUE:
		case Column.Type.COUNTER_VALUE: {
			Long value = row.getLong(name);
			if (value != null) {
				cdb.setValue(Long.toString(value));
			}
		}
			break;
		case Column.Type.BLOB_VALUE: {
			ByteBuffer value = row.getBytes(name);
			if (value != null) {
				cdb.setValue(value.toString());
			}
		}
			break;
		case Column.Type.DOUBLE_VALUE: {
			Double value = row.getDouble(name);
			if (value != null) {
				cdb.setValue(Double.toString(value));
			}
		}
			break;
		case Column.Type.FLOAT_VALUE: {
			Float value = row.getFloat(name);
			if (value != null) {
				cdb.setValue(Float.toString(value));
			}
		}
			break;
		case Column.Type.TIMESTAMP_VALUE: {
			Date value = row.getDate(name);
			if (value != null) {
				cdb.setValue(value.toString());
			}
		}
			break;
		case Column.Type.VARINT_VALUE: {
			BigInteger value = row.getVarint(name);
			if (value != null) {
				cdb.setValue(value.toString());
			}
		}
			break;
		case Column.Type.INET_VALUE: {
			InetAddress value = row.getInet(name);
			if (value != null) {
				cdb.setValue(value.toString());
			}
		}
			break;
		default:
			throw new IOException("No conversion for type " + type + " found!");
		}
		return cdb.build();
	}

	public static DataRow rowToDataRow(Row row, boolean getObjects,
			Integer object_counter, List<List<UUID>> objects,
			List<Column> schema) throws IOException {
		// Create DataRow
		DataRow.Builder newRow = DataRow.newBuilder();
		newRow.setId(row.getString("key")).setRev(row.getInt("version"));

		// Get rest of columns/values
		for (Column c : schema) {
			// object columns
			if (getObjects && c.getType() == Column.Type.OBJECT) {

				List<UUID> chunks = row.getList(c.getName(), UUID.class);

				if (chunks != null && chunks.size() > 0) {

					// Create ObjectHeader for SyncHeader mesasge
					ObjectHeader h = ObjectHeader.newBuilder()
							.setColumn(c.getName()).setOid(object_counter)
							.build();

					newRow.addObj(h);

					// Save chunk UUIDs to GET from Swift later
					// objects.add(row.getUUID(c.getName()));
					objects.add(chunks);

					// increment our transaction object id counter
					object_counter++;
				}
			} else {
				// non object columns
				newRow.addData(Utils.decodeColumn(c.getName(), c.getType(), row));
			}
		}

		// build this row
		return newRow.build();
	}

	public static DataRow rowToDataRow2(Row row, boolean getObjects,
			Integer object_counter, List<Set<Entry<Integer, UUID>>> objects,
			List<Column> schema) throws IOException {
		// Create DataRow
		DataRow.Builder newRow = DataRow.newBuilder();
		newRow.setId(row.getString("key")).setRev(row.getInt("version"));

		// Get rest of columns/values
		for (Column c : schema) {
			// object columns
			if (getObjects && c.getType() == Column.Type.OBJECT) {

				List<UUID> chunks = row.getList(c.getName(), UUID.class);

				if (chunks != null && chunks.size() > 0) {
					// Create ObjectHeader for SyncHeader message
					ObjectHeader h = ObjectHeader.newBuilder()
							.setColumn(c.getName()).setOid(object_counter)
							.build();

					newRow.addObj(h);

					// Save chunk UUIDs to GET from Swift later
					Set<Entry<Integer, UUID>> objset = new HashSet<Entry<Integer, UUID>>();
					for (int i = 0; i < chunks.size(); i++) {
						objset.add(new AbstractMap.SimpleEntry<Integer, UUID>(
								i, chunks.get(i)));
					}
					// objects.add(row.getUUID(c.getName()));
					objects.add(objset);

					// increment our transaction object id counter
					object_counter++;
				}
			} else {
				// non object columns
				newRow.addData(Utils.decodeColumn(c.getName(), c.getType(), row));
			}
		}

		// build this row
		return newRow.build();
	}
}
