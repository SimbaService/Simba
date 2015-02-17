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
package com.necla.simba.server.simbastore.table;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Properties;

import com.necla.simba.protocol.Common.DataRow;

public class OperationLog {
	// new operation rows
	private FileOutputStream log;
	private ObjectOutputStream oos;

	// rollback safeguard rows
	// private FileOutputStream rlog;
	// private ObjectOutputStream roos;

	private boolean enableLog;

	public OperationLog(String tablename, Properties props) {
        enableLog = Boolean.parseBoolean(props.getProperty("oplog.enable"));
        

		if (enableLog) {
			String logName = null;
			File logsDir = new File("logs");

			if (!logsDir.exists()) {
				logsDir.mkdir();
			}

			logName = logsDir.getName() + "/"+ tablename + ".log";

			try {
				// Initialize operation on-disk log
				log = new FileOutputStream(logName, true); // true = append
				oos = new ObjectOutputStream(log);

				// Initialize operation rollback log
				//true = append
				// log = new FileOutputStream(rollbackLog, true); 
				
				// oos = new ObjectOutputStream(rlog);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void write(DataRow row) {
		if (enableLog) {
			try {
				oos.writeObject(row);
				oos.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void write(String message) {
		if (enableLog) {
			try {
				oos.writeBytes(message + "\n");
				oos.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
