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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.UUID;

/***
 * Utility class that manages sensitive information for Simba client: device
 * ID, registration user name and password.
 * For device ID, it generates (if needed) an id for the device. The only
 * requirement is that it should be unique for the device when registering with
 * Simba server.
 * For user name and password, it can be read from some configuration file or
 * database. Currently they are returned as dummy values.
 * 
 * @file WalletManager.java
 * @author shao@nec-labs.com
 * @created 11:15:17 AM, Aug 6, 2012
 * @modified 11:15:17 AM, Aug 6, 2012
 */
public class WalletManager {
	private static String id = null;

	public synchronized static String getDeviceID() {
		File file = new File(Preferences.FILE_WALLET);

		try {
			if (file.exists()) {
				BufferedReader br = new BufferedReader(new FileReader(file));
				id = br.readLine();
				br.close();
			} else {
				id = UUID.randomUUID().toString();
				PrintWriter pw = new PrintWriter(file);
				pw.println(id);
				pw.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return id;
	}

	public synchronized static String regenerateDeviceID() {
		if (id == null)
			return WalletManager.getDeviceID();

		String newId = "";
		do {
			newId = UUID.randomUUID().toString();
		} while (newId.equals(id));

		id = newId;
		try {
			PrintWriter pw = new PrintWriter(new File(Preferences.FILE_WALLET));
			pw.println(id);
			pw.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return newId;
	}

	public synchronized static String getUserID() {
		return "user";
	}

	public synchronized static String getUserPassword() {
		return "pass";
	}
}
