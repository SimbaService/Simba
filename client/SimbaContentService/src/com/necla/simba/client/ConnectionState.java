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

import android.content.Context;
import android.net.ConnectivityManager;
import android.net.NetworkInfo;
import android.telephony.TelephonyManager;
import android.util.Log;

public class ConnectionState {

	private Context context;

	public ConnectionState(Context c) {
		this.context = c;
	}

	public ConnState getConnectionState() {

		ConnState nwconnState = ConnState.NONE;
		ConnectivityManager cm = (ConnectivityManager) context
				.getSystemService(Context.CONNECTIVITY_SERVICE);
		TelephonyManager tm = (TelephonyManager) context
				.getSystemService(Context.TELEPHONY_SERVICE);
		NetworkInfo net_info = cm.getActiveNetworkInfo();

		if (net_info != null && net_info.isConnected()) {

			switch (net_info.getType()) {
			case (ConnectivityManager.TYPE_WIFI):
				nwconnState = ConnState.WIFI;
				break;

			// Fix this switch case and account for each network type
			case (ConnectivityManager.TYPE_MOBILE): {
				switch (tm.getNetworkType()) {
				case TelephonyManager.NETWORK_TYPE_LTE:
					nwconnState = ConnState.FG;
					break;
				case TelephonyManager.NETWORK_TYPE_EHRPD:
				case TelephonyManager.NETWORK_TYPE_EDGE:
				case TelephonyManager.NETWORK_TYPE_GPRS:
				case TelephonyManager.NETWORK_TYPE_HSPAP:
				case TelephonyManager.NETWORK_TYPE_HSPA:
					nwconnState = ConnState.TG;
					break;
				default:
					break;
				}
				break;
			}
			default:
				break;
			}
		}

		return nwconnState;
	}
}
