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

import java.io.IOException;

import android.app.Activity;
import android.app.ActivityManager;
import android.app.ActivityManager.RunningServiceInfo;
import android.content.BroadcastReceiver;
import android.content.ComponentName;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.ServiceConnection;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.os.Environment;
import android.os.IBinder;
import android.os.RemoteException;
import android.util.Log;
import android.view.View;
import android.view.View.OnFocusChangeListener;
import android.widget.CompoundButton;
import android.widget.CompoundButton.OnCheckedChangeListener;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;
import android.widget.ToggleButton;

/***
 * Driver class to start the service.
 */
public class SimbaActivity extends Activity {
	private static final String TAG = "SimbaActivity";
	private IntentFilter intentFilter = new IntentFilter(
			SimbaBroadcastReceiver.CONNECTION_STATE_CHANGED);

	private BroadcastReceiver broadcastReceiver = new SimbaBroadcastReceiver() {
		@Override
		public void onReceive(Context arg0, Intent intent) {
			boolean networkState = intent.getBooleanExtra(
					SimbaBroadcastReceiver.EXTRA_CONNECTION_STATE, false);
			setNetworkState(networkState);

		}

	};

	SharedPreferences settings;
	EditText hostname;
	EditText port;
	TextView networkState;

	/** Called when the activity is first created. */
	@Override
	public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.main);
		hostname = (EditText) findViewById(R.id.hostname);
		port = (EditText) findViewById(R.id.port);
		networkState = (TextView) findViewById(R.id.network_state);
		// Restore preferences
		settings = getSharedPreferences(Preferences.PREFS_NAME, 0);
		hostname.setText(settings.getString("hostname",
				Preferences.DEFAULT_HOST));


		hostname.setOnFocusChangeListener(new OnFocusChangeListener() {
			@Override
			public void onFocusChange(View v, boolean hasFocus) {
				if (!hasFocus)
					saveSettings();
			}
		});
		port.setText("" + settings.getInt("port", Preferences.DEFAULT_PORT));
		port.setOnFocusChangeListener(new OnFocusChangeListener() {
			@Override
			public void onFocusChange(View v, boolean hasFocus) {
				if (!hasFocus)
					saveSettings();
			}
		});

		add_button_handler();

	}

	@Override
	public void onResume() {
		super.onResume();
		registerReceiver(broadcastReceiver, intentFilter);
		setNetworkState(SimbaContentService.isNetworkConnected());
	}

	@Override
	public void onPause() {
		unregisterReceiver(broadcastReceiver);
		super.onPause();
	}

	private void setNetworkState(boolean connected) {
		networkState.setText(connected ? "Network connected"
				: "Network disconnected");
	}

	private void add_button_handler() {
		ToggleButton tb = (ToggleButton) findViewById(R.id.toggleButton1);
		tb.setOnCheckedChangeListener(new OnCheckedChangeListener() {
			public void onCheckedChanged(CompoundButton buttonView,
					boolean isChecked) {
				if (isChecked) {
					if (!SimbaContentService.isRunning()) {
						Log.v(TAG, "Starting SCS service");
						Intent intent = new Intent(getBaseContext(),
								SimbaContentService.class);

						startService(intent);
					}
				} else {
					stopService(new Intent(getBaseContext(),
							SimbaContentService.class));
					// do nothing for now, since we don't want to turn it off
					Toast.makeText(getBaseContext(), "do nothing",
							Toast.LENGTH_SHORT).show();
				}
			}
		});

		// Logging button
		tb = (ToggleButton) findViewById(R.id.toggleButtonLog);
		tb.setOnCheckedChangeListener(new OnCheckedChangeListener() {
			public void onCheckedChanged(CompoundButton buttonView,
					boolean isChecked) {
				if (isChecked) {
					SimbaLogger.start();
					SimbaLogger.log(SimbaLogger.Type.START,SimbaLogger.Dir.LO,0, "hello world");
				} else {
					SimbaLogger.log(SimbaLogger.Type.END,SimbaLogger.Dir.LO,0,"bye world");
					SimbaLogger.stop();
				}
			}
		});
	}

	protected void onStop() {
		super.onStop();
		saveSettings();

	}
	
	public void onClickQuit(View v) {
		try {
			Runtime.getRuntime().exec("rm -r " + Environment.getExternalStorageDirectory()  + "/SCS");
			Runtime.getRuntime().exec("rm -r " + Environment.getExternalStorageDirectory()  + "/SimbaLevelDB");
		} catch (IOException e) {
			Log.d(TAG, "error while clearing data: " + e.getMessage());
		}
		System.exit(0);
	}


	private void saveSettings() {
		SharedPreferences.Editor editor = settings.edit();
		editor.putString("hostname", hostname.getText().toString());
		editor.putInt("port", Integer.parseInt(port.getText().toString()));
		editor.commit();

	}

}
