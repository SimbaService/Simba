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
package com.necla.simba.apps.notes;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Random;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.graphics.Bitmap;
import android.graphics.BitmapFactory;
import android.os.AsyncTask;
import android.os.Bundle;
import android.os.Environment;
import android.provider.Settings.Secure;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.CompoundButton;
import android.widget.CompoundButton.OnCheckedChangeListener;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;
import android.widget.ToggleButton;

import com.necla.simba.apps.notes.R;
import com.necla.simba.client.CRChoice;
import com.necla.simba.client.ConnState;
import com.necla.simba.client.DataObjectRow;
import com.necla.simba.client.TableProperties;
import com.necla.simba.clientlib.ISCSClientAdapter;
import com.necla.simba.clientlib.ISCSClientApp;
import com.necla.simba.clientlib.SCSClientAPI;
import com.necla.simba.clientlib.SCSClientAdapter;
import com.necla.simba.clientlib.SCSCursor;
import com.necla.simba.clientlib.SCSInputStream;
import com.necla.simba.clientlib.SCSOutputStream;

/***
 * Simple SimbaNoteApp for testing
 * 
 * @file Main.java
 * @author Younghwan Go
 * @created 6:46:37 PM, Oct 5, 2012
 * @modified 2:07:32 PM, Feb 10, 2015
 */
public class Main extends Activity {

	private class StatContainer {

		public void reset() {
			nops = 0;
			totalTime = 0;
		}

		public void accumulate(long val) {
			times[nops++] = val;
			totalTime += val;
		}

		public double mean() {
			if (nops == 0)
				return Double.NaN;

			return totalTime / nops;
		}

		private double var() {
			double sum = 0.0;
			if (nops < 2)
				return Double.NaN;
			double avg = mean();

			for (int i = 0; i < nops; i++) {
				sum += (times[i] - avg) * (times[i] - avg);
			}
			return sum / (nops - 1);
		}

		public double stddev() {
			return Math.sqrt(var());
		}

		public long last() {
			return times[nops - 1];
		}

		public int nops() {
			return nops;
		}

		private long[] times = new long[1000];
		private int nops = 0;
		private long totalTime = 0;

	}

	private final String TAG = "SimbaNotesApp";
	private SCSClientAPI adapter;
	private Context context;
	private ISCSClientApp notify_callback;
	private ISCSClientAdapter ready_callback;
	private boolean init = false;
	private final String TBL = "notes";
	private String title, content;
	private SimpleDateFormat dateFormatter = new SimpleDateFormat(
			"HH:mm:ss.SSS");

	private TextView console = null, newDataView = null,
			conflictDataView = null;
	private ImageView newImageView = null, conflictImageView = null;

	/* generate random string */
	static final String AB = "ABCDEF4HIJKLMNOPQRSTUVWXYZ";
	static Random rnd = new Random();
	private StatContainer localUpdateTime = new StatContainer();
	private StatContainer syncTime = new StatContainer();
	private StatContainer localReadTime = new StatContainer();

	private String identifier;

	enum ClientType {
		WRITER, READER, CONFLICTOR
	}

	private ClientType myType = ClientType.WRITER;

	public static String randomString(int len) {
		StringBuilder sb = new StringBuilder(len);
		for (int i = 0; i < len; i++)
			sb.append(AB.charAt(rnd.nextInt(AB.length())));
		return sb.toString();
	}

	private void init() {
		context = this.getApplicationContext();

		identifier = null;
		TelephonyManager tm = (TelephonyManager) context
				.getSystemService(Context.TELEPHONY_SERVICE);
		if (tm != null) {
			identifier = tm.getDeviceId();
			Log.d(TAG, "DEVICE_ID = " + identifier);
		}

		if (identifier == null || identifier.length() == 0) {
			identifier = Secure.getString(context.getContentResolver(),
					Secure.ANDROID_ID);
			Log.d(TAG, "ANDROID_ID = " + identifier);
		}


		// used for setting client types with phone ids
		if (identifier.equals("268435460715619591")) {
			myType = ClientType.WRITER;
			Log.d(TAG, "THIS DEVICE IS THE WRITER!");
		} else if (identifier.equals("a9be645046f356ca")) {
			myType = ClientType.READER;
			Log.d(TAG, "THIS DEVICE IS THE READER!");
		} else {
			myType = ClientType.CONFLICTOR;
		}

		Log.d(TAG, "imei=" + identifier);
		// Log.d("CONTEXTCONTEXTCONTEXT", "context="+context);
		notify_callback = new ISCSClientApp() {
			public void newData(String table, final int rows,
					final int numDeletedRows) {
				new NewDataTask().execute(System.currentTimeMillis());
				/*
				 * runOnUiThread(new Runnable() { public void run() {
				 * newDataView.setText("New: " + rows + " Del: " +
				 * numDeletedRows); Log.d(TAG, "\n\nSynced: " +
				 * dateFormatter.format(System .currentTimeMillis()) +
				 * "\n\n\n"); } });
				 */
			}

			public void syncConflict(String table, final int rows) {
				new ConflictFixTask().execute();
				/*
				 * runOnUiThread(new Runnable() { public void run() {
				 * conflictDataView.setText("Conflict: " + rows); } });
				 */
			}

			public void subscribeDone() {
				Log.d(TAG, "Subscription completed!");
				// adapter.registerPeriodicWriteSync(TBL, 10000, 5000,
				// ConnState.WIFI);
				// adapter.registerPeriodicReadSync(TBL, 10000, 5000,
				// ConnState.WIFI);
			}
		};

		ready_callback = new ISCSClientAdapter() {

			public void disconnected() {
				disableUI();
			}

			public void ready() {
				enableUI();
				if (!init) {
					// STRONG

					// case 1) create table from app
					adapter.createTable(TBL,
							"timestamp VARCHAR, comment VARCHAR, obj_1 BIGINT",
							/* STRONG */1, new TableProperties(false));

					// case 2) subscribe to a table with read sync setting
					// subscribeTable (tablename, period, dt, ...)

					if (myType == ClientType.WRITER) {
						 adapter.registerPeriodicWriteSync(TBL, 1000, 0,
						 ConnState.WIFI);
					} else if (myType == ClientType.CONFLICTOR) {

						adapter.registerPeriodicWriteSync(TBL, 1000, 0,
								ConnState.WIFI);
						adapter.subscribePeriodicReadSync(TBL, 1000, 0,
								ConnState.WIFI);

					} else if (myType == ClientType.READER) {
						 adapter.subscribePeriodicReadSync(TBL, 1000, 0,
						 ConnState.WIFI);
					}

					// CAUSAL OR EVENTUAL
					// case 1) create table from app

					// adapter.createTable(TBL,
					// "timestamp VARCHAR, obj_1 BIGINT",
					// /* CAUSAL */3, new TableProperties(false));

					// adapter.createTable(TBL,
					// "timestamp VARCHAR, obj_1 BIGINT",
					// /* EVENTUAL */4, new TableProperties(false));

					// case 2) subscribe to a table with read sync setting
					// subscribeTable (tablename, period, dt, ...)

					// adapter.registerPeriodicWriteSync(TBL, 1, 0,
					// ConnState.FG);
					// adapter.subscribePeriodicReadSync(TBL, 1, 0,
					// ConnState.FG);

					init = true;

				}
			}

			public void networkState(boolean arg0) {
				Log.v(TAG, "simba network state=" + arg0);
			}
		};
		registerClickListeners();

		load_dictionary();
	}

	private void load_dictionary() {
		File file = new File(Environment.getExternalStorageDirectory()
				+ "/SimbaNotes/dict.txt");
		File ofp = file.getParentFile();
		if (!ofp.exists()) {
			ofp.mkdirs();
		}

		if (file.exists()) {
			try {
				BufferedReader br = new BufferedReader(new FileReader(file));
				title = br.readLine();
				content = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			try {
				PrintWriter pw = new PrintWriter(file);
				Random R = new Random();

				title = gen_word(R);
				content = gen_word(R);

				pw.println(title);
				pw.println(content);

				pw.flush();
				pw.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	private String gen_word(Random R) {
		int len = 3 + R.nextInt(7);
		char[] arr = new char[len];
		for (int i = 0; i < len; i++) {
			arr[i] = (char) ('a' + R.nextInt(26));
		}

		return new String(arr);
	}

	private void registerClickListeners() {
		console = (TextView) findViewById(R.id.textView4);
		console.setMovementMethod(new ScrollingMovementMethod());
		newImageView = (ImageView) findViewById(R.id.imageView1);
		conflictImageView = (ImageView) findViewById(R.id.imageView2);

		newDataView = (TextView) findViewById(R.id.textView2);
		conflictDataView = (TextView) findViewById(R.id.textView3);

		View v;
		Button btn;

		// write data
		v = findViewById(R.id.button1);
		btn = (Button) v;
		btn.setOnClickListener(new OnClickListener() {
			public void onClick(View arg0) {
				new BatchWriteTask().execute();
			}

		});

		// read data
		v = findViewById(R.id.button2);
		btn = (Button) v;
		btn.setOnClickListener(new OnClickListener() {
			public void onClick(View arg0) {
				new BatchReadTask().execute();
			}
		});

		// update data (update local row)
		v = findViewById(R.id.update50);
		btn = (Button) v;
		btn.setOnClickListener(new OnClickListener() {
			public void onClick(View arg0) {
				localUpdateTime.reset();
				new BatchUpdateTask().execute(50);
			}
		});
		v = findViewById(R.id.update1);
		btn = (Button) v;
		btn.setOnClickListener(new OnClickListener() {
			public void onClick(View arg0) {
				new BatchUpdateTask().execute(1);
			}
		});

		// delete data (delete entire row)
		v = findViewById(R.id.button4);
		btn = (Button) v;
		btn.setOnClickListener(new OnClickListener() {
			public void onClick(View arg0) {
				new BatchDeleteTask().execute();
			}
		});

		// Conflict (check conflict)
		v = findViewById(R.id.button7);
		btn = (Button) v;
		btn.setOnClickListener(new OnClickListener() {
			public void onClick(View arg0) {
				new BatchConflictTask().execute();
			}
		});

		// CR (always choose local copy)
		v = findViewById(R.id.button5);
		btn = (Button) v;
		btn.setOnClickListener(new OnClickListener() {
			public void onClick(View arg0) {
				new BatchCRLTask().execute();
			}
		});

		// CR (always choose remote copy)
		v = findViewById(R.id.button6);
		btn = (Button) v;
		btn.setOnClickListener(new OnClickListener() {
			public void onClick(View arg0) {
				new BatchCRRTask().execute();
			}
		});

		btn = (Button) findViewById(R.id.resetStats);
		btn.setOnClickListener(new OnClickListener() {
			public void onClick(View arg0) {
				localUpdateTime.reset();
				syncTime.reset();
				localReadTime.reset();
			}
		});

		// Finally, the main plug button
		ToggleButton tb = (ToggleButton) findViewById(R.id.toggleButton1);
		tb.setOnCheckedChangeListener(new OnCheckedChangeListener() {
			public void onCheckedChanged(CompoundButton buttonView,
					boolean isChecked) {
				if (isChecked) {
					adapter.plug(context, notify_callback, ready_callback);
				} else {
					adapter.unplug();
					disableUI();
				}
			}
		});
	}

	public void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		setContentView(R.layout.main);

		adapter = new SCSClientAdapter();
		init();
	}

	private void enableUI() {
		TextView tv = (TextView) findViewById(R.id.textView1);
		tv.setText("Client API: Connected");

		findViewById(R.id.textView2).setVisibility(View.VISIBLE);
		findViewById(R.id.textView3).setVisibility(View.VISIBLE);
		findViewById(R.id.button1).setVisibility(View.VISIBLE);
		findViewById(R.id.button2).setVisibility(View.VISIBLE);
		findViewById(R.id.update50).setVisibility(View.VISIBLE);
		findViewById(R.id.update1).setVisibility(View.VISIBLE);
		findViewById(R.id.button4).setVisibility(View.VISIBLE);
		findViewById(R.id.button5).setVisibility(View.VISIBLE);
		findViewById(R.id.button6).setVisibility(View.VISIBLE);
		findViewById(R.id.button7).setVisibility(View.VISIBLE);
		findViewById(R.id.textView4).setVisibility(View.VISIBLE);
		findViewById(R.id.imageView1).setVisibility(View.VISIBLE);
		findViewById(R.id.imageView2).setVisibility(View.VISIBLE);
		findViewById(R.id.textView5).setVisibility(View.VISIBLE);
		findViewById(R.id.textView6).setVisibility(View.VISIBLE);
	}

	private void disableUI() {
		TextView tv = (TextView) findViewById(R.id.textView1);
		tv.setText("Client API: Not Ready");

		findViewById(R.id.textView2).setVisibility(View.GONE);
		findViewById(R.id.textView3).setVisibility(View.GONE);
		findViewById(R.id.button1).setVisibility(View.GONE);
		findViewById(R.id.button2).setVisibility(View.GONE);
		findViewById(R.id.update50).setVisibility(View.GONE);
		findViewById(R.id.update1).setVisibility(View.GONE);
		findViewById(R.id.button4).setVisibility(View.GONE);
		findViewById(R.id.button5).setVisibility(View.GONE);
		findViewById(R.id.button6).setVisibility(View.GONE);
		findViewById(R.id.button7).setVisibility(View.GONE);
		findViewById(R.id.textView4).setVisibility(View.GONE);
		findViewById(R.id.imageView1).setVisibility(View.GONE);
		findViewById(R.id.imageView2).setVisibility(View.GONE);
		findViewById(R.id.textView5).setVisibility(View.GONE);
		findViewById(R.id.textView6).setVisibility(View.GONE);
		ToggleButton tb = (ToggleButton) findViewById(R.id.toggleButton1);
		tb.setChecked(false);

	}

	private class NewDataTask extends AsyncTask<Long, Void, String> {
		protected String doInBackground(Long... begin) {
			boolean ignore = false;
			StringBuilder sb = new StringBuilder();
			int i = 0;
			long st = 0;

			sb.append("Start: "
					+ dateFormatter.format(System.currentTimeMillis()));
			Log.d(TAG, "Begin read!");
			long before = System.currentTimeMillis();

			SCSCursor cursor = adapter.readData(TBL, null, null, null, null);
			if (cursor.getCount() > 0) {
				cursor.moveToFirst();
				long writeTime = Long.parseLong(cursor.getString(1));
				ignore = cursor.getString(2).equals("bad");
				Log.d(TAG, "data created at=" + writeTime);

				st = begin[0] - writeTime;

				if (!ignore) {
					List<SCSInputStream> mis = cursor.getInputStream();
					for (SCSInputStream m : mis) {
						byte[] buf = new byte[1024 * 1024];
						m.read(buf);

						Log.d(TAG, "Read row " + i + ": " + cursor.getString(0));
						i++;
					}
				}
			}

			cursor.close();
			long after = System.currentTimeMillis();

			if (ignore) {
				sb.append("\nIgnored new data");
			} else {
				updateAndShowOn(newDataView, syncTime, "syncTime", st);
				updateAndShowStats(localReadTime, "localReadTime", after
						- before);
			}

			sb.append("\nEnd: "
					+ dateFormatter.format(System.currentTimeMillis()));
			Log.d(TAG, "End read!");

			return sb.toString();
		}

		protected void onPostExecute(String s) {
			Log.d(TAG, s);
		}
	}

	private class ConflictFixTask extends AsyncTask<Void, Void, String> {
		protected String doInBackground(Void... begin) {
			int i = 0;

			adapter.beginCR(TBL);
			List<DataObjectRow> rows = adapter.getConflictedRows(TBL);
			for (DataObjectRow row : rows) {
				if (i % 2 == 0)
					adapter.resolveConflict(TBL, row, CRChoice.MINE);
				++i;
			}
			adapter.endCR(TBL);
			return "" + i;
		}

		@Override
		protected void onPostExecute(String s) {
			Toast.makeText(Main.this, s + " conflicts fixed",
					Toast.LENGTH_SHORT).show();
		}
	}

	private class BatchWriteTask extends AsyncTask<Void, Void, String> {
		protected String doInBackground(Void... arg0) {
			localUpdateTime.reset();
			StringBuilder sb = new StringBuilder();
			byte[] buffer = new byte[100 * 1024];
			rnd.nextBytes(buffer);

			ContentValues cv;
			List<SCSOutputStream> mos_list;
			int i;

			final long before = System.currentTimeMillis();
			sb.append("Start: " + dateFormatter.format(before));
			Log.d(TAG, "Begin write!");

			for (i = 0; i < 1; i++) {
				cv = new ContentValues();
				long now = System.currentTimeMillis();
				cv.put("timestamp", "" + now);
				cv.put("comment", myType == ClientType.WRITER ? "good" : "bad");
				// insert object
				cv.put("obj_1", "");
				String[] objectOrdering = new String[] { "obj_1" };

				mos_list = adapter.writeData(TBL, cv, objectOrdering);

				// write to leveldb
				int j = 0;
				for (SCSOutputStream mos : mos_list) {
					mos.writeStream(adapter, buffer);
					mos.close(adapter);
					j++;
				}

				Log.d(TAG, "Wrote row " + i);

			}
			final long after = System.currentTimeMillis();

			sb.append("\nEnd: "
					+ dateFormatter.format(System.currentTimeMillis()));
			Log.d(TAG, "End write!");

			updateAndShowStats(localUpdateTime, "write", after - before);

			return sb.toString();
		}

		protected void onPostExecute(String s) {
			Log.d(TAG, s);
		}
	}

	private void resetStats(final StatContainer cont) {
		runOnUiThread(new Runnable() {

			@Override
			public void run() {
				cont.reset();

				console.setText("num ops=0");
			}
		});

	}

	private void updateAndShowStats(final StatContainer cont,
			final String extra, final long diff) {

		updateAndShowOn(console, cont, extra, diff);
	}

	private void updateAndShowOn(final TextView tv, final StatContainer cont,
			final String extra, final long diff) {
		runOnUiThread(new Runnable() {

			@Override
			public void run() {

				cont.accumulate(diff);

				String s = String.format(
						"num_ops=%d last=%d avg=%.2f stddev=%.2f %s",
						cont.nops(), cont.last(), cont.mean(), cont.stddev(),
						extra);

				tv.setText(s);
			}
		});

	}

	private class BatchReadTask extends AsyncTask<Void, Void, String> {
		protected String doInBackground(Void... arg0) {
			StringBuilder sb = new StringBuilder();
			int i = 0;

			sb.append("Start: "
					+ dateFormatter.format(System.currentTimeMillis()));
			Log.d(TAG, "Begin read!");

			SCSCursor cursor = adapter.readData(TBL, null, null, null, null);
			cursor.moveToFirst();
			do {
				List<SCSInputStream> mis = cursor.getInputStream();
				for (SCSInputStream m : mis) {
					byte[] buf = new byte[1024 * 1024];
					m.read(buf);

					Log.d(TAG, "Read row " + i + ": " + cursor.getString(0));
					i++;
				}
			} while (cursor.moveToNext());
			cursor.close();

			sb.append("\nEnd: "
					+ dateFormatter.format(System.currentTimeMillis()));
			Log.d(TAG, "End read!");

			return sb.toString();
		}

		protected void onPostExecute(String s) {
			Log.d(TAG, s);
		}
	}

	SCSOutputStream tempStream;

	private class BatchUpdateTask extends AsyncTask<Integer, Void, Integer> {
		protected Integer doInBackground(Integer... arg0) {
			byte[] buffer = new byte[100 * 1024];
			rnd.nextBytes(buffer);
			String row_id = null;
			int i = 0;

			sb.append("Start: "
					+ dateFormatter.format(System.currentTimeMillis()));
			Log.d(TAG, "Begin update!");

			SCSCursor cursor = adapter.readData(TBL,
					new String[] { "timestamp, obj_1" }, null, null, null);
			if (cursor.getCount() > 0) {
				cursor.moveToFirst();
				row_id = cursor.getString(0);
			}
			cursor.close();

			if (row_id == null)
				return i;

			String[] objectOrdering = new String[] { "obj_1" };

			for (; i < arg0[0]; i++) {
				ContentValues cv = new ContentValues();
				cv.put("obj_1", "");
				cv.put("comment", myType == ClientType.WRITER ? "good" : "bad");
				final long before = System.currentTimeMillis();
				Log.d(TAG, "update ts=" + before);

				cv.put("timestamp", "" + before);

				List<SCSOutputStream> mos_list = adapter.updateData(TBL, cv,
						"_id = ?", new String[] { row_id }, objectOrdering);

				// TEST 1) update
				// write to leveldb
				for (SCSOutputStream mos : mos_list) {
					mos.writeStream(adapter, buffer);
					mos.close(adapter);
				}

				// TEST 2) truncate
				/*
				 * for (SCSOutputStream mos : mos_list) { mos.truncate(adapter,
				 * 1024); tempStream = mos; mos.close(adapter); }
				 */

				final long after = System.currentTimeMillis();
				Log.d(TAG, "update time=" + (after - before));
				updateAndShowStats(localUpdateTime, "update", after - before);
				try {
					Thread.sleep(2000);
				} catch (InterruptedException e) {
				}
			}

			sb.append("\nEnd: "
					+ dateFormatter.format(System.currentTimeMillis()));
			Log.d(TAG, "End update!");

			Log.d(TAG, sb.toString());
			sb.setLength(0);

			// cursor.close();
			return i;
		}

		protected void onPostExecute(Integer cnt) {
			// console.setText("Updated " + cnt.intValue() + " local rows...");
		}
	}

	StringBuilder sb = new StringBuilder();

	private class BatchDeleteTask extends AsyncTask<Void, Void, String> {
		protected String doInBackground(Void... arg0) {
			StringBuilder sb = new StringBuilder();
			sb.append("Start: "
					+ dateFormatter.format(System.currentTimeMillis()));
			Log.d(TAG, "Begin delete!");

			adapter.deleteData(TBL, null, null);

			sb.append("\nEnd: "
					+ dateFormatter.format(System.currentTimeMillis()));
			Log.d(TAG, "End delete!");

			return sb.toString();
		}

		protected void onPostExecute(String s) {
			Log.d(TAG, s);
		}
	}

	private class BatchConflictTask extends AsyncTask<Void, Void, StringBitmap> {
		protected StringBitmap doInBackground(Void... arg0) {
			StringBuilder sb = new StringBuilder();
			Bitmap image = null;
			Bitmap conflict = null;

			Log.d(TAG, "TIME:" + String.valueOf(System.currentTimeMillis()));

			adapter.beginCR(TBL);

			List<DataObjectRow> rows = adapter.getConflictedRows(TBL);
			Log.v(TAG, "Got " + rows.size() + " rows...");

			for (int i = 0; i < rows.size(); i++) {
				List<SCSInputStream> mis = rows.get(i).getSCSInputStream();
				for (SCSInputStream m : mis) {
					byte[] buf = new byte[84992];
					int len = m.read(buf);
					if (i % 2 == 0) {
						sb.append("Local: " + rows.get(i).getColumnData());
						if (rows.get(i).isDeleted()) {
							sb.append(" DELETED!");
						}
						image = BitmapFactory.decodeByteArray(buf, 0, len);
					} else {
						sb.append("Remote: " + rows.get(i).getColumnData());
						conflict = BitmapFactory.decodeByteArray(buf, 0, len);
					}
				}
				sb.append("\n");
			}
			adapter.endCR(TBL);
			return new StringBitmap(sb.toString(), image, conflict);
		}

		protected void onPostExecute(StringBitmap s) {
			Log.v(TAG, "BatchConflictTask finished...");
			console.setText(s.getColumn());
		}
	}

	private class BatchCRLTask extends AsyncTask<Void, Void, String> {
		protected String doInBackground(Void... arg0) {
			adapter.beginCR(TBL);

			List<DataObjectRow> rows = adapter.getConflictedRows(TBL);
			Log.v(TAG, "Got " + rows.size() + " rows...");

			StringBuilder sb = new StringBuilder();
			sb.append("Chosen local copy.\n");

			for (int i = 0; i < rows.size(); i += 2) {
				DataObjectRow dr1 = rows.get(i);
				adapter.resolveConflict(TBL, dr1, CRChoice.MINE);
				Log.v(TAG, "Resolved row " + (i / 2 + 1) + " with local");
			}

			adapter.endCR(TBL);

			return sb.toString();
		}

		protected void onPostExecute(String s) {
			Log.v(TAG, "BatchCRLTask finished...");
			console.setText(s);

			adapter.beginCR(TBL);
			conflictDataView.setText("Conflicted rows: "
					+ adapter.getConflictedRows(TBL).size());
			adapter.endCR(TBL);
		}
	}

	private class BatchCRRTask extends AsyncTask<Void, Void, String> {
		protected String doInBackground(Void... arg0) {
			adapter.beginCR(TBL);

			List<DataObjectRow> rows = adapter.getConflictedRows(TBL);
			Log.v(TAG, "Got " + rows.size() + " rows...");

			StringBuilder sb = new StringBuilder();
			sb.append("Chosen remote copy.\n");

			for (int i = 0; i < rows.size(); i += 2) {
				DataObjectRow dr1 = rows.get(i);
				adapter.resolveConflict(TBL, dr1, CRChoice.SERVER);
				Log.v(TAG, "Resolved row " + (i / 2 + 1) + " with remote");
			}

			adapter.endCR(TBL);
			return sb.toString();
		}

		protected void onPostExecute(String s) {
			Log.v(TAG, "BatchCRRTask finished...");
			console.setText(s);

			adapter.beginCR(TBL);
			conflictDataView.setText("Conflicted rows: "
					+ adapter.getConflictedRows(TBL).size());
			adapter.endCR(TBL);
		}
	}
}
