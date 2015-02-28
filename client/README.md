Simba Client
===========

Prerequisites
-------------
Before running the Simba Client, you must have Android NDK setup for leveldb.  

Configuration
-------------
You must set certain configuration options in the following files:  
```
./SimbaContentService/src/com/necla/simba/client/Preferences.java
./apps/SimbaNoteApp/src/com/necla/simba/apps/notes/Main.java
```

In `Preferences.java`, set:  
  * `DEFAULT_HOST`: Default IP address to Simba Cloud.  
  * `DEFAULT_PORT`: Default port number to Simba Cloud.  

In `Main.java`, set:  
  * `TBL`: Name of Simba Table.  

SimbaContentService Setup
---------------
Open a new Android project with SimbaContentService.  

SimbaClientLib Setup
-----------
Open a new Android project with SimbaClientLib.

SimbaNoteApp Setup
--------------------
Open a new Android project with SimbaNoteApp.

Create a dependency to SimbaClientLib.

Starting SimbaContentService
----------------------
Toggle `Service Inactive` button to `Service Active` and confirm connection to Simba Cloud using ADB Logcat. 

Note that SimbaContentService must stay running in background.

```
V/SimbaActivity( 2978): Starting SCS service
V/SimbaContentService( 2978): SCS onCreate
...
D/SimbaLevelDB( 2978): Setting MAX_OBJ_ID to 1
D/SimbaContentService( 2978): SCS started
V/NetworkIOHandler( 2978): Connecting to host x.x.x.x:x
...
D/SimbaMessageHandler( 2978): Received: seq=486972587, type=CONTROL_RESPONSE
```

Starting SimbaNoteApp
-----------------------------------
Press `Plug Adapter` button and confirm that table is created and accepted by Simba Cloud using ADB Logcat.

```
V/SCSClientAdapter( 6330): Service Connected: android.os.BinderProxy
...
I/System.out( 2978): schema=CREATE TABLE x (...)
...
D/SimbaContentService( 2978): Inserting <x, y> to table bitmap
...
D/SimbaMessageHandler( 2978): Received: seq=486972589, type=SUB_RESPONSE
D/SimbaMessageHandler( 2978): Table <x, y> already created
```

Simba API Samples
-------------------
Overall Simba API can be found in:
```
[./SimbaClientLib/src/com/necla/simba/client/SimbaContentServiceAPI.aidl](./SimbaClientLib/src/com/necla/simba/client/SimbaContentServiceAPI.aidl)
```

Creating a link to SimbaClientAPI from Simba apps.
```
SCSClientAPI adapter = new **SCSClientAdapter**();
```

Setting up Simba Table with tabular + object schema and properties.
```
adapter.**createTable**(TBL, schema, properties);
adapter.**updateTable**(TBL, properties);
```

Writing tabular + object data.
```
ContentValues cv = new ContentValues();
cv.put("COLUMN_NAME", STRING_DATA);
cv.put("OBJECT_NAME", "");
String[] object_order = new String[] {"OBJECT_NAME"};

List<SCSOutputStream> sos_list = adapter.**writeData**(TBL, cv, object_order);
for (SCSOutputStream sos : sos_list) {
	sos.**writeStream**(adapter, buffer);
}
```

Reading tabular + object data.
```
SCSCursor cursor = adapter.**readData**(TBL, null, null, null, null);
cursor.moveToFirst();
String row_id = cursor.getString(0);
String column_data = cursor.getString(1);
List<SCSInputStream> sis_list = cursor.**getInputStream**();
for (SCSInputStream sis : sis_list) {
	sis.**read**(buffer);
}
```

Deleting data.
```
adapter.**deleteData**(TBL, "COLUMN_NAME = ?", new String[] { STRING_DATA });
```

Resolving conflicted data.
```
adapter.**beginCR**(TBL);
List<DataObjectRow> rows = adapter.**getConflictedRows**(TBL);
List<SCSInputStream> sis_list = rows.get(index).**getSCSInputStream**();
for (SCSInputstream sis : sis_list) {
	int row_id = rows.get(index); 
	if (index % 2 == 0) { // local data
		rows.get(index).**getColumnData**();
		sis.**read**(local_buffer);		
	} else { // conflict data
		rows.get(index).**getColumnData**();
		sis.**read**(conflict_buffer);
	}
	adapter.**resolveConflict**(TBL, row_id, CRChoice.MINE); // MINE, SERVER, IGNORE
}
adapter.**endCR**(TBL);
```














