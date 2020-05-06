package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import android.os.AsyncTask;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

public class SimpleDynamoProvider extends ContentProvider {
	static final String TAG = SimpleDynamoProvider.class.getSimpleName();
	final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledynamo.provider");
	static final int SERVER_PORT = 10000;
	SQLiteDatabase sqLiteDatabase;
	SimpleDynamoDBHelper dbHelper;
	SimpleDynamoDBManager dbManager;
	String currentPort;
	String myPort;
	Map<String, String> map;
	Map<String, Integer> portIndexMap;
	Map<Integer, String> indexPortMap;
	Map<Integer, int[]> indexSuccMap;
	Map<Integer, int[]> indexPredMap;
	List<String> nodeList;
	MatrixCursor dumpCursor;
	String queryResult;
	private final ReentrantLock lockObject = new ReentrantLock();



	@Override
	public synchronized int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		if(selection.equals("@")) {
			Log.v(TAG, "@ Delete ");

			String tableName = "SimpleDynamoDB_"+portIndexMap.get(currentPort);
			Cursor dumpCursorTemp = sqLiteDatabase.rawQuery("DROP table "+tableName, null);

			for(int i: indexPredMap.get(portIndexMap.get(currentPort))){
				tableName = "SimpleDynamoDB_"+i;
				dumpCursorTemp = sqLiteDatabase.rawQuery("DROP table "+tableName, null);
			}
			Log.v(TAG, "@ Delete completed");

			return 0;
		}

		if(selection.equals("*")) {
			Log.v(TAG, "----- Entering star delete ----- ");

			int i = 0;
			HashSet<Integer> hashset = new HashSet<Integer>();

			String tableName = "SimpleDynamoDB_"+portIndexMap.get(currentPort);
			Cursor dumpCursorTemp = sqLiteDatabase.rawQuery("DROP table "+tableName, null);
			hashset.add(portIndexMap.get(currentPort));

			for(int j: indexPredMap.get(portIndexMap.get(currentPort))){
				tableName = "SimpleDynamoDB_"+j;
				dumpCursorTemp = sqLiteDatabase.rawQuery("DROP table "+tableName, null);
				hashset.add(j);
			}

			Log.v(TAG, "* @ delete dump 3: hashset: " + hashset);

			try{
				for(i=0; i<=4; ++i){
					if(!hashset.contains(i)) {
						Log.v(TAG, "In Star Delete 1: i: " + i);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i)) * 2);
						Log.v(TAG, "In Star Query 2: selection: " + selection + " calling port " + Integer.parseInt(indexPortMap.get(i)) * 2);

						String msgToSend = "StarDelete" + "::" + i;
						Log.v(TAG, "In Star Delete 3: msgToSend: " + msgToSend);
						DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
						dataOutputStream.writeUTF(msgToSend);

						DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
						String msgRcvd = dataInputStream.readUTF();
						Log.v(TAG, "In Star Delete 4: msgRcvd: " + msgRcvd);

						hashset.add(i);
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					}
				}

				return 0;
			} catch (Exception e) {
				Log.v(TAG, "Generic Exception in Star Delete: " + e.getMessage());
				try {
					Log.v(TAG, "In exception in Star Delete 1: i: " + i);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(indexSuccMap.get(i)[0])) * 2);
					Log.v(TAG, "In exception in Star Delete 2: selection: " + selection + " calling port " + Integer.parseInt(indexPortMap.get(indexSuccMap.get(i)[0])) * 2);

					String msgToSend = "StarDelete" + "::" + indexSuccMap.get(i)[0];
					Log.v(TAG, "In exception in Star StarDelete 3: msgToSend: " + msgToSend);
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					dataOutputStream.writeUTF(msgToSend);

					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
					String msgRcvd = dataInputStream.readUTF();
					Log.v(TAG, "In exception in Star StarDelete 4: msgRcvd: " + msgRcvd);

					hashset.add(i);
					++i;

					dataOutputStream.close();
					dataInputStream.close();
					socket.close();
				} catch (Exception ex) {
					Log.v(TAG, "Generic Exception in exception before for in Star Delete: " + ex.getMessage());
				}

				try {
					for (; i <= 4; ++i) {
						if (!hashset.contains(i)) {
							Log.v(TAG, "In exception in Star Delete 9: i: " + i);
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i)) * 2);
							Log.v(TAG, "In exception in Star Delete 10: selection: " + selection + " calling port " + Integer.parseInt(indexPortMap.get(i)) * 2);

							String msgToSend = "StarDelete" + "::" + i;
							Log.v(TAG, "In exception in Star Delete 11: msgToSend: " + msgToSend);
							DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
							dataOutputStream.writeUTF(msgToSend);

							DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
							String msgRcvd = dataInputStream.readUTF();
							Log.v(TAG, "In exception in Star Delete 12: msgRcvd: " + msgRcvd);

							hashset.add(i);

							dataOutputStream.close();
							dataInputStream.close();
							socket.close();
						}
					}

					return 0;
				} catch (Exception ex){
					Log.v(TAG, "Generic Exception in exception after for in Query else: " + ex.getMessage());
				}
			}
		}

		try {
			Log.v(TAG, "Delete function called: " + selection);
			if (belongsToCurrentPartition(genHash(selection))) {
				Log.v(TAG, "In delete if 1: " + selection);
//				SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
				String tableName = "SimpleDynamoDB_"+portIndexMap.get(currentPort);
//				sqLiteQueryBuilder.setTables(tableName);
				String[] sel = new String[]{selection};
				Log.v(TAG, "In delete if 2: tableName: " + tableName);
				sqLiteDatabase.delete(tableName,"key = ?", sel);
				Log.v(TAG, "In delete if 3: " + selection);

				for (int i : indexSuccMap.get(portIndexMap.get(currentPort))) {
					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i))*2);
						Log.v(TAG, "In delete if for 1: selection: " + selection + " calling port "+ Integer.parseInt(indexPortMap.get(i))*2);

						String msgToSend = "DeleteKey" + "::" + portIndexMap.get(currentPort) + "::" + selection;
						DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
						Log.v(TAG, "In delete if for 2: msgToSend: " + msgToSend);
						dataOutputStream.writeUTF(msgToSend);

						DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
						String msgRcvd = dataInputStream.readUTF();
						Log.v(TAG, "In delete if for 3: msgRcvd: " + msgRcvd);
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					} catch (Exception e) {
						System.out.println("In delete, Exception 1 while deleting from successor ports: " + selection +"\n" + e.getMessage());
					}
				}
			}
			else {
				Log.v(TAG, "In delete: else, to be deleted in: "+nodeList.get(getCorrectPartition(genHash(selection)))+ " selection: "+selection);

				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(selection)))))) * 2);
					Log.v(TAG, "In delete: else, 1: selection: " + selection + " selection hash " + genHash(selection) + " calling port " + Integer.parseInt(indexPortMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(selection)))))) * 2);

					String msgToSend = "DeleteKey" + "::" + nodeList.get(getCorrectPartition(genHash(selection))) + "::" + selection;
					Log.v(TAG, "In delete: else, 2: msgToSend: " + msgToSend);
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					dataOutputStream.writeUTF(msgToSend);

					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
					String msgRcvd = dataInputStream.readUTF();
					Log.v(TAG, "In delete: else, 3: msgRcvd: " + msgRcvd);
					dataOutputStream.close();
					dataInputStream.close();
					socket.close();
				} catch (Exception e) {
					System.out.println("In delete: else, Exception 1 while routing selection: " + selection + " selection hash " + genHash(selection) + e.getMessage());
				}
				try{

					for (int i : indexSuccMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(selection)))))) {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i)) * 2);
						Log.v(TAG, "In delete: else, 4: selection: " + selection + " selection hash " + genHash(selection) + " calling port " + Integer.parseInt(indexPortMap.get(i)) * 2);

						String msgToSend = "DeleteKey" + "::" + nodeList.get(getCorrectPartition(genHash(selection))) + "::" + selection;
						Log.v(TAG, "In delete: else, 5: msgToSend: " + msgToSend);
						DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
						dataOutputStream.writeUTF(msgToSend);
						DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

						String msgRcvd = dataInputStream.readUTF();
						Log.v(TAG, "In delete: else, 6: msgRcvd: " + msgRcvd);
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					}
				} catch (Exception e) {
					System.out.println("In delete: else, Exception 2 while routing selection: " + selection + " selection hash " + genHash(selection) + e.getMessage());
				}
			}
		} catch (NoSuchAlgorithmException nsae){
			Log.v(TAG, "Delete key nsae "+nsae.getMessage());
		}

		return 0;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public synchronized Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub
//		synchronized(this){
			String key = (String) values.get("key");
			String value = (String) values.get("value");

//		lockObject.lock();
//		if(lockObject.isHeldByCurrentThread())
//			lockObject.unlock();

			try {
				Log.v(TAG, "In insert, key: "+key+" key hash: "+genHash(key)+" value: "+value);
				if(belongsToCurrentPartition(genHash(key))) {
					Log.v(TAG, "In insert, checking if key exists already 1: key: " + key + " key hash " + genHash(key));
					SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
					String TBL_NAME = "SimpleDynamoDB_" + portIndexMap.get(currentPort);
					sqLiteQueryBuilder.setTables(TBL_NAME);
					String sel = "key" + " like '" + key + "'";
					Cursor tempCursor = sqLiteQueryBuilder.query(sqLiteDatabase, null, sel, null, null, null, null);
					Log.v(TAG, "In insert, checking if key exists already 2: key: " + key);
					if(tempCursor.getCount() > 0){
						String[] selec = new String[]{key};
						Log.v(TAG, "In insert, deleting key: "+key+" in TBL_NAME: " + TBL_NAME);
						sqLiteDatabase.delete(TBL_NAME,"key = ?", selec);
					}
					Log.v(TAG, "inserting!!!!!!!!! key: " + key + " key hash " + genHash(key));
					sqLiteDatabase.insert("SimpleDynamoDB_" + portIndexMap.get(currentPort), null, values);

					for (int i : indexSuccMap.get(portIndexMap.get(currentPort))) {
						try {
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i))*2);
							Log.v(TAG, "1 Creating replica key: " + key + " key hash " + genHash(key) + " calling port "+ Integer.parseInt(indexPortMap.get(i))*2);

							String msgToSend = "InsertAndReturn" + "::" + portIndexMap.get(currentPort) + "::" + key + "::" + value + "::" + myPort;
							DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
							Log.v(TAG, "2 Creating replica msgToSend: " + msgToSend);
							dataOutputStream.writeUTF(msgToSend);

							DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
							String msgRcvd = dataInputStream.readUTF();
							Log.v(TAG, "3 Creating replica msgRcvd: " + msgRcvd);
							dataOutputStream.close();
							dataInputStream.close();
							socket.close();
						} catch (Exception e) {
							System.out.println("In insert, Exception while creating replica key: \" + key + \" key hash \" + genHash(key) " + e.getMessage());
						}
					}
				}
				else {

					Log.v(TAG, "In insert: else, to be inserted in: "+nodeList.get(getCorrectPartition(genHash(key)))+ " key: "+key+" key hash: "+genHash(key));

					try {
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(key)))))) * 2);
						Log.v(TAG, "else 1 Creating replica key: " + key + " key hash " + genHash(key) + " calling port " + Integer.parseInt(indexPortMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(key)))))) * 2);

						String msgToSend = "InsertAndReturn" + "::" + nodeList.get(getCorrectPartition(genHash(key))) + "::" + key + "::" + value+ "::" + myPort;
						Log.v(TAG, "else 2 Creating replica, msgToSend: " + msgToSend);
						DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
						dataOutputStream.writeUTF(msgToSend);

						DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
						String msgRcvd = dataInputStream.readUTF();
						Log.v(TAG, "else 3 Creating replica, msgRcvd: " + msgRcvd);
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					} catch (Exception e) {
						System.out.println("In insert, Exception 1 while creating replica key: " + key + " key hash " + genHash(key) + e.getMessage());
					}


					for (int i : indexSuccMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(key)))))) {
						try{
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i)) * 2);
							Log.v(TAG, "else 4 Creating replica key: " + key + " key hash " + genHash(key) + " calling port " + Integer.parseInt(indexPortMap.get(i)) * 2);

							String msgToSend = "InsertAndReturn" + "::" + nodeList.get(getCorrectPartition(genHash(key))) + "::" + key + "::" + value+ "::" + myPort;
							Log.v(TAG, "else 5 Creating replica msgToSend: " + msgToSend);
							DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
							dataOutputStream.writeUTF(msgToSend);
							DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());

							String msgRcvd = dataInputStream.readUTF();
							Log.v(TAG, "else 6 Creating replica msgRcvd: " + msgRcvd);
							dataOutputStream.close();
							dataInputStream.close();
							socket.close();
						} catch (Exception e) {
							System.out.println("In insert, else, Exception 2 while creating replica key: " + key + " key hash " + genHash(key) + e.getMessage());
						}
					}
				}
			} catch (NoSuchAlgorithmException nsae){
				Log.v(TAG, "insert key nsae "+nsae.getMessage());
			}
			return uri;
//		}
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		lockObject.lock();
		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		currentPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);  //5554
		myPort = String.valueOf((Integer.parseInt(currentPort) * 2));  //11108
		dbHelper = new SimpleDynamoDBHelper(this.getContext());
		sqLiteDatabase = dbHelper.getWritableDatabase();
		dumpCursor = new MatrixCursor(new String[]{"key", "value"});

		map = new TreeMap<String, String>();
		map.put("33d6357cfaaf0f72991b0ecd8c56da066613c089", "5554");
		map.put("208f7f72b198dadd244e61801abe1ec3a4857bc9", "5556");
		map.put("abf0fd8db03e5ecb199a9b82929e9db79b909643", "5558");
		map.put("c25ddd596aa7c81fa12378fa725f706d54325d12", "5560");
		map.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1", "5562");

		portIndexMap = new HashMap<String, Integer>();
		portIndexMap.put("5562", 4);
		portIndexMap.put("5556", 1);
		portIndexMap.put("5554", 0);
		portIndexMap.put("5558", 2);
		portIndexMap.put("5560", 3);

		indexPortMap = new HashMap<Integer, String>();
		indexPortMap.put(4, "5562");
		indexPortMap.put(1, "5556");
		indexPortMap.put(0, "5554");
		indexPortMap.put(2, "5558");
		indexPortMap.put(3, "5560");

		indexPredMap = new HashMap<Integer, int[]>(); // p1  p2
		int[] val = new int[] {2, 0};
		indexPredMap.put(3, val);
		val = new int[] {4, 3};
		indexPredMap.put(1, val);
		val = new int[] {1, 4};
		indexPredMap.put(0, val);
		val = new int[] {0, 1};
		indexPredMap.put(2, val);
		val = new int[] {3, 2};
		indexPredMap.put(4, val);

		indexSuccMap = new HashMap<Integer, int[]>(); // s1 s2
		val = new int[] {4, 1};
		indexSuccMap.put(3, val);
		val = new int[] {0, 2};
		indexSuccMap.put(1, val);
		val = new int[] {2, 3};
		indexSuccMap.put(0, val);
		val = new int[] {3, 4};
		indexSuccMap.put(2, val);
		val = new int[] {1, 0};
		indexSuccMap.put(4, val);

		nodeList = new ArrayList<String>(Arrays.asList("4", "1", "0", "2", "3"));

		String tableName = "SimpleDynamoDB_"+portIndexMap.get(currentPort);
		dbHelper.createDB(sqLiteDatabase, tableName);

		for(int i: indexPredMap.get(portIndexMap.get(currentPort))){
			tableName = "SimpleDynamoDB_"+i;
			dbHelper.createDB(sqLiteDatabase, tableName);
		}

		Log.v(TAG, "In onCreate -- currentPort: "+currentPort+", myPort: "+myPort);

		try{
			ServerSocket serverSocket = new ServerSocket();
			Log.v(TAG, "In onCreate -- serverSocket: 1");
			serverSocket.setReuseAddress(true);
			Log.v(TAG, "In onCreate -- serverSocket: 2");
			serverSocket.bind(new InetSocketAddress(SERVER_PORT));
			Log.v(TAG, "In onCreate -- serverSocket: 3");
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
		}
		catch (IOException ioe){
			Log.v(TAG, "In onCreate -- Could not create server socket");
			Log.v(TAG, "In onCreate "+ ioe.getMessage());
			return false;
		}


		new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "", myPort);

		if(lockObject.isHeldByCurrentThread()){
			lockObject.unlock();
		}
		return false;
	}

	@Override
	public synchronized Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
//		synchronized (this){
			dumpCursor = new MatrixCursor(new String[]{"key", "value"});
			Log.v(TAG, "selection is : "+ selection);

//			lockObject.lock();
//			if(lockObject.isHeldByCurrentThread())
//				lockObject.unlock();

			if (selection.equals("@")) {
				Log.v(TAG, "@ Query ");

				String tableName = "SimpleDynamoDB_"+portIndexMap.get(currentPort);

				Cursor dumpCursorTemp = sqLiteDatabase.rawQuery("SELECT * FROM "+tableName, null);
				if (dumpCursorTemp.moveToFirst()) {
					while (true) {
						dumpCursor.addRow(new Object[]{dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("key")), dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("value"))});
						Log.v(TAG, "@ Query dump 1: " + dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("key")) + " " + dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("value")));

						if (!dumpCursorTemp.moveToNext())
							break;
					}
				}
				for(int i: indexPredMap.get(portIndexMap.get(currentPort))){
					tableName = "SimpleDynamoDB_"+i;
					dumpCursorTemp = sqLiteDatabase.rawQuery("SELECT * FROM "+tableName, null);
					if (dumpCursorTemp.moveToFirst()) {
						while (true) {
							dumpCursor.addRow(new Object[]{dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("key")), dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("value"))});
							Log.v(TAG, "@ Query dump 2: " + dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("key")) + " " + dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("value")));

							if (!dumpCursorTemp.moveToNext())
								break;
						}
					}
				}

				Log.v(TAG, "Printing the result going to be returned for @ query: ");
				StringBuilder sb = new StringBuilder();

				if(dumpCursor.moveToFirst()) {
					while(true) {
						sb.append(dumpCursor.getString(dumpCursor.getColumnIndex("key")));
						sb.append(": ");
						sb.append(dumpCursor.getString(dumpCursorTemp.getColumnIndex("value")));
						sb.append(System.lineSeparator());

						if(!dumpCursor.moveToNext())
							break;
					}
				}
				Log.v(TAG, " +++++ ");
				Log.v(TAG, sb.toString());
				Log.v(TAG, " +++++ ");
				Log.v(TAG, "End of printing the result going to be returned for @ query: ");

				return dumpCursor;
			}

			if(selection.equals("*")) {
				Log.v(TAG, "----- Entering star query ----- ");

				int i = 0;
				HashSet<Integer> hashset = new HashSet<Integer>();

				String tableName = "SimpleDynamoDB_"+portIndexMap.get(currentPort);

				Cursor dumpCursorTemp = sqLiteDatabase.rawQuery("SELECT * FROM "+tableName, null);
				if (dumpCursorTemp.moveToFirst()) {
					while (true) {
						dumpCursor.addRow(new Object[]{dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("key")), dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("value"))});
						Log.v(TAG, "* @ Query dump 1: " + dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("key")) + " " + dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("value")));

						if (!dumpCursorTemp.moveToNext()) {
							break;
						}
					}
				}
				hashset.add(portIndexMap.get(currentPort));

				for(int j: indexPredMap.get(portIndexMap.get(currentPort))){
					tableName = "SimpleDynamoDB_"+j;
					dumpCursorTemp = sqLiteDatabase.rawQuery("SELECT * FROM "+tableName, null);
					if (dumpCursorTemp.moveToFirst()) {
						while (true) {
							dumpCursor.addRow(new Object[]{dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("key")), dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("value"))});
							Log.v(TAG, "* @ Query dump 2: " + dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("key")) + " " + dumpCursorTemp.getString(dumpCursorTemp.getColumnIndex("value")));

							if (!dumpCursorTemp.moveToNext()) {
								break;
							}
						}
					}
					hashset.add(j);
				}

				Log.v(TAG, "* @ Query dump 3: hashset: " + hashset);

				try{
					for(i=0; i<=4; ++i){
						if(!hashset.contains(i)) {
							Log.v(TAG, "In Star Query 1: i: " + i);
							Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i)) * 2);
							Log.v(TAG, "In Star Query 2: selection: " + selection + " calling port " + Integer.parseInt(indexPortMap.get(i)) * 2);

							String msgToSend = "StarQuery" + "::" + i;
							Log.v(TAG, "In Star Query 3: msgToSend: " + msgToSend);
							DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
							dataOutputStream.writeUTF(msgToSend);

							DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
							String msgRcvd = dataInputStream.readUTF();
							Log.v(TAG, "In Star Query 4: msgRcvd: " + msgRcvd);
							String[] starQueryResult;
							if (!msgRcvd.equals("")) {
								Log.v(TAG, "In Star Query 5: in if");
								starQueryResult = msgRcvd.split("::");
								Log.v(TAG, "In Star Query 6: Received array:" + Arrays.toString(starQueryResult));
								for (int j = 0; j < starQueryResult.length; j += 2) {
									if (!starQueryResult[j].equals("StarQueryResult"))
										dumpCursor.addRow(new Object[]{starQueryResult[j], starQueryResult[j + 1]});
									else
										j--;
									Log.v(TAG, "In Star Query 7: key: " + starQueryResult[j] + " value: " + starQueryResult[j + 1]);
								}
								Log.v(TAG, "In Star Query 8: end of for loop");
							}

							hashset.add(i);
							dataOutputStream.close();
							dataInputStream.close();
							socket.close();
						}
					}

					Log.v(TAG, "Printing the result going to be returned for star query: ");
					StringBuilder sb = new StringBuilder();

					if(dumpCursor.moveToFirst()) {
						while(true) {
							sb.append(dumpCursor.getString(dumpCursor.getColumnIndex("key")));
							sb.append(": ");
							sb.append(dumpCursor.getString(dumpCursorTemp.getColumnIndex("value")));
							sb.append(System.lineSeparator());

							if(!dumpCursor.moveToNext())
								break;
						}
					}
					Log.v(TAG, " +++++ ");
					Log.v(TAG, sb.toString());
					Log.v(TAG, " +++++ ");
					Log.v(TAG, "End of printing the result going to be returned for star query: ");

					return dumpCursor;
				} catch (Exception e) {
					Log.v(TAG, "Generic Exception in Query else: " + e.getMessage());
					try {
						Log.v(TAG, "In exception in Star Query 1: i: " + i);
						Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(indexSuccMap.get(i)[0])) * 2);
						Log.v(TAG, "In exception in Star Query 2: selection: " + selection + " calling port " + Integer.parseInt(indexPortMap.get(indexSuccMap.get(i)[0])) * 2);

//					String msgToSend = "StarQuery" + "::" + indexSuccMap.get(i)[0];
						String msgToSend = "StarQuery" + "::" + i;
						Log.v(TAG, "In exception in Star Query 3: msgToSend: " + msgToSend);
						DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
						dataOutputStream.writeUTF(msgToSend);

						DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
						String msgRcvd = dataInputStream.readUTF();
						Log.v(TAG, "In exception in Star Query 4: msgRcvd: " + msgRcvd);
						String[] starQueryResult;
						if (!msgRcvd.equals("")) {
							Log.v(TAG, "In exception in Star Query 5: in if");
							starQueryResult = msgRcvd.split("::");
							Log.v(TAG, "In exception in Star Query 6: Received array:" + Arrays.toString(starQueryResult));
							for (int j = 0; j < starQueryResult.length; j += 2) {
								if (!starQueryResult[j].equals("StarQueryResult"))
									dumpCursor.addRow(new Object[]{starQueryResult[j], starQueryResult[j + 1]});
								else
									j--;
								Log.v(TAG, "In exception in Star Query 7: " + starQueryResult[j] + " " + starQueryResult[j + 1]);
							}
							Log.v(TAG, "In exception in Star Query 8: end of for loop");
						}

						hashset.add(i);
						++i;
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					} catch (Exception ex) {
						Log.v(TAG, "Generic Exception in exception before for in Query else: " + ex.getMessage());
					}

					try {
						for (; i <= 4; ++i) {
							if (!hashset.contains(i)) {
								Log.v(TAG, "In exception in Star Query 9: i: " + i);
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i)) * 2);
								Log.v(TAG, "In exception in Star Query 10: selection: " + selection + " calling port " + Integer.parseInt(indexPortMap.get(i)) * 2);

								String msgToSend = "StarQuery" + "::" + i;
								Log.v(TAG, "In exception in Star Query 11: msgToSend: " + msgToSend);
								DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
								dataOutputStream.writeUTF(msgToSend);

								DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
								String msgRcvd = dataInputStream.readUTF();
								Log.v(TAG, "In exception in Star Query 12: msgRcvd: " + msgRcvd);
								String[] starQueryResult;
								if (!msgRcvd.equals("")) {
									Log.v(TAG, "In exception in Star Query 13: in if");
									starQueryResult = msgRcvd.split("::");
									Log.v(TAG, "In exception in Star Query 14: Received array:" + Arrays.toString(starQueryResult));
									for (int j = 0; j < starQueryResult.length; j += 2) {
										if (!starQueryResult[j].equals("StarQueryResult"))
											dumpCursor.addRow(new Object[]{starQueryResult[j], starQueryResult[j + 1]});
										else
											j--;
										Log.v(TAG, "In exception in Star Query 15: " + starQueryResult[j] + " " + starQueryResult[j + 1]);
									}
									Log.v(TAG, "In exception in Star Query 16: end of for loop");
								}

								hashset.add(i);
								dataOutputStream.close();
								dataInputStream.close();
								socket.close();
							}
						}

						return dumpCursor;
					} catch (Exception ex){
						Log.v(TAG, "Generic Exception in exception after for in Query else: " + ex.getMessage());
					}
				}
			}

			try {
				if (belongsToCurrentPartition(genHash(selection))) {
					Log.v(TAG, "In if key Query 1: " + selection);
					SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
					String TABLE_NAME = "SimpleDynamoDB_" + portIndexMap.get(currentPort);
					sqLiteQueryBuilder.setTables(TABLE_NAME);
					String sel = "key" + " like '" + selection + "'";
					Cursor tempCursor = sqLiteQueryBuilder.query(sqLiteDatabase, projection, sel, selectionArgs, null, null, sortOrder);
					Log.v(TAG, "In if key Query 2: " + selection);

					if (tempCursor.moveToFirst()) {
						while (true) {
							if (tempCursor.getString((tempCursor.getColumnIndex(("key")))).equals(selection)) {
								queryResult = tempCursor.getString(tempCursor.getColumnIndex("value"));
								break;
							}
							if (!tempCursor.moveToNext())
								break;
						}
					}
					Log.v(TAG, "In if key Query 3: queryResult: " + queryResult);

					MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
					returnCursor.addRow(new Object[]{selection, queryResult});
					Log.v(TAG, "In if key Query 4: queryResult: " + queryResult);
					return returnCursor;
				} else if (Arrays.asList(indexPredMap.get(portIndexMap.get(currentPort))).contains(nodeList.get(getCorrectPartition(genHash(selection))))) {
					Log.v(TAG, "In else if key Query 1: " + selection);
					SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
					String TABLE_NAME = "SimpleDynamoDB_" + nodeList.get(getCorrectPartition(genHash(selection)));
					sqLiteQueryBuilder.setTables(TABLE_NAME);
					String sel = "key" + " like '" + selection + "'";
					Cursor tempCursor = sqLiteQueryBuilder.query(sqLiteDatabase, projection, sel, selectionArgs, null, null, sortOrder);
					Log.v(TAG, "In else if key Query 2: " + selection);

					if (tempCursor.moveToFirst()) {
						while (true) {
							if (tempCursor.getString((tempCursor.getColumnIndex(("key")))).equals(selection)) {
								queryResult = tempCursor.getString(tempCursor.getColumnIndex("value"));
								break;
							}
							if (!tempCursor.moveToNext())
								break;
						}
					}
					Log.v(TAG, "In else if key Query 3: queryResult: " + queryResult);

					MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
					returnCursor.addRow(new Object[]{selection, queryResult});
					Log.v(TAG, "In else if key Query 4: queryResult: " + queryResult);
					return returnCursor;
				} else{
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(selection)))))) * 2);
					Log.v(TAG, "In else key Query 1:  " + selection + " calling port " + Integer.parseInt(indexPortMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(selection)))))) * 2);

					String msgToSend = "QueryKey" + "::" + nodeList.get(getCorrectPartition(genHash(selection)))+ "::" + selection;
					Log.v(TAG, "In else key Query 2: msgToSend: " + msgToSend);
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					dataOutputStream.writeUTF(msgToSend);

					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
					String msgRcvd = dataInputStream.readUTF();
					Log.v(TAG, "In else key Query 3: msgRcvd: " + msgRcvd);
					dataOutputStream.close();
					dataInputStream.close();
					socket.close();

					MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
					returnCursor.addRow(new Object[]{selection, msgRcvd});
					Log.v(TAG, "In else key Query 4: msgRcvd: " + msgRcvd);
					return returnCursor;
				}
			} catch (NoSuchAlgorithmException nsae) {
				Log.v(TAG, "Exception in Query else: " + nsae.getMessage());
			} catch (Exception e){
				Log.v(TAG, "Generic Exception in Query else: " + e.getMessage());

				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(indexSuccMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(selection)))))[0])) * 2);
					Log.v(TAG, "In exception in else key Query 1:  " + selection + " calling port " + Integer.parseInt(indexPortMap.get(indexSuccMap.get(Integer.parseInt(nodeList.get(getCorrectPartition(genHash(selection)))))[0])) * 2);

					String msgToSend = "QueryKey" + "::" + nodeList.get(getCorrectPartition(genHash(selection))) + "::" + selection;
					Log.v(TAG, "In exception in else key Query 2:  msgToSend: " + msgToSend);
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					dataOutputStream.writeUTF(msgToSend);

					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
					String msgRcvd = dataInputStream.readUTF();
					Log.v(TAG, "In exception in else key Query 3: msgRcvd: " + msgRcvd);
					dataOutputStream.close();
					dataInputStream.close();
					socket.close();

					MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
					returnCursor.addRow(new Object[]{selection, msgRcvd});
					Log.v(TAG, "In exception in else key Query 4: returned query");
					return returnCursor;
				} catch (NoSuchAlgorithmException nsae) {
					Log.v(TAG, "Exception in exception in Query else: " + nsae.getMessage());
				} catch (Exception ex) {
					Log.v(TAG, "Generic Exception in Query else: " + ex.getMessage());
				}
			}
			return null;
//		}
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	private boolean belongsToCurrentPartition(String hash){
		Log.v(TAG, "In belongs: myPort: " + myPort);
		try {
			TreeMap<String, String> checkMap = (TreeMap) map;
			checkMap.put(hash, "");
			int hashIndex = 0;

			for(Map.Entry<String, String> entry: checkMap.entrySet()){
				if(entry.getKey().equals(hash)){
					checkMap.remove(hash);
					break;
				}
				hashIndex ++;
			}

			if(hashIndex==checkMap.size())
				hashIndex = 0;

			if(checkMap.keySet().toArray()[hashIndex].equals(genHash(currentPort)))
				return true;
			else
				return false;

		}catch(NoSuchAlgorithmException nsae){
			Log.v(TAG, "belongsToCurrentPartition "+nsae.getMessage());
		}

		Log.v(TAG, "belongsToCurrentPartition return false");
		return false;
	}

	private int getCorrectPartition(String hash){
		Log.v(TAG, "In getCorrectPartition: myPort: " + myPort);

		TreeMap<String, String> checkMap = (TreeMap) map;
		checkMap.put(hash, "");
		int hashIndex = 0;

		for(Map.Entry<String, String> entry: checkMap.entrySet()){
			if(entry.getKey().equals(hash)){
				checkMap.remove(hash);
				break;
			}
			hashIndex ++;
		}

		if(hashIndex==checkMap.size())
			hashIndex = 0;

		return hashIndex;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

		int count = 0;

		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			Log.v(TAG, "In serverTask: 1");

			ServerSocket serverSocket = sockets[0];
			Log.v(TAG, "In serverTask: 2");

			/*
			 * TODO: Fill in your server code that receives messages and passes them
			 * to onProgressUpdate().
			 */
            /*
            Code from PA1
            */


			while (true) {
				Socket socket = null;
				lockObject.lock();
				if(lockObject.isHeldByCurrentThread()){
					lockObject.unlock();
				}
				try {
					Log.v(TAG, "In serverTask: 3");
					socket = serverSocket.accept();
					Log.v(TAG, "In serverTask: 4");
					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
					Log.v(TAG, "In serverTask: 5");
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					Log.v(TAG, "In serverTask: 6");

					String msgRcvd = dataInputStream.readUTF(); // join::11108
					Log.v(TAG, "In serverTask: 7 " + msgRcvd);
					String[] splitMsgRcvd = msgRcvd.split("::");

					if (splitMsgRcvd[0].equals("InsertAndReturn")) {
						Log.v(TAG, "In serverTask in InsertAndReturn 1: " + Arrays.toString(splitMsgRcvd));
						ContentValues contentValues = new ContentValues();
						contentValues.put("key", splitMsgRcvd[2]);
						contentValues.put("value", splitMsgRcvd[3]);
						Log.v(TAG, "In serverTask in InsertAndReturn 2: ");

						Log.v(TAG, "In serverTask, checking if key exists already 1: key: " + splitMsgRcvd[2]);
						SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
						String TBLE_NAME = "SimpleDynamoDB_" + splitMsgRcvd[1];
						sqLiteQueryBuilder.setTables(TBLE_NAME);
						String sele = "key" + " like '" + splitMsgRcvd[2] + "'";
						Cursor tempCursor = sqLiteQueryBuilder.query(sqLiteDatabase, null, sele, null, null, null, null);
						Log.v(TAG, "In serverTask, checking if key exists already 2: key: " + splitMsgRcvd[2]);
						if(tempCursor.getCount() > 0){
							String[] select = new String[]{splitMsgRcvd[2]};
							Log.v(TAG, "In serverTask, deleting key: "+splitMsgRcvd[2]+" in TBLE_NAME: " + TBLE_NAME);
							sqLiteDatabase.delete(TBLE_NAME,"key = ?", select);
						}

						sqLiteDatabase.insert("SimpleDynamoDB_" + splitMsgRcvd[1], null, contentValues);
						Log.v(TAG, "In serverTask in InsertAndReturn 3: ");
						dataOutputStream.writeUTF("Done");
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					} else if (splitMsgRcvd[0].equals("QueryKey")) {
						Log.v(TAG, "In serverTask in QueryKey 1: " + Arrays.toString(splitMsgRcvd));
						SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
						String TABLE_NAME = "SimpleDynamoDB_" + splitMsgRcvd[1];
						sqLiteQueryBuilder.setTables(TABLE_NAME);
						String sel = "key" + " like '" + splitMsgRcvd[2] + "'";
						Cursor tempCursor = sqLiteQueryBuilder.query(sqLiteDatabase, null, sel, null, null, null, null);
						Log.v(TAG, "In serverTask in QueryKey 2: key: " + splitMsgRcvd[2]);

						if (tempCursor.moveToFirst()) {
							while (true) {
								if (tempCursor.getString((tempCursor.getColumnIndex(("key")))).equals(splitMsgRcvd[2])) {
									queryResult = tempCursor.getString(tempCursor.getColumnIndex("value"));
									break;
								}
								if (!tempCursor.moveToNext())
									break;
							}
						}

						dataOutputStream.writeUTF(queryResult);
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					} else if (splitMsgRcvd[0].equals("StarQuery")) {
						Log.v(TAG, "In serverTask in StarQuery 1: " + Arrays.toString(splitMsgRcvd));
//						Cursor starQueryServer = query(uri, null, "@", null, null);
						String tableName = "SimpleDynamoDB_"+splitMsgRcvd[1];
						Cursor starQueryServer = sqLiteDatabase.rawQuery("SELECT * FROM "+tableName, null);
						Log.v(TAG, "In serverTask in StarQuery 2");
						StringBuilder starQueryResultString = new StringBuilder();
						if (starQueryServer.moveToFirst()) {
//                            starQueryResultString.append("StarQueryResult");
							while (true) {
								starQueryResultString.append(starQueryServer.getString(starQueryServer.getColumnIndex("key")));
								starQueryResultString.append("::");
								starQueryResultString.append(starQueryServer.getString(starQueryServer.getColumnIndex("value")));
								starQueryResultString.append("::");
								if (!starQueryServer.moveToNext())
									break;
							}
							Log.v(TAG, "In serverTask in StarQuery 3");
						}

						Log.v(TAG,"In serverTask in StarQuery 4: Returning result : "+ starQueryResultString.toString());
						dataOutputStream.writeUTF(starQueryResultString.toString());   // sending key1::value1::key2::value2::key3::value3

//						dataOutputStream.writeUTF(queryResult);
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					} else if (splitMsgRcvd[0].equals("DeleteKey")) {
						Log.v(TAG, "In serverTask in DeleteKey 1: " + Arrays.toString(splitMsgRcvd));
						SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
						String tableName = "SimpleDynamoDB_" + splitMsgRcvd[1];
						sqLiteQueryBuilder.setTables(tableName);
						String[] sel = new String[]{splitMsgRcvd[2]};
						sqLiteDatabase.delete(tableName,"key = ?", sel);
						Log.v(TAG, "In serverTask in DeleteKey 2");

						dataOutputStream.writeUTF("Done");

//						dataOutputStream.writeUTF(queryResult);
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					} else if (splitMsgRcvd[0].equals("StarDelete")) {

						Log.v(TAG, "In serverTask in StarDelete 1: " + Arrays.toString(splitMsgRcvd));

						String tableName = "SimpleDynamoDB_"+portIndexMap.get(currentPort);
						Cursor dumpCursorTemp = sqLiteDatabase.rawQuery("DROP table "+tableName, null);

						for(int i: indexPredMap.get(portIndexMap.get(currentPort))){
							tableName = "SimpleDynamoDB_"+i;
							dumpCursorTemp = sqLiteDatabase.rawQuery("DROP table "+tableName, null);
						}
						Log.v(TAG, "In serverTask in StarDelete 2");

						dataOutputStream.writeUTF("Done");

						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					}  else if (splitMsgRcvd[0].equals("GetData")) {

						Log.v(TAG, "In serverTask in GetData 1: " + Arrays.toString(splitMsgRcvd)+" received from "+splitMsgRcvd[2]);
						String tableName = "SimpleDynamoDB_"+splitMsgRcvd[1];
						Cursor getDataServer = sqLiteDatabase.rawQuery("SELECT * FROM "+tableName, null);
						Log.v(TAG, "In serverTask in GetData 2");
						StringBuilder getDataResultString = new StringBuilder();
						if (getDataServer.moveToFirst()) {
							while (true) {
								getDataResultString.append(getDataServer.getString(getDataServer.getColumnIndex("key")));
								getDataResultString.append("::");
								getDataResultString.append(getDataServer.getString(getDataServer.getColumnIndex("value")));
								getDataResultString.append("::");
								if (!getDataServer.moveToNext())
									break;
							}
							Log.v(TAG, "In serverTask in GetData 3");
						}

						Log.v(TAG,"In serverTask in GetData 4: Returning result : "+ getDataResultString.toString());
						dataOutputStream.writeUTF(getDataResultString.toString());   // sending key1::value1::key2::value2::key3::value3

//						dataOutputStream.writeUTF(queryResult);
						dataOutputStream.close();
						dataInputStream.close();
						socket.close();
					}
				} catch (Exception e) {
					Log.v(TAG, "ClientTask UnknownHostException 1 " + e.getMessage());
				} finally {
					try {
						socket.close();
					} catch (Exception e) {
						Log.v(TAG, "ClientTask UnknownHostException 2" + e.getMessage());
					}
				}
			}
		}
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

        /*
        References:
        Reusing code from PA1
         */

		@Override
		protected Void doInBackground(String... msgs) {
//			try {
			Log.v(TAG, "In clientTask myPort: "+myPort);

			for (int i : indexSuccMap.get(portIndexMap.get(currentPort))) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i))*2);
					Log.v(TAG, "In clientTask 1" + " calling port "+ Integer.parseInt(indexPortMap.get(i))*2);

					String msgToSend = "GetData" + "::" + portIndexMap.get(currentPort) + "::" + myPort;
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					Log.v(TAG, "In clientTask 2 msgToSend: " + msgToSend);
					dataOutputStream.writeUTF(msgToSend);

					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
					String msgRcvd = dataInputStream.readUTF();
					Log.v(TAG, "In clientTask 3 msgRcvd: " + msgRcvd);

					String[] getDataResult;
					if (!msgRcvd.equals("")) {
						Log.v(TAG, "In clientTask 4: in if");
						getDataResult = msgRcvd.split("::");
						Log.v(TAG, "In clientTask 5: Received array:" + Arrays.toString(getDataResult));
						for (int j = 0; j < getDataResult.length; j += 2) {
							if (!getDataResult[i].equals("GetDataResult")) {
								ContentValues contentValues = new ContentValues();
								contentValues.put("key", getDataResult[j]);
								contentValues.put("value", getDataResult[j+1]);

								Log.v(TAG, "In clientTask, checking if key exists already 1 1: key: " + getDataResult[j]);
								SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
								String TBLE_NAME = "SimpleDynamoDB_" + portIndexMap.get(currentPort);
								sqLiteQueryBuilder.setTables(TBLE_NAME);
								String sele = "key" + " like '" + getDataResult[j] + "'";
								Cursor tempCursor = sqLiteQueryBuilder.query(sqLiteDatabase, null, sele, null, null, null, null);
								Log.v(TAG, "In clientTask, checking if key exists already 1 2: key: " + getDataResult[j]);
								if(tempCursor.getCount() > 0){
									String[] select = new String[]{getDataResult[j]};
									Log.v(TAG, "In clientTask, deleting key: "+getDataResult[j]+" in 1 TBLE_NAME: " + TBLE_NAME);
									sqLiteDatabase.delete(TBLE_NAME,"key = ?", select);
								}

								sqLiteDatabase.insert("SimpleDynamoDB_" + portIndexMap.get(currentPort), null, contentValues);
							}
							else {
								j--;
							}
							Log.v(TAG, "In clientTask 6: key: " + getDataResult[j] + " value: " + getDataResult[j + 1]);
						}
						Log.v(TAG, "In clientTask 7: end of for loop");
					}

					dataOutputStream.close();
					dataInputStream.close();
					socket.close();
					break;
				} catch (Exception e) {
					System.out.println("In clientTask, Exception while calling successor port "+ Integer.parseInt(indexPortMap.get(i))*2 + e.getMessage());
				}
			}

			Log.v(TAG, "In clientTask getting predecessor's data: myPort "+myPort);

//			try {
			for(int i : indexPredMap.get(portIndexMap.get(currentPort))) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(i)) * 2);
					Log.v(TAG, "In clientTask 8, predecessor's data" + " calling port " + Integer.parseInt(indexPortMap.get(i)) * 2);

					String msgToSend = "GetData" + "::" + i + "::" + myPort;
					DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
					Log.v(TAG, "In clientTask 9, predecessor's data: msgToSend: " + msgToSend);
					dataOutputStream.writeUTF(msgToSend);

					DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
					String msgRcvd = dataInputStream.readUTF();
					Log.v(TAG, "In clientTask 10, predecessor's data: msgRcvd: " + msgRcvd);

					String[] getDataResult;
					if (!msgRcvd.equals("")) {
						Log.v(TAG, "In clientTask 11, predecessor's data: in if");
						getDataResult = msgRcvd.split("::");
						Log.v(TAG, "In clientTask 12, predecessor's data: Received array:" + Arrays.toString(getDataResult));
						for (int j = 0; j < getDataResult.length; j += 2) {
							if (!getDataResult[i].equals("GetDataResult")) {
								ContentValues contentValues = new ContentValues();
								contentValues.put("key", getDataResult[j]);
								contentValues.put("value", getDataResult[j + 1]);

								Log.v(TAG, "In clientTask, checking if key exists already 2 1: key: " + getDataResult[j]);
								SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
								String TBLE_NAME = "SimpleDynamoDB_" + i;
								sqLiteQueryBuilder.setTables(TBLE_NAME);
								String sele = "key" + " like '" + getDataResult[j] + "'";
								Cursor tempCursor = sqLiteQueryBuilder.query(sqLiteDatabase, null, sele, null, null, null, null);
								Log.v(TAG, "In clientTask, checking if key exists already 2 2: key: " + getDataResult[j]);
								if(tempCursor.getCount() > 0){
									String[] select = new String[]{getDataResult[j]};
									Log.v(TAG, "In clientTask, deleting key: "+getDataResult[j]+" in 2 TBLE_NAME: " + TBLE_NAME);
									sqLiteDatabase.delete(TBLE_NAME,"key = ?", select);
								}

								sqLiteDatabase.insert("SimpleDynamoDB_" + i, null, contentValues);
							} else
								j--;
							Log.v(TAG, "In clientTask 13, predecessor's data: key: " + getDataResult[j] + " value: " + getDataResult[j + 1]);
						}
						Log.v(TAG, "In clientTask 14, predecessor's data: end of for loop");
					}

					dataOutputStream.close();
					dataInputStream.close();
					socket.close();
				} catch (Exception exc){
					Log.v(TAG, "In clientTask exception 1 while getting predecessor's data: myPort "+myPort+" "+ exc.getMessage());

					for(int j: indexSuccMap.get(i)){
						if(j!=portIndexMap.get(currentPort)) {
							try {
								Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(indexPortMap.get(j)) * 2);
								Log.v(TAG, "In clientTask 14 exception, predecessor's data" + " calling port " + Integer.parseInt(indexPortMap.get(j)) * 2);

								String msgToSend = "GetData" + "::" + i + "::" + myPort;
								DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
								Log.v(TAG, "In clientTask 15 exception, predecessor's data: msgToSend: " + msgToSend);
								dataOutputStream.writeUTF(msgToSend);

								DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
								String msgRcvd = dataInputStream.readUTF();
								Log.v(TAG, "In clientTask 16 exception, predecessor's data: msgRcvd: " + msgRcvd);

								String[] getDataResult;
								if (!msgRcvd.equals("")) {
									Log.v(TAG, "In clientTask 17 exception, predecessor's data: in if");
									getDataResult = msgRcvd.split("::");
									Log.v(TAG, "In clientTask 18 exception, predecessor's data: Received array:" + Arrays.toString(getDataResult));
									for (int k = 0; k < getDataResult.length; k += 2) {
										if (!getDataResult[i].equals("GetDataResult")) {
											ContentValues contentValues = new ContentValues();
											contentValues.put("key", getDataResult[k]);
											contentValues.put("value", getDataResult[k + 1]);

											Log.v(TAG, "In clientTask, checking if key exists already 3 1: key: " + getDataResult[k]);
											SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
											String TBLE_NAME = "SimpleDynamoDB_" + i;
											sqLiteQueryBuilder.setTables(TBLE_NAME);
											String sele = "key" + " like '" + getDataResult[k] + "'";
											Cursor tempCursor = sqLiteQueryBuilder.query(sqLiteDatabase, null, sele, null, null, null, null);
											Log.v(TAG, "In clientTask, checking if key exists already 3 2: key: " + getDataResult[k]);
											if(tempCursor.getCount() > 0){
												String[] select = new String[]{getDataResult[k]};
												Log.v(TAG, "In clientTask, deleting key: "+getDataResult[k]+" in 3 TBLE_NAME: " + TBLE_NAME);
												sqLiteDatabase.delete(TBLE_NAME,"key = ?", select);
											}

											sqLiteDatabase.insert("SimpleDynamoDB_" + i, null, contentValues);
										} else
											k--;
										Log.v(TAG, "In clientTask 19 exception, predecessor's data: key: " + getDataResult[k] + " value: " + getDataResult[k + 1]);
									}
									Log.v(TAG, "In clientTask 20 exception, predecessor's data: end of for loop");
								}

								dataOutputStream.close();
								dataInputStream.close();
								socket.close();
							} catch (Exception ex){
								Log.v(TAG, "In clientTask exception 2 while getting predecessor's data: myPort "+myPort+" "+ex.getMessage());
							}
						}
					}
				}
			}
//			} catch (Exception e) {
//				System.out.println("In clientTask, Exception while calling successor port "+ Integer.parseInt(indexPortMap.get(i))*2 + e.getMessage());
//			}

//			}
			return null;
		}
		@Override
		protected void onPostExecute(Void aVoid){
			super.onPostExecute(aVoid);
			if(lockObject.isHeldByCurrentThread()){
				lockObject.unlock();
			}
		}
	}
}