package edu.buffalo.cse.cse486586.simpledht;

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
import java.util.Arrays;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
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

public class SimpleDhtProvider extends ContentProvider {

    class Node
    {
        String data;
        Node next;

        Node(String d)
        {
            data = d;
            next = null;
        }
    }

    class LinkedList
    {
        Node head;
        LinkedList(){
            head = null;
        }

        Node getHead(){
            return head;
        }

        void sortedInsert(Node new_node)
        {
            Node current = head;

            if (current == null)
            {
                new_node.next = new_node;
                head = new_node;
            }
            else if (current.data.compareTo(new_node.data)>0)
            {
                while (current.next != head)
                    current = current.next;

                current.next = new_node;
                new_node.next = head;
                head = new_node;
            }
            else
            {
                while (current.next != head &&
                        current.next.data.compareTo(new_node.data)<0)
                    current = current.next;

                new_node.next = current.next;
                current.next = new_node;
            }
        }

        void printList()
        {
            if (head != null)
            {
                Node temp = head;
                do
                {
                    System.out.println(temp.data + " ");
                    temp = temp.next;
                }  while (temp != head);
            }
        }
    }


    static final String TAG = SimpleDhtProvider.class.getSimpleName();
    final Uri uri = Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider");
    SimpleDHTDBHelper dbHelper;
    SimpleDHTDBManager dbManager;
    SQLiteDatabase sqLiteDatabase;
    static final int SERVER_PORT = 10000;
    Cursor dumpCursor;
    String queryResult;
    MatrixCursor starQueryCursor;
    String myPort;
    String currentPort;
    String currentPortHash;
    String predecessor = "predecessor";
    String successor = "successor";
    LinkedList chordList;
    Map<String, String> map;

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        // TODO Auto-generated method stub

        if (selection.equals("@")) {
            Log.v(TAG, "@ Delete ");
            dumpCursor = sqLiteDatabase.rawQuery("DROP table SimpleDHTDB", null);
            Log.v(TAG, "@ Delete " + dumpCursor.toString());
            return 0;
        }

        if(selection.equals("*")) {
            dumpCursor = sqLiteDatabase.rawQuery("DROP table SimpleDHTDB", null);
            try {
                Log.d(TAG, "In  star delete 1: " + selection);
                Socket socket5 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)) * 2);
                Log.d(TAG, "Star delete 2");
                DataOutputStream output5 = new DataOutputStream(socket5.getOutputStream());
                Log.d(TAG, "Star delete 3");
                String msgToSend5 = "StarDelete::" + currentPort;
                Log.d(TAG, "Star delete 4");
                output5.writeUTF(msgToSend5);
                Log.d(TAG, "Star delete 5 "+msgToSend5);
                DataInputStream input5 = new DataInputStream(socket5.getInputStream());
                Log.d(TAG, "Star delete 6");
                String msgRcvd3 = input5.readUTF();
                Log.d(TAG, "Star delete 7 " + msgRcvd3 + " " + currentPort);
                String[] starQueryResult;
                output5.close();
                input5.close();
                socket5.close();

                return 0;
            } catch (UnknownHostException uhe) {
                Log.d(TAG, "Star delete uhe " + uhe.getMessage());
            } catch (IOException io) {
                Log.d(TAG, "Star delete io " + io.getMessage());
            }
        }
        else {
            try {
                if (belongsToCurrentPartition(genHash(selection))) {
                    Log.d(TAG, "In delete 1: " + selection);
                    SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
                    sqLiteQueryBuilder.setTables(SimpleDHTDBManager.TABLE_NAME);
                    String[] sel = new String[]{selection};
                    sqLiteDatabase.delete(SimpleDHTDBManager.TABLE_NAME,"key = ?", sel);
                    Log.v(TAG, "In delete 2: " + selection);

                    Log.v(TAG, "In delete 4: " + queryResult);
                } else {
                    Log.d(TAG, "In delete, map: " + map);
                    Log.d(TAG, "In delete, successor: " + successor);
                    Log.d(TAG, "In delete, port: " + Integer.parseInt(map.get(successor)) * 2);
                    String msgToSend4 = "Delete" + "::" + selection;
                    Socket socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)) * 2);
                    Log.d(TAG, "delete calling successor selection: " + selection + " hash:" + genHash(selection));
                    DataOutputStream output4 = new DataOutputStream(socket4.getOutputStream());
                    output4.writeUTF(msgToSend4);
                    DataInputStream input4 = new DataInputStream(socket4.getInputStream());
                    String msgRcvd2 = input4.readUTF();
                    queryResult = msgRcvd2.split("::")[1];
                    output4.close();
                    input4.close();
                    socket4.close();
                }
            } catch (NoSuchAlgorithmException nsae) {
                Log.d(TAG, "Exception in delete: " + nsae.getMessage());
            } catch (UnknownHostException uhe) {
                Log.e(TAG, "Exception in delete, uhe: " + uhe.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "Exception in delete, IO " + e.getMessage());
            }
        }
        Log.v(TAG, "In delete 5: " + queryResult);

        return 0;
    }

    @Override
    public String getType(Uri uri) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        // TODO Auto-generated method stub

        String key = (String) values.get("key");
        String value = (String) values.get("value");
        try {
            Log.d(TAG, "In insert, key: "+key+" key hash: "+genHash(key)+" value: "+value);
            if(belongsToCurrentPartition(genHash(key))) {
                Log.d(TAG, "inserting!!!!!!!!! "+genHash(key));
                sqLiteDatabase.insert(dbManager.TABLE_NAME, null, values);
            }
            else {
                Log.d(TAG, "In insert, map: "+map);
                Log.d(TAG, "In insert, successor: "+successor);
                Log.d(TAG, "In insert, port: "+Integer.parseInt(map.get(successor))*2);
                String msgToSend3 = "Insert"+"::"+key+"::"+value;
                Socket socket3 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor))*2);
                Log.d(TAG, "Insert calling successor key: "+key+" hash:"+genHash(key));
                DataOutputStream output3 = new DataOutputStream(socket3.getOutputStream());
                output3.writeUTF(msgToSend3);
                DataInputStream input3 = new DataInputStream(socket3.getInputStream());
                String msgRcvd1 = input3.readUTF();
                output3.close();
                input3.close();
                socket3.close();
            }
        } catch (NoSuchAlgorithmException nsae){
            Log.e(TAG, "insert key nsae "+nsae.getMessage());
        } catch (UnknownHostException uhe){
            Log.e(TAG, "insert key uhe "+uhe.getMessage());
        } catch (IOException e){
            Log.e(TAG, "insert key IO "+e.getMessage());
        }
        return uri;

    }

    @Override
    public boolean onCreate() {
        // TODO Auto-generated method stub
        TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
        currentPort = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);  //5554
        myPort = String.valueOf((Integer.parseInt(currentPort) * 2));  //11108
        dbHelper = new SimpleDHTDBHelper(this.getContext());
        sqLiteDatabase = dbHelper.getWritableDatabase();
        chordList = new LinkedList();
        map = new HashMap<String, String>();
        map.put("33d6357cfaaf0f72991b0ecd8c56da066613c089", "5554");
        map.put("208f7f72b198dadd244e61801abe1ec3a4857bc9", "5556");
        map.put("abf0fd8db03e5ecb199a9b82929e9db79b909643", "5558");
        map.put("c25ddd596aa7c81fa12378fa725f706d54325d12", "5560");
        map.put("177ccecaec32c54b82d5aaafc18a2dadb753e3b1", "5562");
        starQueryCursor = new MatrixCursor(new String[]{"key", "value"});

        Log.e(TAG, "In onCreate -- currentPort: "+currentPort+", myPort: "+myPort);

        try{
            ServerSocket serverSocket = new ServerSocket();
            Log.e(TAG, "In onCreate -- serverSocket: 1");
            serverSocket.setReuseAddress(true);
            Log.e(TAG, "In onCreate -- serverSocket: 2");
            serverSocket.bind(new InetSocketAddress(SERVER_PORT));
            Log.e(TAG, "In onCreate -- serverSocket: 3");
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        }
        catch (IOException ioe){
            Log.e(TAG, "In onCreate -- Could not create server socket");
            Log.e(TAG, "In onCreate "+ ioe.getMessage());
            return false;
        }

        new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "", myPort);

        try {
            currentPortHash = genHash(currentPort);
        }
        catch (NoSuchAlgorithmException e){
            Log.e(TAG, "In onCreate -- Exception while generating port hash");
            e.printStackTrace();
        }
        return true;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
                        String sortOrder) {
        // TODO Auto-generated method stub
        if (selection.equals("@")) {
            Log.v(TAG, "@ Query ");
            dumpCursor = sqLiteDatabase.rawQuery("SELECT * FROM SimpleDHTDB", null);
            Log.v(TAG, "@ Query " + dumpCursor.toString());
            return dumpCursor;
        }


        if(selection.equals("*")) {
            dumpCursor = sqLiteDatabase.rawQuery("SELECT * FROM SimpleDHTDB", null);
            if (dumpCursor.moveToFirst()) {
                while (true) {
                    starQueryCursor.addRow(new Object[]{dumpCursor.getString(dumpCursor.getColumnIndex("key")), dumpCursor.getString(dumpCursor.getColumnIndex("value"))});
                    Log.d(TAG, "Star Query dump " + dumpCursor.getString(dumpCursor.getColumnIndex("key")) + " " + dumpCursor.getString(dumpCursor.getColumnIndex("value")));

                    if (!dumpCursor.moveToNext())
                        break;
                }
            }
            try {
                Log.d(TAG, "In  star query 1: " + selection);
                Socket socket5 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)) * 2);
                Log.d(TAG, "Star Query 2");
                DataOutputStream output5 = new DataOutputStream(socket5.getOutputStream());
                Log.d(TAG, "Star Query 3");
                String msgToSend5 = "StarQuery::" + currentPort;
                Log.d(TAG, "Star Query 4");
                output5.writeUTF(msgToSend5);
                Log.d(TAG, "Star Query 5 "+msgToSend5);
                DataInputStream input5 = new DataInputStream(socket5.getInputStream());
                Log.d(TAG, "Star Query 6");
                String msgRcvd3 = input5.readUTF();
                Log.d(TAG, "Star Query 7 " + msgRcvd3 + " " + currentPort);
                String[] starQueryResult;
                if (!msgRcvd3.equals("")) {
                    Log.d(TAG, "Star Query in if");
                    starQueryResult = msgRcvd3.split("::");
                    Log.d(TAG, "Star Query in for " + Arrays.toString(starQueryResult));
                    for (int i = 0; i < starQueryResult.length; i += 2) {
                        if (!starQueryResult[i].equals("StarQueryResult"))
                            starQueryCursor.addRow(new Object[]{starQueryResult[i], starQueryResult[i + 1]});
                        else
                            i--;
                        Log.d(TAG, "Star Query 8 " + starQueryResult[i] + " " + starQueryResult[i + 1]);
                    }
                    Log.d(TAG, "Star Query 9");
                }
                output5.close();
                input5.close();
                socket5.close();

                return starQueryCursor;
            } catch (UnknownHostException uhe) {
                Log.d(TAG, "Star query uhe " + uhe.getMessage());
            } catch (IOException io) {
                Log.d(TAG, "Star query io " + io.getMessage());
            }
        }
        else {
            try {
                if (belongsToCurrentPartition(genHash(selection))) {
                    Log.d(TAG, "In Query 1: " + selection);
                    SQLiteQueryBuilder sqLiteQueryBuilder = new SQLiteQueryBuilder();
                    sqLiteQueryBuilder.setTables(SimpleDHTDBManager.TABLE_NAME);
                    String sel = SimpleDHTDBManager.key + " like '" + selection + "'";
                    Cursor cursor = sqLiteQueryBuilder.query(sqLiteDatabase, projection, sel, selectionArgs, null, null, sortOrder);
                    Log.v(TAG, "In Query 2: " + selection);

                    if (cursor.moveToFirst()) {
                        while (true) {
                            if (cursor.getString((cursor.getColumnIndex(("key")))).equals(selection)) {
                                queryResult = cursor.getString(cursor.getColumnIndex("value"));
                                break;
                            }

                            if (!cursor.moveToNext())
                                break;
                        }
                    }

                    Log.v(TAG, "In Query 4: " + queryResult);
                } else {
                    Log.d(TAG, "In query, map: " + map);
                    Log.d(TAG, "In query, successor: " + successor);
                    Log.d(TAG, "In query, port: " + Integer.parseInt(map.get(successor)) * 2);
                    String msgToSend4 = "Query" + "::" + selection;
                    Socket socket4 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)) * 2);
                    Log.d(TAG, "Query calling successor selection: " + selection + " hash:" + genHash(selection));
                    DataOutputStream output4 = new DataOutputStream(socket4.getOutputStream());
                    output4.writeUTF(msgToSend4);
                    DataInputStream input4 = new DataInputStream(socket4.getInputStream());
                    String msgRcvd2 = input4.readUTF();
                    queryResult = msgRcvd2.split("::")[1];
                    output4.close();
                    input4.close();
                    socket4.close();
                }
            } catch (NoSuchAlgorithmException nsae) {
                Log.d(TAG, "Exception in Query: " + nsae.getMessage());
            } catch (UnknownHostException uhe) {
                Log.e(TAG, "Exception in query, uhe: " + uhe.getMessage());
            } catch (IOException e) {
                Log.e(TAG, "Exception in query, IO " + e.getMessage());
            }
        }
        Log.v(TAG, "In Query 5: " + queryResult);
        MatrixCursor returnCursor = new MatrixCursor(new String[]{"key", "value"});
        returnCursor.addRow(new Object[]{selection, queryResult});
        return returnCursor;

    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
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
        Log.e(TAG, "In belongs: pred succ: " + myPort+" "+predecessor+" "+successor);
//        if(predecessor.equals(successor)) {
//            Log.e(TAG, "belongsToCurrentPartition return true 1");
//            return true;
//        }
        try {
            if ((genHash(currentPort).compareTo(hash) >= 0 && predecessor.compareTo(hash) < 0) || (predecessor.compareTo(genHash(currentPort)) >= 0
                    && predecessor.compareTo(hash) <= 0) || (predecessor.compareTo(genHash(currentPort)) >= 0
                    && genHash(currentPort).compareTo(hash) >= 0)){
                Log.e(TAG, "belongsToCurrentPartition return true 2");
                return true;
            }
        }catch(NoSuchAlgorithmException nsae){
            Log.e(TAG, "belongsToCurrentPartition "+nsae.getMessage());
        }

        Log.e(TAG, "belongsToCurrentPartition return false");
        return false;
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        /*
        References:
        Reusing code from PA1
         */

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                Log.e(TAG, "In clientTask myPort: 1: "+myPort);
                Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11108);
                Log.e(TAG, "In clientTask myPort: 2");
                /*
                 * TODO: Fill in your client code that sends out a message.
                 */

                String msgToSend = "Join::"+myPort;
                Log.e(TAG, "In clientTask myPort: 3");

                DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                Log.e(TAG, "In clientTask myPort: 4");

                dataOutputStream.writeUTF(msgToSend);
                Log.e(TAG, "In clientTask myPort: 5");

                Log.e(TAG, "ClientTask: " + msgToSend);
                DataInputStream input = new DataInputStream(socket.getInputStream());
                String msgRcvd = "" ;
                msgRcvd = input.readUTF();
                Log.e(TAG, "ClientTask readUTF: " + msgRcvd);
            } catch (IOException e) {
                try{
                    Node chord = new Node(genHash(currentPort));
                    chordList.sortedInsert(chord);
                    predecessor = successor = genHash((currentPort));
                    Log.i(TAG, "zero inactive Chord in exception: ");
                    chordList.printList();
                }catch (NoSuchAlgorithmException nsae) {
                    Log.e(TAG, "ClientTask: " + nsae.getMessage());
                }
                Log.e(TAG, "ClientTask socket IOException");
            }catch (Exception e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            }

            return null;
        }
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        int count = 0;

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            Log.e(TAG, "In serverTask: 1");

            ServerSocket serverSocket = sockets[0];
            Log.e(TAG, "In serverTask: 2");

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            /*
            Code from PA1
            */


            while (true) {
                Socket socket = null;
                try {
                    Log.e(TAG, "In serverTask: 3");
                    socket = serverSocket.accept();
                    Log.e(TAG, "In serverTask: 4");
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    Log.e(TAG, "In serverTask: 5");
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    Log.e(TAG, "In serverTask: 6");

                    String msgRcvd = dataInputStream.readUTF(); // join::11108
                    Log.e(TAG, "In serverTask: 7" + msgRcvd);
                    String[] splitMsgRcvd = msgRcvd.split("::");


                    if (splitMsgRcvd[0].equals(("Join"))) {
                        dataOutputStream.writeUTF("Done");
                        String port = String.valueOf(Integer.parseInt(splitMsgRcvd[1]) / 2);
                        Node chord = new Node(genHash(port));
                        chordList.sortedInsert(chord);
                        map.put(chord.data, port);
                        Log.i(TAG, "Chord before serverTask: 8 "+port);
                        chordList.printList();

                        Log.e(TAG, "In serverTask: 8");

                        Node head = chordList.getHead();
                        Node lastElement = new Node("");
                        if (head != null) {
                            Node temp = head;
                            do {
                                lastElement = temp;
                                temp = temp.next;
                            } while (temp != head);
                        }
                        Log.e(TAG, "In serverTask: 9" +lastElement.data);
                        chordList.printList();
                        if (head != null) {
                            Node temp = head;
                            do {
                                Log.e(TAG, "In serverTask: 10 temp" +temp.data);
                                Log.e(TAG, "In serverTask: 10 pred" +lastElement.data);
                                Log.e(TAG, "In serverTask: 11 port " +Integer.parseInt(map.get(temp.data))*2);
                                if(((Integer.parseInt(map.get(temp.data))*2)+"").equals(myPort)){
                                    predecessor = lastElement.data;
                                    successor = temp.next.data;
                                    Log.e(TAG, "In serverTask 14: pred succ in if: " + myPort+" "+predecessor+" "+successor);
                                }
                                else {
                                    Socket socket1 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(temp.data)) * 2);

                                    Log.e(TAG, "In serverTask: 12 suc" + temp.next.data);
                                    String msgToSend1 = "Update::" + lastElement.data + "::" + temp.next.data;
                                    Log.e(TAG, "In serverTask: 13 msgtosend : " + msgToSend1);
                                    DataOutputStream output1 = new DataOutputStream(socket1.getOutputStream());
                                    DataInputStream input1 = new DataInputStream(socket1.getInputStream());

                                    output1.writeUTF(msgToSend1);
                                    String msgRcvd1 = "";
                                    msgRcvd1 = input1.readUTF();
                                    Log.e(TAG, "In serverTask: 13 msg received : " + msgRcvd1);
                                }
                                lastElement = temp;
                                temp = temp.next;
                            } while (temp != head);
                        }
                        Log.e(TAG, "In serverTask: 13");
                        dataInputStream.close();
                        dataOutputStream.close();
                    } else if(splitMsgRcvd[0].equals("Update")) {
                        Log.e(TAG, "In serverTask in update: " + Arrays.toString(splitMsgRcvd));
                        predecessor = splitMsgRcvd[1];
                        successor = splitMsgRcvd[2];
                        Log.e(TAG, "In serverTask 14: pred succ: " + myPort+" "+predecessor+" "+successor);
                        dataOutputStream.writeUTF("Done");
                        dataOutputStream.close();
                        dataInputStream.close();
                        socket.close();
                    }else if(splitMsgRcvd[0].equals("Insert")) {
                        Log.e(TAG, "In serverTask 15: message insert: " + Arrays.toString(splitMsgRcvd));
                        ContentValues contentValues = new ContentValues();
                        contentValues.put("key", splitMsgRcvd[1]);
                        contentValues.put("value", splitMsgRcvd[2]);
                        insert(uri, contentValues);
                        Log.e(TAG, "In serverTask 16: successor insert: " + successor+" "+splitMsgRcvd[1]);
                        dataOutputStream.writeUTF("Done");
                        dataOutputStream.close();
                        dataInputStream.close();
                        socket.close();
                    }else if(splitMsgRcvd[0].equals("Query")) {
                        Log.e(TAG, "In serverTask 16: query: " + Arrays.toString(splitMsgRcvd));
                        Cursor resultCursor = query(uri, null, splitMsgRcvd[1], null, null);
                        Log.e(TAG, "In serverTask 17: successor query: " + successor+" "+splitMsgRcvd[1]);
                        dataOutputStream.writeUTF("QueryResult"+"::"+queryResult);
                        dataOutputStream.close();
                        dataInputStream.close();
                        socket.close();
                    }else if(splitMsgRcvd[0].equals("StarQuery")) {
                        String routedStarQuery = "";
                        Log.e(TAG, "In serverTask 19: query: " + Arrays.toString(splitMsgRcvd));
                        Cursor starQueryServer = query(uri, null, "@", null, null);
                        Log.e(TAG, "In serverTask 20:");
                        StringBuilder starQueryResultString = new StringBuilder();
                        if (starQueryServer.moveToFirst()) {
                            while (true) {
                                starQueryResultString.append(starQueryServer.getString(starQueryServer.getColumnIndex("key")));
                                starQueryResultString.append("::");
                                starQueryResultString.append(starQueryServer.getString(starQueryServer.getColumnIndex("value")));
                                starQueryResultString.append("::");
                                if (!starQueryServer.moveToNext())
                                    break;
                            }
                        }
                        if(!splitMsgRcvd[1].equals(map.get(successor))) {
                            Socket socket6 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)) * 2);
                            DataOutputStream dos = new DataOutputStream(socket6.getOutputStream());
                            dos.writeUTF(msgRcvd);
                            DataInputStream dis = new DataInputStream(socket6.getInputStream());
                            routedStarQuery = dis.readUTF();

                            dis.close();
                            dos.close();
                            socket6.close();
                            starQueryServer.close();
                        }
                        if( starQueryResultString.toString() != null && starQueryCursor.toString() != "")
                            starQueryResultString.append(routedStarQuery);
                        else
                            starQueryResultString = new StringBuilder(routedStarQuery);
                        Log.e(TAG," Returning result to star query : "+ starQueryResultString.toString());
                        dataOutputStream.writeUTF(starQueryResultString.toString());   // sending key1:value1 , key2 : value2 , key3 : value3
                        dataInputStream.close();
                        dataOutputStream.close();
                        socket.close();
                    }else if(splitMsgRcvd[0].equals("Delete")) {
                        Log.e(TAG, "In serverTask 21: delete: " + Arrays.toString(splitMsgRcvd));
                        int returnDelete = delete(uri, splitMsgRcvd[1], null);
                        Log.e(TAG, "In serverTask 22: successor delete: " + successor+" "+splitMsgRcvd[1]);
                        dataOutputStream.writeUTF("Done_deleting");
                        dataOutputStream.close();
                        dataInputStream.close();
                        socket.close();
                    }else if(splitMsgRcvd[0].equals("StarDelete")) {
                        String routedStarQuery = "";
                        Log.e(TAG, "In serverTask 21: delete: " + Arrays.toString(splitMsgRcvd));
                        int delReturn = delete(uri, "@", null);
                        Log.e(TAG, "In serverTask 20:");

                        if(!splitMsgRcvd[1].equals(map.get(successor))) {
                            Socket socket6 = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(map.get(successor)) * 2);
                            DataOutputStream dos = new DataOutputStream(socket6.getOutputStream());
                            dos.writeUTF("Done_deleting");
                            DataInputStream dis = new DataInputStream(socket6.getInputStream());
                            routedStarQuery = dis.readUTF();

                            dis.close();
                            dos.close();
                            socket6.close();
                        }
                        Log.e(TAG," Returning result to star delete : ");
                        dataOutputStream.writeUTF("Done");   // sending key1:value1 , key2 : value2 , key3 : value3
                        dataInputStream.close();
                        dataOutputStream.close();
                        socket.close();
                    }
                } catch (Exception e) {
                    Log.e(TAG, "ClientTask UnknownHostException 1 " + e.getMessage());
                } finally {
                    try {
                        socket.close();
                    } catch (Exception e) {
                        Log.e(TAG, "ClientTask UnknownHostException 2"+ e.getMessage()  );
                    }
                }
            }
        }
    }
}