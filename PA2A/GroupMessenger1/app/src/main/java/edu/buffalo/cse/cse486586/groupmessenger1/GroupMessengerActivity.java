package edu.buffalo.cse.cse486586.groupmessenger1;

import android.app.Activity;
import android.content.ContentValues;
import android.content.Context;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.telephony.TelephonyManager;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
public class GroupMessengerActivity extends Activity {

    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT[] = new String[]{"11108","11112","11116","11120","11124"};
    static final int SERVER_PORT = 10000;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        
        /*
         * Registers OnPTestClickListener for "button1" in the layout, which is the "PTest" button.
         * OnPTestClickListener demonstrates how to access a ContentProvider.
         */
        findViewById(R.id.button1).setOnClickListener(
                new OnPTestClickListener(tv, getContentResolver()));
        
        /*
         * TODO: You need to register and implement an OnClickListener for the "Send" button.
         * In your implementation you need to get the message from the input box (EditText)
         * and send it to other AVDs.
         */

//        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
//        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
//        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
//
//        try {
//            /*
//             * Create a server socket as well as a thread (AsyncTask) that listens on the server
//             * port.
//             *
//             * AsyncTask is a simplified thread construct that Android provides. Please make sure
//             * you know how it works by reading
//             * http://developer.android.com/reference/android/os/AsyncTask.html
//             */
//            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
//            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
//        } catch (IOException e) {
//            /*
//             * Log is a good way to debug your code. LogCat prints out all the messages that
//             * Log class writes.
//             *
//             * Please read http://developer.android.com/tools/debugging/debugging-projects.html
//             * and http://developer.android.com/tools/debugging/debugging-log.html
//             * for more information on debugging.
//             */
//            Log.e(TAG, "Can't create a ServerSocket");
//            return;
//        }

        //Reusing code from PA1

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        Log.v("portStr", portStr);
        final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));
        Log.i("myport", myPort);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket",e);
            return;
        }

        final EditText editText = (EditText) findViewById(R.id.editText1);
        final Button b4 = (Button) findViewById(R.id.button4);
        b4.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String msg = editText.getText().toString() + "\n";
                editText.setText("");
                new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, msg, myPort);
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_group_messenger, menu);
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        int count = 0;
        Uri.Builder uriBuilder = new Uri.Builder();

                @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];

            /*
             * TODO: Fill in your server code that receives messages and passes them
             * to onProgressUpdate().
             */
            /*
            Code from PA1
            */
            try {
                uriBuilder.scheme("content");
                uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger1.provider");
                Uri uri = uriBuilder.build();

                while(true)
                {
                    Socket socket = serverSocket.accept();
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());

                    String msgRcvd = dataInputStream.readUTF();

                    ContentValues contentValues = new ContentValues();
                    contentValues.put("key", String.valueOf(count));
                    contentValues.put("value", msgRcvd);
                    count ++;
                    Log.v("Inserting now", contentValues.toString());
                    getContentResolver().insert(uri, contentValues);
                    publishProgress(msgRcvd);
                    dataOutputStream.writeUTF(msgRcvd);
                    socket.close();
                }
            }catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            }catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }catch (Exception e) {
                Log.e(TAG, "ServerTask failed");
                Log.e(TAG, e.getMessage());
            }
            return null;
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String strReceived = strings[0].trim();
//            TextView remoteTextView = (TextView) findViewById(R.id.textView1);
//            remoteTextView.append(strReceived + "\t\n");
//            TextView localTextView = (TextView) findViewById(R.id.local_text_display);
//            localTextView.append("\n");

            TextView textView = (TextView) findViewById(R.id.textView1);
            textView.append(strReceived+"\t\n");
            /*
             * The following code creates a file in the AVD's internal storage and stores a file.
             *
             * For more information on file I/O on Android, please take a look at
             * http://developer.android.com/training/basics/data-storage/files.html
             */

            return;
        }
    }

    /***
     * ClientTask is an AsyncTask that should send a string over the network.
     * It is created by ClientTask.executeOnExecutor() call whenever OnKeyListener.onKey() detects
     * an enter key press event.
     *
     * @author stevko
     *
     */
    private class ClientTask extends AsyncTask<String, Void, Void> {

        /*
        References:
        Reusing code from PA1
         */

        @Override
        protected Void doInBackground(String... msgs) {
            try {
                for(int i=0;i< 5;i++) {
                    Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT[i]));

                    String msgToSend = msgs[0];
                    /*
                     * TODO: Fill in your client code that sends out a message.
                     */
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                    dataOutputStream.writeUTF(msgToSend);
                }

            } catch (UnknownHostException e) {
                Log.e(TAG, "ClientTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ClientTask socket IOException");
            }

            return null;
        }
    }
}