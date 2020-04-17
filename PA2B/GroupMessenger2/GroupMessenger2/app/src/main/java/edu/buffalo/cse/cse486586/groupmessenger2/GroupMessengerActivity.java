package edu.buffalo.cse.cse486586.groupmessenger2;

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
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GroupMessengerActivity is the main Activity for the assignment.
 * 
 * @author stevko
 *
 */
class Message{
    int senderID;
    String msg;
    boolean deliverable;
    int msgID;
    Double seqNo;

    public int getMsgID() {
        return msgID;
    }

    public void setMsgID(int msgID) {
        this.msgID = msgID;
    }

    public Double getSeqNo() {
        return seqNo;
    }

    public void setSeqNo(Double seqNo) {
        this.seqNo = seqNo;
    }

    Message(int senderID, String msg, boolean deliverable, int msgID, Double seqNo){
        this.senderID = senderID;
        this.msg = msg;
        this.deliverable = deliverable;
        this.msgID = msgID;
        this.seqNo = seqNo;
    }

    public int getSenderID() {
        return senderID;
    }

    public void setSenderID(int senderID) {
        this.senderID = senderID;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public boolean isDeliverable() {
        return deliverable;
    }

    public void setDeliverable(boolean deliverable) {
        this.deliverable = deliverable;
    }

    @Override
    public String toString(){
        return senderID+" "+msg+" "+deliverable;
    }
}

public class GroupMessengerActivity extends Activity {
    static final String TAG = GroupMessengerActivity.class.getSimpleName();
    static final String REMOTE_PORT[] = new String[]{"11108", "11112", "11116", "11120", "11124"};;
    static final int SERVER_PORT = 10000;
    String myPort;

    TreeMap<Double, Message> treemap;
    AtomicInteger sequenceNo;
    String request;
    String accepted;
    String proposed;
    Queue<Integer> proposedQueue;
    Queue<Integer> acceptedQueue;
    int deadProcess;
    int msgCount = 0;
    int seqNo = 0;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        treemap = new TreeMap<Double, Message>();
        sequenceNo = new AtomicInteger();
        request = "Request::";
        accepted = "Accepted::";
        proposed = "Proposed::";
        proposedQueue = new PriorityQueue<Integer>();
        acceptedQueue = new PriorityQueue<Integer>();
        deadProcess = -1;

        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_group_messenger);

        /*
         * TODO: Use the TextView to display your messages. Though there is no grading component
         * on how you display the messages, if you implement it, it'll make your debugging easier.
         */
        TextView tv = (TextView) findViewById(R.id.textView1);
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

        //Reusing code from PA1

        TelephonyManager tel = (TelephonyManager) this.getSystemService(Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
//        Log.v("portStr", TAG+" "+portStr);
        myPort = String.valueOf((Integer.parseInt(portStr) * 2));
//        Log.i("myport", TAG+" "+myPort);

        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
//            Log.e(TAG, "Can't create a ServerSocket",e);
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

    public static int getProcessID(String myPort){
        int port = Integer.parseInt(myPort);

        return (((port-11108)/4)+1);
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {
        Uri.Builder uriBuilder = new Uri.Builder();
        Uri uri;

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
                uriBuilder.authority("edu.buffalo.cse.cse486586.groupmessenger2.provider");
                uri = uriBuilder.build();

                while (true) {
                    Socket socket = serverSocket.accept();
//                    Log.e(TAG, "Worked 1");
                    DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
//                    Log.e(TAG, "Worked 2");
                    DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
//                    Log.e(TAG, "Worked 3");
                    String msgRcvd = dataInputStream.readUTF();
//                    Log.e(TAG, "Worked 4");
                    if (msgRcvd != null) {
//                        Log.e(TAG, "Worked 5");
                        serverProcessMsg(msgRcvd, dataOutputStream);
//                        Log.e(TAG, "Worked 6");
                    }
                }
            } catch (UnknownHostException e) {
//                Log.e(TAG, "ClientTask UnknownHostException");
            }catch (IOException e) {
//                Log.e(TAG, "ClientTask socket IOException");
            }catch (Exception e) {
//                Log.e(TAG, "ServerTask failed here");
//                Log.e(TAG, e.getMessage());
            }
            return null;
        }

        public void serverProcessMsg(String msg, DataOutputStream dataOutputStream){
            String[] splitMsg = msg.split("::");
//            Log.e(TAG, "Worked till 1");
//            Log.e(TAG, splitMsg.length+"");
            int senderID = Integer.parseInt(splitMsg[1]);
//            Log.e(TAG, "Worked till 2");

            try{
                if(deadProcess!=-1) {
                    for (Map.Entry<Double, Message> entry: treemap.entrySet()){
                        Double key = entry.getKey();
                        Message value = entry.getValue();

                        if(!value.isDeliverable() && value.getSenderID()==deadProcess) {
                            treemap.remove(key);
//                                Log.e(TAG, "deadProcess"+treemap.toString());
                        }
                    }
                }
//                int seqNo = 0;
                if(msg.contains(request)){
//                    Log.e(TAG, "Worked 1");
                    String msgContent = splitMsg[2];
//                    Log.e(TAG, "Worked 2");
                    synchronized (this) {
                        seqNo++;
//                                = incrementSequenceNo();
//                    Log.e(TAG, "Worked 3");
                    }
                    dataOutputStream.writeUTF(proposed+getProcessID(myPort)+"::"+seqNo);
//                    Log.e(TAG, "Worked 4");
                    dataOutputStream.flush();
//                    Log.e(TAG, "Worked 5");
                    Log.e(TAG, "Request"+msg);


                    Message message = new Message(senderID, msgContent, false, Integer.parseInt(splitMsg[3]), Double.valueOf(seqNo+(Double.valueOf(getProcessID(myPort)))/10));
                    treemap.put(Double.valueOf(seqNo+(Double.valueOf(getProcessID(myPort))/10)), message);
//                    Log.e(TAG, "Worked 6");
                }
                else{
                    int processID = Integer.parseInt(splitMsg[2]);
//                    Log.e(TAG, "Worked 7");
                    int acceptedSeqNo = Integer.parseInt(splitMsg[3]);
//                    Log.e(TAG, "Worked 8");
                    String msgContent = splitMsg[4];
//                    Log.e(TAG, "Worked 9");
//                    Log.e(TAG, "Accepted"+msg);
                    if(seqNo<acceptedSeqNo)
                        seqNo = acceptedSeqNo+1;
                    synchronized (treemap) {
                        for (Map.Entry entry : treemap.entrySet()) {
                            Message value = (Message) entry.getValue();
                            if (value.senderID == senderID && value.msgID == Integer.parseInt(splitMsg[5])) {
                                treemap.remove(entry.getKey());
                                break;
                            }
                        }
                        Message message = new Message(senderID, msgContent, true, Integer.parseInt(splitMsg[5]), Double.valueOf(acceptedSeqNo + (Double.valueOf(processID)) / 10));
                        //                    Log.e(TAG, "Worked 10");
                        treemap.put(Double.valueOf(acceptedSeqNo + (Double.valueOf(processID) / 10)), message);
                        //                    Log.e(TAG, "Worked 11");
                        acceptedQueue.add(acceptedSeqNo);
                        //                    Log.e(TAG, "Worked 12");
                    }
                    dataOutputStream.writeUTF("acknowledge");
//                    Log.e(TAG, "Worked 13");
                    dataOutputStream.flush();
//                    Log.e(TAG, acceptedSeqNo+getProcessID(myPort)/10+"");




                    int count = 0;
                    List<Message> messageList = new ArrayList<Message>(treemap.values());
                    Log.e(TAG, "Before"+treemap.toString());
//                    while(!messageList.isEmpty() && messageList.get(count).isDeliverable()) {
//                        Message priority = messageList.get(count);
//                        messageList.remove(count);
//                        treemap.remove(treemap.firstKey());
//                        publishProgress(priority.getMsg(), String.valueOf(sequenceNo.getAndIncrement()));
//                    }
                    while(!messageList.isEmpty()) {
                        Message priority = messageList.get(count);
                        if (priority.getSenderID() == deadProcess && !priority.isDeliverable()) {
                            messageList.remove(count);
                            treemap.remove(treemap.firstKey());
                        } else if (priority.isDeliverable()) {
                            messageList.remove(count);
                            treemap.remove(treemap.firstKey());
                            publishProgress(priority.getMsg(), String.valueOf(sequenceNo.getAndIncrement()));
                        } else
                            break;
                        ;
                        Log.e(TAG, "After" + treemap.toString());
                    }
                }
            }catch (Exception e) {
//                Log.e(TAG, "ServerTask failed");
//                Log.e(TAG, e.getMessage());
                deadProcess = senderID;
                if(deadProcess!=-1) {
                    for (Map.Entry<Double, Message> entry: treemap.entrySet()){
                        Double key = entry.getKey();
                        Message value = entry.getValue();

                        if(!value.isDeliverable() && value.getSenderID()==deadProcess) {
                            treemap.remove(key);
//                                Log.e(TAG, "deadProcess"+treemap.toString());
                        }
                    }
                }
            }
        }

        public int incrementSequenceNo() {
            if(proposedQueue.isEmpty()) {
                proposedQueue.add(0);

                return proposedQueue.peek();
            } else {
                int acceptedNo = acceptedQueue.size() == 0? 0:acceptedQueue.peek();
                int seqNo = Math.max(proposedQueue.peek(), acceptedNo) + 1;
                proposedQueue.add(seqNo);

                return seqNo;
            }
        }

        protected void onProgressUpdate(String...strings) {
            /*
             * The following code displays what is received in doInBackground().
             */
            String msg = strings[0].trim();
            int seqNo = Integer.parseInt(strings[1]);

            TextView tv = (TextView) findViewById(R.id.textView1);
            tv.append(msg+"\t\n");

            ContentValues contentValues = new ContentValues();
            contentValues.put("key", String.valueOf(seqNo));
            contentValues.put("value", msg);
            getContentResolver().insert(uri, contentValues);
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
        List<String> proposedSequence = new ArrayList<String>();

        @Override
        protected Void doInBackground(String... msgs) {
            String msgToSend = msgs[0];
            msgCount++;
            for (int i = 0; i < 5; i++) {
                if (i + 1 != deadProcess) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT[i]));
                        socket.setSoTimeout(500);
                        /*
                         * TODO: Fill in your client code that sends out a message.
                         */
                        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
                        dataOutputStream.writeUTF(request + getProcessID(myPort) + "::" + msgToSend +"::"+msgCount);
//                        Log.e(TAG, request + getProcessID(myPort) + "::" + msgToSend);
                        dataOutputStream.flush();

                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String msgRcvd = dataInputStream.readUTF();
//                        Log.e(TAG, msgRcvd);
                        if (msgRcvd != null) {
                            if (msgRcvd.contains(proposed)) {
                                String[] splitMsgs = msgRcvd.split("::");
                                int processID = Integer.parseInt(splitMsgs[1]);
                                int proposedSequenceNo = Integer.parseInt(splitMsgs[2]);
                                proposedSequence.add(processID + "::" + proposedSequenceNo);
                                socket.close();
                            }
                        }
                    } catch (Exception e) {
                        deadProcess = i + 1;
                    }
//                    return null;
                }
            }

            Double seqProcess = -1.0;
            int accept = -1;
            int processID = -1;
            for (String s : proposedSequence) {
                String[] split = s.split("::");
                Double arrValue = Double.valueOf(split[0])+(Double.valueOf(split[1])/10);
                int pid = Integer.parseInt(split[0]);
                int seqNum = Integer.parseInt(split[1]);
                if (arrValue >= seqProcess) {
                    accept = seqNum;
                    processID = pid;
                }
            }


            for (int i = 0; i < 5; i++) {
                if (i + 1 != deadProcess) {
                    try {
                        Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(REMOTE_PORT[i]));
                        socket.setSoTimeout(500);

                        DataOutputStream dataOutputStream = new DataOutputStream(socket.getOutputStream());
//                        Log.i(TAG, "Sending Agreed Sequence # - " + agreed + "." + processID + " - " + msgToSend);
                        dataOutputStream.writeUTF(accepted + getProcessID(myPort) + "::" + processID + "::" + accept + "::" + msgToSend+"::"+msgCount);

                        dataOutputStream.flush();

                        DataInputStream dataInputStream = new DataInputStream(socket.getInputStream());
                        String message = dataInputStream.readUTF();
                        if (message != null) {
                            if (message.equals("acknowledge")) {
//                                Log.i(TAG, "Received ACK for Agreed Sequence # - " + agreed + "." + processID + " - " + msgToSend);
                                socket.close();
                            }
                        }
                    } catch (Exception e) {
                        deadProcess = i + 1;
//                        Log.e(TAG, "(ACCEPT) Error: Dead Process " + e.getMessage());
                    }

//                    try {
//                        Thread.sleep(500);
//                    } catch (InterruptedException e1) {
//                        e1.printStackTrace();
//                    }
                }
            }
            return null;
        }
    }
}
