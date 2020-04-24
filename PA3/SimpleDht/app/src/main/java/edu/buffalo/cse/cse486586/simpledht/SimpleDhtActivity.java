package edu.buffalo.cse.cse486586.simpledht;

import android.os.Bundle;
import android.app.Activity;
import android.text.method.ScrollingMovementMethod;
import android.view.Menu;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;
import android.database.Cursor;
import android.net.Uri;

public class SimpleDhtActivity extends Activity {
    static final String TAG = SimpleDhtActivity.class.getSimpleName();


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_simple_dht_main);
        
        final TextView tv = (TextView) findViewById(R.id.textView1);
        tv.setMovementMethod(new ScrollingMovementMethod());
        findViewById(R.id.button3).setOnClickListener(
                new OnTestClickListener(tv, getContentResolver()));

        Button LDump = (Button)findViewById(R.id.button1);
        Button GDump = (Button)findViewById(R.id.button2);

        LDump.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View view) {
                Cursor cursor = getContentResolver().query(Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider"), null, "@", null, null);

                tv.setText("");
                int key = cursor.getColumnIndex("key");
                int value = cursor.getColumnIndex("value");
                StringBuilder sb = new StringBuilder();

                if(cursor.moveToFirst()) {
                    while(true) {
                        sb.append(cursor.getString(key));
                        sb.append(": ");
                        sb.append(cursor.getString(value));
                        sb.append(System.lineSeparator());

                        if(!cursor.moveToNext())
                            break;
                    }
                }
                tv.setText(sb.toString());
            }
        });

        GDump.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View view) {
                Cursor cursor = getContentResolver().query(Uri.parse("content://edu.buffalo.cse.cse486586.simpledht.provider"), null, "*", null, null);
                tv.setText("");
                int key = cursor.getColumnIndex("key");
                int value = cursor.getColumnIndex("value");
                StringBuilder sb = new StringBuilder();

                if(cursor.moveToFirst()) {
                    while(true) {
                        sb.append(cursor.getString(key));
                        sb.append(": ");
                        sb.append(cursor.getString(value));
                        sb.append(System.lineSeparator());

                        if(!cursor.moveToNext())
                            break;
                    }
                }
                tv.setText(sb.toString());
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.activity_simple_dht_main, menu);
        return true;
    }

}
