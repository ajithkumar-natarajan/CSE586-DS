package edu.buffalo.cse.cse486586.simpledht;


import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class SimpleDHTDBHelper extends SQLiteOpenHelper {

    //Referred from https://www.journaldev.com/9438/android-sqlite-database-example-tutorial

    static final String DB_NAME = "Simple_DHT.DB";
    static final int DB_VERSION = 1;

    public SimpleDHTDBHelper(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        SimpleDHTDBManager.onCreate(sqLiteDatabase);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int oldVersion, int newVersion) {
        SimpleDHTDBManager.onUpgrade(sqLiteDatabase);
    }
}