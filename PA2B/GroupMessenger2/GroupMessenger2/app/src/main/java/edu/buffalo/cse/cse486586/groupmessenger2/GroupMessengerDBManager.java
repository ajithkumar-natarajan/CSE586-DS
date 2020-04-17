package edu.buffalo.cse.cse486586.groupmessenger2;

import android.database.sqlite.SQLiteDatabase;


public class GroupMessengerDBManager {

    //Referred from https://www.journaldev.com/9438/android-sqlite-database-example-tutorial

    public static final String key = "key";
    public static final String value = "value";

    public static final String TABLE_NAME = "GroupMessengerDB";

    private static final String CREATE_DB = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" + key + " ," + value + ");";
    private static final String DROP_DB = "DROP TABLE IF EXISTS '"+ TABLE_NAME + "'";

    public static void onCreate(SQLiteDatabase sqLiteDatabase) {
//        sqLiteDatabase.execSQL(DROP_DB);
        sqLiteDatabase.execSQL(CREATE_DB);
    }

    public static void onUpgrade(SQLiteDatabase sqLiteDatabase, int oldVersion, int newVersion) {
        sqLiteDatabase.execSQL(DROP_DB);
        onCreate(sqLiteDatabase);
    }
}