package edu.buffalo.cse.cse486586.simpledynamo;

import android.database.sqlite.SQLiteDatabase;


public class SimpleDynamoDBManager {

    //Referred from https://www.journaldev.com/9438/android-sqlite-database-example-tutorial

    public static final String key = "key";
    public static final String value = "value";

//    public static final String TABLE_NAME = "SimpleDynamoDB";

//    private static final String CREATE_DB = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" + key + " TEXT PRIMARY KEY, " + value + " TEXT NOT NULL)";
//    private static final String DROP_DB = "DROP TABLE IF EXISTS "+ TABLE_NAME;

    public static void onCreate(SQLiteDatabase sqLiteDatabase, String TABLE_NAME) {
//        sqLiteDatabase.execSQL(DROP_DB);
        sqLiteDatabase.execSQL("CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (" + key + " TEXT, " + value + " TEXT NOT NULL)");
    }

    public static void onUpgrade(SQLiteDatabase sqLiteDatabase, String TABLE_NAME) {
        sqLiteDatabase.execSQL("DROP TABLE IF EXISTS "+ TABLE_NAME);
        onCreate(sqLiteDatabase, TABLE_NAME);
    }
}