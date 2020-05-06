package edu.buffalo.cse.cse486586.simpledynamo;


import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class SimpleDynamoDBHelper extends SQLiteOpenHelper {

    //Referred from https://www.journaldev.com/9438/android-sqlite-database-example-tutorial

    static final String DB_NAME = "Simple_Dynamo.DB";
    static final int DB_VERSION = 1;

    public SimpleDynamoDBHelper(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
//        SimpleDynamoDBManager.onCreate(sqLiteDatabase);
    }

    public void createDB(SQLiteDatabase sqLiteDatabase, String TABLE_NAME) {
        SimpleDynamoDBManager.onCreate(sqLiteDatabase, TABLE_NAME);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int oldVersion, int newVersion) {
//        SimpleDynamoDBManager.onUpgrade(sqLiteDatabase);
    }
}