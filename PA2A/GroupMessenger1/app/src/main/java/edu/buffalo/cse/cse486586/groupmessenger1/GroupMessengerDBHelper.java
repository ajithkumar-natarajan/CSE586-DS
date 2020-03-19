package edu.buffalo.cse.cse486586.groupmessenger1;


import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class GroupMessengerDBHelper extends SQLiteOpenHelper {

    //Referred from https://www.journaldev.com/9438/android-sqlite-database-example-tutorial

    static final String DB_NAME = "Group_Messenger_1.DB";
    static final int DB_VERSION = 1;

    public GroupMessengerDBHelper(Context context) {
        super(context, DB_NAME, null, DB_VERSION);
    }

    @Override
    public void onCreate(SQLiteDatabase sqLiteDatabase) {
        GroupMessengerDBManager.onCreate(sqLiteDatabase);
    }

    @Override
    public void onUpgrade(SQLiteDatabase sqLiteDatabase, int oldVersion, int newVersion) {
        GroupMessengerDBManager.onUpgrade(sqLiteDatabase, oldVersion, newVersion);
    }
}