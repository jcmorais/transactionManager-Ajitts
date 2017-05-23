package test;

import hbase.HBaseTransactionManager;
import hbase.RollbackException;
import hbase.TTable;
import hbase.Transaction;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Created by carlosmorais on 27/04/2017.
 */
public class Test2 {
    public static final byte[] family = Bytes.toBytes("MY_CF");
    public static final byte[] qualifier1 = Bytes.toBytes("MY_Q1");
    public static final byte[] qualifier2 = Bytes.toBytes("MY_Q2");


    public static void main(String[] args) throws ExecutionException, InterruptedException, IOException {


        HBaseTransactionManager tm = new HBaseTransactionManager();
        Transaction tx = tm.begin();
        Transaction tx2 = tm.begin();


        TTable t = new TTable("MY_TEST");

        Put put = new Put(Bytes.toBytes("qwerty"));
        put.add(family, qualifier1, Bytes.toBytes(10));

        t.put(tx, put);



        TTable t2 = new TTable("MY_TEST");

        Put put2 = new Put(Bytes.toBytes("qwerty"));
        put2.add(family, qualifier1, Bytes.toBytes(11));

        t2.put(tx2, put2);

        try {
            tm.commit(tx);
            System.out.println("comitted");
        } catch (RollbackException e) {
            System.out.println("Abort: "+e.getMessage());
        }

        try {
            tm.commit(tx2);
            System.out.println("comitted");
        } catch (RollbackException e) {
            System.out.println("Abort: "+e.getMessage());
        }



        Transaction tx3 = tm.begin();
        Get get = new Get(Bytes.toBytes("qwerty"));
        get.addColumn(family, qualifier1);
        System.out.println(t.get(tx3, get));

        try {
            tm.commit(tx3);
        } catch (RollbackException e) {
            e.printStackTrace();
        }








        System.exit(0);

    }
}
