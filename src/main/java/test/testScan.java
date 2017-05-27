package test;

import hbase.HBaseTransactionManager;
import hbase.RollbackException;
import hbase.TTable;
import hbase.Transaction;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by carlosmorais on 27/05/2017.
 */
public class testScan {

    public static final byte[] family = Bytes.toBytes("family");
    public static final byte[] qualifier1 = Bytes.toBytes("MY_Q1");

    public static void main(String[] args) throws IOException {

        HBaseTransactionManager tm = new HBaseTransactionManager();



        Transaction tx0 = tm.begin();


        TTable t = new TTable("testYCSB");

        Put p1 = new Put(Bytes.toBytes("user101"));
        p1.add(family, Bytes.toBytes("field0"), Bytes.toBytes(1000));
        p1.add(family, Bytes.toBytes("field1"), Bytes.toBytes(1001));
        p1.add(family, Bytes.toBytes("field2"), Bytes.toBytes(1002));
        p1.add(family, Bytes.toBytes("field3"), Bytes.toBytes(1003));
        p1.add(family, Bytes.toBytes("field4"), Bytes.toBytes(1004));
        p1.add(family, Bytes.toBytes("field5"), Bytes.toBytes(1005));
        p1.add(family, Bytes.toBytes("field6"), Bytes.toBytes(1006));
        p1.add(family, Bytes.toBytes("field7"), Bytes.toBytes(1007));
        p1.add(family, Bytes.toBytes("field8"), Bytes.toBytes(1008));
        p1.add(family, Bytes.toBytes("field9"), Bytes.toBytes(1009));
        t.put(tx0, p1);

        Put p2 = new Put(Bytes.toBytes("user102"));
        p2.add(family, Bytes.toBytes("field0"), Bytes.toBytes(1000));
        p2.add(family, Bytes.toBytes("field1"), Bytes.toBytes(1001));
        p2.add(family, Bytes.toBytes("field2"), Bytes.toBytes(1002));
        p2.add(family, Bytes.toBytes("field3"), Bytes.toBytes(1003));
        p2.add(family, Bytes.toBytes("field4"), Bytes.toBytes(1004));
        p2.add(family, Bytes.toBytes("field5"), Bytes.toBytes(1005));
        p2.add(family, Bytes.toBytes("field6"), Bytes.toBytes(1006));
        p2.add(family, Bytes.toBytes("field7"), Bytes.toBytes(1007));
        p2.add(family, Bytes.toBytes("field8"), Bytes.toBytes(1008));
        p2.add(family, Bytes.toBytes("field9"), Bytes.toBytes(1009));
        t.put(tx0, p2);

        Put p3 = new Put(Bytes.toBytes("user103"));
        p3.add(family, Bytes.toBytes("field0"), Bytes.toBytes(1000));
        p3.add(family, Bytes.toBytes("field1"), Bytes.toBytes(1001));
        p3.add(family, Bytes.toBytes("field2"), Bytes.toBytes(1002));
        p3.add(family, Bytes.toBytes("field3"), Bytes.toBytes(1003));
        p3.add(family, Bytes.toBytes("field4"), Bytes.toBytes(1004));
        p3.add(family, Bytes.toBytes("field5"), Bytes.toBytes(1005));
        p3.add(family, Bytes.toBytes("field6"), Bytes.toBytes(1006));
        p3.add(family, Bytes.toBytes("field7"), Bytes.toBytes(1007));
        p3.add(family, Bytes.toBytes("field8"), Bytes.toBytes(1008));
        p3.add(family, Bytes.toBytes("field9"), Bytes.toBytes(1009));
        t.put(tx0, p3);

        Put p4 = new Put(Bytes.toBytes("user104"));
        p4.add(family, Bytes.toBytes("field0"), Bytes.toBytes(1000));
        p4.add(family, Bytes.toBytes("field1"), Bytes.toBytes(1001));
        p4.add(family, Bytes.toBytes("field2"), Bytes.toBytes(1002));
        p4.add(family, Bytes.toBytes("field3"), Bytes.toBytes(1003));
        p4.add(family, Bytes.toBytes("field4"), Bytes.toBytes(1004));
        p4.add(family, Bytes.toBytes("field5"), Bytes.toBytes(1005));
        p4.add(family, Bytes.toBytes("field6"), Bytes.toBytes(1006));
        p4.add(family, Bytes.toBytes("field7"), Bytes.toBytes(1007));
        p4.add(family, Bytes.toBytes("field8"), Bytes.toBytes(1008));
        p4.add(family, Bytes.toBytes("field9"), Bytes.toBytes(1009));
        t.put(tx0, p4);

        Put p5 = new Put(Bytes.toBytes("user105"));
        p5.add(family, Bytes.toBytes("field0"), Bytes.toBytes(1000));
        p5.add(family, Bytes.toBytes("field1"), Bytes.toBytes(1001));
        p5.add(family, Bytes.toBytes("field2"), Bytes.toBytes(1002));
        p5.add(family, Bytes.toBytes("field3"), Bytes.toBytes(1003));
        p5.add(family, Bytes.toBytes("field4"), Bytes.toBytes(1004));
        p5.add(family, Bytes.toBytes("field5"), Bytes.toBytes(1005));
        p5.add(family, Bytes.toBytes("field6"), Bytes.toBytes(1006));
        p5.add(family, Bytes.toBytes("field7"), Bytes.toBytes(1007));
        p5.add(family, Bytes.toBytes("field8"), Bytes.toBytes(1008));
        p5.add(family, Bytes.toBytes("field9"), Bytes.toBytes(1009));
        t.put(tx0, p5);




          try {
              tm.commit(tx0);
              System.out.println("comitted");
          } catch (RollbackException e) {
              System.out.println("Abort: "+e.getMessage());Transaction tx = tm.begin();
          }

        Transaction tx = tm.begin();

        TTable t1 = new TTable("testYCSB");

        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("user101"));
        scan.setStopRow(Bytes.toBytes("user106"));


        ResultScanner scanner = t1.getScanner(tx, scan);

        Result r=null;
        while ((r = scanner.next()) != null)
            System.out.println("result: "+r.toString());


        try {
            tm.commit(tx);
            System.out.println("comitted");
        } catch (RollbackException e) {
            System.out.println("Abort: "+e.getMessage());
        }


        System.exit(0);

    }
}
