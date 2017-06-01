package test;

import client.Client;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by carlosmorais on 14/04/2017.
 */
public class Run {
    public static void main(String[] args) throws InterruptedException, IOException, DeserializationException {

        Put p = new Put(Bytes.toBytes("sasasa"));
        p.add(Bytes.toBytes("x1"), Bytes.toBytes("x2222"), Bytes.toBytes("x1276312786361287"));


        System.out.println(p.getAttributesMap());
        System.out.println(p.getFamilyCellMap().toString());

        System.out.println(p.getDurability().toString());
        System.out.println(p.toString());
        System.out.println(p.toJSON());


        System.out.println(p.getFamilyCellMap());
        System.out.println(p.get(Bytes.toBytes("x1"), Bytes.toBytes("x2222")));
    }

}
