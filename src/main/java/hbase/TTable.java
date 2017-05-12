package hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by carlosmorais on 09/05/2017.
 */
public class TTable implements Closeable {
    private HTableInterface table;


    public TTable(Configuration conf, byte[] tableName) throws IOException {
        this(new HTable(conf, tableName));
    }

    public TTable(String tableName) throws IOException {
        this(HBaseConfiguration.create(), Bytes.toBytes(tableName));
    }

    public TTable(Configuration conf, String tableName) throws IOException {
        this(conf, Bytes.toBytes(tableName));
    }

    public TTable(HTableInterface table) {
        this.table = table;
    }


    @Override
    public void close() throws IOException {
        table.close();
    }


    public void put(Transaction tx, Put put) throws IOException {


        HBaseTransaction transaction = (HBaseTransaction) tx;

        final long startTimestamp = transaction.getStartTimestamp();
        // create put with correct ts
        final Put tsput = new Put(put.getRow(), startTimestamp);
        Map<byte[], List<Cell>> kvs = put.getFamilyCellMap();
        for (List<Cell> kvl : kvs.values()) {
            for (Cell c : kvl) {
                // Reach into keyvalue to update timestamp.
                // It's not nice to reach into keyvalue internals,
                // but we want to avoid having to copy the whole thing
                KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                Bytes.putLong(kv.getValueArray(), kv.getTimestampOffset(), startTimestamp);
                tsput.add(kv);

                transaction.addWriteSetElement(
                        new HBaseCellId(table,
                                CellUtil.cloneRow(kv),
                                CellUtil.cloneFamily(kv),
                                CellUtil.cloneQualifier(kv),
                                kv.getTimestamp()));
            }
        }

        table.put(tsput);
    }
    
    public Result get(Transaction t, Get get) throws IOException {

        // TODO: 11/05/2017 implements SI
        return table.get(get);
    }

    public ResultScanner getScanner(Transaction tx, Scan scan) throws IOException {
        return table.getScanner(scan);
    }


    public void delete(Transaction tx, Delete delete) throws IOException {
        table.delete(delete);
    }



    public void flushCommits() throws IOException {
        table.flushCommits();
    }




}
