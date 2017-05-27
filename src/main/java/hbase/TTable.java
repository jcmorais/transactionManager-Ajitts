package hbase;

import com.google.common.base.*;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import server.Sheduler;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

/**
 * Created by carlosmorais on 09/05/2017.
 */
public class TTable implements Closeable {
    private HTableInterface table;
    private static final Logger LOG = LoggerFactory.getLogger(TTable.class);


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

    public Result get(Transaction tx, final Get get) throws IOException {

        LOG.info("start Get: {}", get);

        HBaseTransaction transaction = (HBaseTransaction) tx;

        final long readTimestamp = transaction.getStartTimestamp();
        final Get tsget = new Get(get.getRow()).setFilter(get.getFilter());
        TimeRange timeRange = get.getTimeRange();
        long startTime = timeRange.getMin();
        long endTime = Math.min(timeRange.getMax(), readTimestamp + 1);
        tsget.setTimeRange(startTime, endTime).setMaxVersions(1);


        Map<byte[], NavigableSet<byte[]>> kvs = get.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null || qualifiers.isEmpty()) {
                tsget.addFamily(family);
            } else {
                for (byte[] qualifier : qualifiers) {
                    tsget.addColumn(family, qualifier);
                }
            }
        }
        LOG.info("TxGet = {}", tsget);

        // Return the KVs that belong to the transaction snapshot, ask for more
        // versions if needed
        Result result = table.get(tsget);
        LOG.info("Initial Result: {}", result);
        List<Cell> filteredKeyValues = Collections.emptyList();
        if (!result.isEmpty()) {
            filteredKeyValues = filterCellsForSnapshot(result.listCells(), transaction, tsget.getMaxVersions());
        }

        Result res = Result.create(filteredKeyValues);
        LOG.info("Final Result: {}", res);
        return res;
    }


    public ResultScanner getScanner(Transaction tx, Scan scan) throws IOException {

        HBaseTransaction transaction = (HBaseTransaction) tx;

        Scan tsscan = new Scan(scan);
        tsscan.setMaxVersions(1);
        tsscan.setTimeRange(0, transaction.getStartTimestamp() + 1);
        Map<byte[], NavigableSet<byte[]>> kvs = scan.getFamilyMap();
        for (Map.Entry<byte[], NavigableSet<byte[]>> entry : kvs.entrySet()) {
            byte[] family = entry.getKey();
            NavigableSet<byte[]> qualifiers = entry.getValue();
            if (qualifiers == null) {
                continue;
            }
            for (byte[] qualifier : qualifiers) {
                tsscan.addColumn(family, qualifier);
            }
        }

        return new TransactionalClientScanner(transaction, tsscan, 1);
    }



    List<Cell> filterCellsForSnapshot(List<Cell> rawCells, HBaseTransaction transaction, int versionsToRequest) throws IOException {

        assert (rawCells != null && transaction != null && versionsToRequest >= 1);

        LOG.info("filter cells for SI: {}", rawCells);

        Set<Long> aborts = transaction.getAbortedTransactions();
        LOG.info("abortedList: {}", aborts);

        List<Cell> cellsInSI = new ArrayList<>();
        List<Get> pendingGets = new ArrayList<>();

        for (Cell cell : rawCells) {
            if ( aborts.contains(cell.getTimestamp() ) ){
                LOG.info("Found a shitt cell, tx={} ", cell.getTimestamp());
                Get g = new Get(CellUtil.cloneRow(cell));
                g.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
                g.setTimeRange(0, cell.getTimestamp());
                pendingGets.add(g);
            }
            else
                cellsInSI.add(cell);
        }

        LOG.info("cells in SI: {}", cellsInSI);
        LOG.info("pending Get's: {}", pendingGets);

        if ( !pendingGets.isEmpty() ){
            Result[] pendingGetsResults = table.get(pendingGets);
            for (Result pendingGetResult : pendingGetsResults) {
                if (!pendingGetResult.isEmpty()) {
                    cellsInSI.addAll(filterCellsForSnapshot(pendingGetResult.listCells(), transaction, versionsToRequest));
                }
            }
        }

        LOG.info("filtered cells: {}", cellsInSI);

        return cellsInSI;

    }


    public void put(Transaction tx, Put put) throws IOException {

        HBaseTransaction transaction = (HBaseTransaction) tx;
        final long commitTimestamp = transaction.getCommitTimestamp();

        // create put with correct ts
        final Put tsput = new Put(put.getRow(), commitTimestamp);
        Map<byte[], List<Cell>> kvs = put.getFamilyCellMap();
        for (List<Cell> kvl : kvs.values()) {
            for (Cell c : kvl) {
                // Reach into keyvalue to update timestamp.
                // It's not nice to reach into keyvalue internals,
                // but we want to avoid having to copy the whole thing
                KeyValue kv = KeyValueUtil.ensureKeyValue(c);
                Bytes.putLong(kv.getValueArray(), kv.getTimestampOffset(), commitTimestamp);
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


    public void delete(Transaction tx, Delete delete) throws IOException {

        HBaseTransaction transaction = (HBaseTransaction) tx;
        final long commitTimestamp = transaction.getCommitTimestamp();
        boolean issueGet = false;

        final Put deleteP = new Put(delete.getRow(), commitTimestamp);
        final Get deleteG = new Get(delete.getRow());
        Map<byte[], List<Cell>> fmap = delete.getFamilyCellMap();
        if (fmap.isEmpty()) {
            issueGet = true;
        }
        for (List<Cell> cells : fmap.values()) {
            for (Cell cell : cells) {
                //CellUtils.validateCell(cell, startTimestamp);
                switch (KeyValue.Type.codeToType(cell.getTypeByte())) {
                    case DeleteColumn:
                        deleteP.add(CellUtil.cloneFamily(cell),
                                CellUtil.cloneQualifier(cell),
                                commitTimestamp,
                                CellUtils.DELETE_TOMBSTONE);
                        transaction.addWriteSetElement(
                                new HBaseCellId(table,
                                        delete.getRow(),
                                        CellUtil.cloneFamily(cell),
                                        CellUtil.cloneQualifier(cell),
                                        cell.getTimestamp()));
                        break;
                    case DeleteFamily:
                        deleteG.addFamily(CellUtil.cloneFamily(cell));
                        issueGet = true;
                        break;
                    case Delete:
                        if (cell.getTimestamp() == HConstants.LATEST_TIMESTAMP) {
                            deleteP.add(CellUtil.cloneFamily(cell),
                                    CellUtil.cloneQualifier(cell),
                                    commitTimestamp,
                                    CellUtils.DELETE_TOMBSTONE);
                            transaction.addWriteSetElement(
                                    new HBaseCellId(table,
                                            delete.getRow(),
                                            CellUtil.cloneFamily(cell),
                                            CellUtil.cloneQualifier(cell),
                                            cell.getTimestamp()));
                            break;
                        } else {
                            throw new UnsupportedOperationException(
                                    "Cannot delete specific versions on Snapshot Isolation.");
                        }
                    default:
                        break;
                }
            }
        }
        if (issueGet) {
            // It's better to perform a transactional get to avoid deleting more
            // than necessary
            Result result = this.get(transaction, deleteG);
            if (!result.isEmpty()) {
                for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entryF : result.getMap()
                        .entrySet()) {
                    byte[] family = entryF.getKey();
                    for (Map.Entry<byte[], NavigableMap<Long, byte[]>> entryQ : entryF.getValue().entrySet()) {
                        byte[] qualifier = entryQ.getKey();
                        deleteP.add(family, qualifier, CellUtils.DELETE_TOMBSTONE);
                        transaction.addWriteSetElement(new HBaseCellId(table, delete.getRow(), family, qualifier,
                                transaction.getStartTimestamp()));
                    }
                }
            }
        }

        if (!deleteP.isEmpty()) {
            table.put(deleteP);
        }
    }



    public void flushCommits() throws IOException {
        table.flushCommits();
    }



    protected class TransactionalClientScanner implements ResultScanner {

        private HBaseTransaction state;
        private ResultScanner innerScanner;
        private int maxVersions;

        TransactionalClientScanner(HBaseTransaction state, Scan scan, int maxVersions)
                throws IOException {
            this.state = state;
            this.innerScanner = table.getScanner(scan);
            this.maxVersions = maxVersions;
        }


        @Override
        public Result next() throws IOException {
            List<Cell> filteredResult = Collections.emptyList();
            while (filteredResult.isEmpty()) {
                Result result = innerScanner.next();
                if (result == null) {
                    return null;
                }
                if (!result.isEmpty()) {
                    filteredResult = filterCellsForSnapshot(result.listCells(), state, maxVersions);
                }
            }
            return Result.create(filteredResult);
        }

        // In principle no need to override, copied from super.next(int) to make
        // sure it works even if super.next(int)
        // changes its implementation
        @Override
        public Result[] next(int nbRows) throws IOException {
            // Collect values to be returned here
            ArrayList<Result> resultSets = new ArrayList<>(nbRows);
            for (int i = 0; i < nbRows; i++) {
                Result next = next();
                if (next != null) {
                    resultSets.add(next);
                } else {
                    break;
                }
            }
            return resultSets.toArray(new Result[resultSets.size()]);
        }

        @Override
        public void close() {
            innerScanner.close();
        }

        @Override
        public Iterator<Result> iterator() {
            return new ResultIterator(this);
        }

        // ------------------------------------------------------------------------------------------------------------
        // --------------------------------- Helper class for TransactionalClientScanner ------------------------------
        // ------------------------------------------------------------------------------------------------------------

        class ResultIterator implements Iterator<Result> {

            TransactionalClientScanner scanner;
            Result currentResult;

            ResultIterator(TransactionalClientScanner scanner) {
                try {
                    this.scanner = scanner;
                    currentResult = scanner.next();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public boolean hasNext() {
                return currentResult != null && !currentResult.isEmpty();
            }

            @Override
            public Result next() {
                try {
                    Result result = currentResult;
                    currentResult = scanner.next();
                    return result;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void remove() {
                throw new RuntimeException("Not implemented");
            }

        }

    }


}
