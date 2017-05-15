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

import java.io.Closeable;
import java.io.IOException;
import java.util.*;

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



    public Result get(Transaction t, final Get get) throws IOException {
        return table.get(get);
    }

     /*

    public Result get(Transaction t, final Get get) throws IOException {
        HBaseTransaction transaction = (HBaseTransaction) t;

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
                    tsget.addColumn(family, CellUtils.addShadowCellSuffix(qualifier));
                }
            }
        }

        // Return the KVs that belong to the transaction snapshot, ask for more
        // versions if needed
        Result result = table.get(tsget);
        List<Cell> filteredKeyValues = Collections.emptyList();
        if (!result.isEmpty()) {
            filteredKeyValues = filterCellsForSnapshot(result.listCells(), transaction, tsget.getMaxVersions());
        }

        return Result.create(filteredKeyValues);
    }


    List<Cell> filterCellsForSnapshot(List<Cell> rawCells, HBaseTransaction transaction,
                                      int versionsToRequest) throws IOException {

        assert (rawCells != null && transaction != null && versionsToRequest >= 1);

        List<Cell> keyValuesInSnapshot = new ArrayList<>();
        List<Get> pendingGetsList = new ArrayList<>();

        int numberOfVersionsToFetch = versionsToRequest * 2;
        if (numberOfVersionsToFetch < 1) {
            numberOfVersionsToFetch = versionsToRequest;
        }

        Map<Long, Long> commitCache = buildCommitCache(rawCells);

        for (Collection<Cell> columnCells : groupCellsByColumnFilteringShadowCells(rawCells)) {
            boolean snapshotValueFound = false;
            Cell oldestCell = null;
            for (Cell cell : columnCells) {
                if (isCellInSnapshot(cell, transaction, commitCache)) {
                    if (!CellUtil.matchingValue(cell, CellUtils.DELETE_TOMBSTONE)) {
                        keyValuesInSnapshot.add(cell);
                    }
                    snapshotValueFound = true;
                    break;
                }
                oldestCell = cell;
            }
            if (!snapshotValueFound) {
                assert (oldestCell != null);
                Get pendingGet = createPendingGet(oldestCell, numberOfVersionsToFetch);
                pendingGetsList.add(pendingGet);
            }
        }

        if (!pendingGetsList.isEmpty()) {
            Result[] pendingGetsResults = table.get(pendingGetsList);
            for (Result pendingGetResult : pendingGetsResults) {
                if (!pendingGetResult.isEmpty()) {
                    keyValuesInSnapshot.addAll(
                            filterCellsForSnapshot(pendingGetResult.listCells(), transaction, numberOfVersionsToFetch));
                }
            }
        }

        Collections.sort(keyValuesInSnapshot, KeyValue.COMPARATOR);

        assert (keyValuesInSnapshot.size() <= rawCells.size());
        return keyValuesInSnapshot;
    }

    private Map<Long, Long> buildCommitCache(List<Cell> rawCells) {

        Map<Long, Long> commitCache = new HashMap<>();

        for (Cell cell : rawCells) {
            if (CellUtils.isShadowCell(cell)) {
                commitCache.put(cell.getTimestamp(), Bytes.toLong(CellUtil.cloneValue(cell)));
            }
        }

        return commitCache;
    }

    private boolean isCellInSnapshot(Cell kv, HBaseTransaction transaction, Map<Long, Long> commitCache)
            throws IOException {

        long startTimestamp = transaction.getStartTimestamp();

        if (kv.getTimestamp() == startTimestamp) {
            return true;
        }

        com.google.common.base.Optional<Long> commitTimestamp =
                tryToLocateCellCommitTimestamp(transaction.getTransactionManager(), transaction.getEpoch(), kv,
                        commitCache);

        return commitTimestamp.isPresent() && commitTimestamp.get() < startTimestamp;
    }

    private Get createPendingGet(Cell cell, int versionCount) throws IOException {

        Get pendingGet = new Get(CellUtil.cloneRow(cell));
        pendingGet.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
        pendingGet.addColumn(CellUtil.cloneFamily(cell), CellUtils.addShadowCellSuffix(cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierLength()));
        pendingGet.setMaxVersions(versionCount);
        pendingGet.setTimeRange(0, cell.getTimestamp());

        return pendingGet;
    }

    private Optional<Long> tryToLocateCellCommitTimestamp(AbstractTransactionManager transactionManager,
                                                          long epoch,
                                                          Cell cell,
                                                          Map<Long, Long> commitCache)
            throws IOException {

        CommitTimestamp tentativeCommitTimestamp =
                transactionManager.locateCellCommitTimestamp(
                        cell.getTimestamp(),
                        epoch,
                        new CommitTimestampLocatorImpl(
                                new HBaseCellId(table,
                                        CellUtil.cloneRow(cell),
                                        CellUtil.cloneFamily(cell),
                                        CellUtil.cloneQualifier(cell),
                                        cell.getTimestamp()),
                                commitCache));

        // If transaction that added the cell was invalidated
        if (!tentativeCommitTimestamp.isValid()) {
            return Optional.absent();
        }

        switch (tentativeCommitTimestamp.getLocation()) {
            case COMMIT_TABLE:
                // If the commit timestamp is found in the persisted commit table,
                // that means the writing process of the shadow cell in the post
                // commit phase of the client probably failed, so we heal the shadow
                // cell with the right commit timestamp for avoiding further reads to
                // hit the storage
                healShadowCell(cell, tentativeCommitTimestamp.getValue());
                return Optional.of(tentativeCommitTimestamp.getValue());
            case CACHE:
            case SHADOW_CELL:
                return Optional.of(tentativeCommitTimestamp.getValue());
            case NOT_PRESENT:
                return Optional.absent();
            default:
                assert (false);
                return Optional.absent();
        }
    }



    void healShadowCell(Cell cell, long commitTimestamp) {
        Put put = new Put(CellUtil.cloneRow(cell));
        byte[] family = CellUtil.cloneFamily(cell);
        byte[] shadowCellQualifier = CellUtils.addShadowCellSuffix(cell.getQualifierArray(),
                cell.getQualifierOffset(),
                cell.getQualifierLength());
        put.add(family, shadowCellQualifier, cell.getTimestamp(), Bytes.toBytes(commitTimestamp));
        try {
            healerTable.put(put);
        } catch (IOException e) {
            LOG.warn("Failed healing shadow cell for kv {}", cell, e);
        }
    }

    static ImmutableList<Collection<Cell>> groupCellsByColumnFilteringShadowCells(List<Cell> rawCells) {

        Predicate<Cell> shadowCellFilter = new Predicate<Cell>() {

            @Override
            public boolean apply(Cell cell) {
                return cell != null && !CellUtils.isShadowCell(cell);
            }

        };

        Function<Cell, ColumnWrapper> cellToColumnWrapper = new Function<Cell, ColumnWrapper>() {

            @Override
            public ColumnWrapper apply(Cell cell) {
                return new ColumnWrapper(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell));
            }

        };

        return Multimaps.index(Iterables.filter(rawCells, shadowCellFilter), cellToColumnWrapper)
                .asMap().values()
                .asList();
    }

    public ResultScanner getScanner(Transaction tx, Scan scan) throws IOException {
        return table.getScanner(scan);
    }

    */


}
