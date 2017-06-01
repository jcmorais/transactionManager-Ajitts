/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hbase;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

public class HBaseTransaction extends AbstractTransaction<HBaseCellId> {
    private static final Logger LOG = LoggerFactory.getLogger(HBaseTransaction.class);
    
    
    private Map<TTable, List<Put>> puts;

    HBaseTransaction(long startTS, long commitTS, Set<HBaseCellId> writeSet, HBaseTransactionManager tm,
                     Set<Long> abortedTransactions) {
        super(startTS, commitTS, writeSet, tm, abortedTransactions);
        this.puts = new HashMap<>();
    }


    public void serialize() throws IOException {

        StringBuilder sb = new StringBuilder();

        for (Map.Entry<TTable, List<Put>> table : puts.entrySet()) {
            sb.append("{\"table\":"+table.getKey().getTableName()+",");
            sb.append("\"puts\":{");
            for (Put put : table.getValue()) {
                sb.append(put.toJSON());
            }
            sb.append("}}");
        }

        FileOutputStream fos = null;
        fos = new FileOutputStream("logs/"+getCommitTimestamp());
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        oos.writeObject(sb.toString());
        oos.close();
    }

    
    public void addPut(TTable tTable, Put put){
        if (! puts.containsKey(tTable))
            puts.put(tTable, new ArrayList<>());

        puts.get(tTable).add(put);
    }
    
    
    @Override
    public void cleanup() {
        Set<HBaseCellId> writeSet = getWriteSet();
        for (final HBaseCellId cell : writeSet) {
            Delete delete = new Delete(cell.getRow());
            delete.deleteColumn(cell.getFamily(), cell.getQualifier(), getCommitTimestamp());
            try {
                cell.getTable().delete(delete);
            } catch (IOException e) {
                //LOG.warn("Failed cleanup cell {} for Tx {}. This issue has been ignored", cell, getTransactionId(), e);
            }
        }
        try {
            flushTables();
        } catch (IOException e) {
            LOG.warn("Failed flushing tables for Tx {}", getTransactionId(), e);
        }
    }
    
    
    public void flushPuts(){
        for (TTable tTable : puts.keySet()) {
            tTable.flushPuts(puts.get(tTable));
        }
    }
    
    

    /**
     * Flushes pending operations for tables touched by transaction
     * @throws IOException in case of any I/O related issues
     */
    public void flushTables() throws IOException {

        for (HTableInterface writtenTable : getWrittenTables()) {
            writtenTable.flushCommits();
        }

    }

    // ****************************************************************************************************************
    // Helper methods
    // ****************************************************************************************************************

    private Set<HTableInterface> getWrittenTables() {
        HashSet<HBaseCellId> writeSet = (HashSet<HBaseCellId>) getWriteSet();
        Set<HTableInterface> tables = new HashSet<HTableInterface>();
        for (HBaseCellId cell : writeSet) {
            tables.add(cell.getTable());
        }
        return tables;
    }

}
