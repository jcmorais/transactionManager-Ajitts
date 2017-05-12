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

import com.google.common.base.Optional;

import java.util.*;


public abstract class AbstractTransaction<T extends CellId> implements Transaction {

    private transient Map<String, Object> metadata = new HashMap<>();
    private final HBaseTransactionManager transactionManager;
    private final long startTimestamp;
    //private final long epoch;
    private long commitTimestamp;
    private boolean isRollbackOnly;
    private final Set<T> writeSet;
    private Status status = Status.RUNNING;

    /**
     * Base constructor
     *
     * @param startTimestamp
     *            transaction start timestamp
     * @param writeSet
     *            initial write set for the transaction.
     *            Should be empty in most cases.
     * @param HBaseTransactionManager
     *            transaction manager associated to this transaction.
     *            Usually, should be the one that created the transaction
     *            instance.
     */
    public AbstractTransaction(long startTimestamp,
                               long commitTimestamp,
                               Set<T> writeSet,
                               HBaseTransactionManager HBaseTransactionManager) {
        this.startTimestamp = startTimestamp;
        this.commitTimestamp = commitTimestamp;
        this.writeSet = writeSet;
        this.transactionManager = HBaseTransactionManager;
    }

    /**
     * Allows to define specific clean-up task for transaction implementations
     */
    public abstract void cleanup();


    /**
     * In AJITTS the transaction id is the commitTimestamp (in omid is the startTimestamp)
     */
    @Override
    public long getTransactionId() {
        return commitTimestamp;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    /**
     * @see Transaction#isRollbackOnly()
     */
    @Override
    public void setRollbackOnly() {
        isRollbackOnly = true;
    }


    @Override
    public boolean isRollbackOnly() {
        return isRollbackOnly;
    }

    /**
     * Returns transaction manager associated to this transaction.
     * @return transaction manager
     */
    public HBaseTransactionManager getTransactionManager() {
        return transactionManager;
    }

    /**
     * Returns the start timestamp for this transaction.
     * @return start timestamp
     */
    public long getStartTimestamp() {
        return startTimestamp;
    }

    /**
     * Returns the commit timestamp for this transaction.
     * @return commit timestamp
     */
    public long getCommitTimestamp() {
        return commitTimestamp;
    }

    /**
     * Sets the commit timestamp for this transaction.
     * @param commitTimestamp
     *            the commit timestamp to set
     */
    public void setCommitTimestamp(long commitTimestamp) {
        this.commitTimestamp = commitTimestamp;
    }

    /**
     * Sets the status for this transaction.
     * @param status
     *            the {@link Status} to set
     */
    public void setStatus(Status status) {
        this.status = status;
    }

    /**
     * Returns the current write-set for this transaction.
     * @return write set
     */
    public Set<T> getWriteSet() {
        return writeSet;
    }

    /**
     * Adds an element to the transaction write-set.
     * @param element
     *            the element to add
     */
    public void addWriteSetElement(T element) {
        writeSet.add(element);
    }

    @Override
    public String toString() {
        return String.format("Tx-%s [%s] (ST=%d, CT=%d) WriteSet %s",
                             Long.toHexString(getTransactionId()),
                             status,
                             startTimestamp,
                             commitTimestamp,
                             writeSet);
    }

    @Override
    public Optional<Object> getMetadata(String key) {
        return Optional.fromNullable(metadata.get(key));
    }

    /**
     * Expects they metadata stored under key "key" to be of the "Set" type,
     * append "value" to the existing set or creates a new one
     */
    @Override
    @SuppressWarnings("unchecked")
    public void appendMetadata(String key, Object value) {
        List existingValue = (List) metadata.get(key);
        if (existingValue == null) {
            List<Object> newList = new ArrayList<>();
            newList.add(value);
            metadata.put(key, newList);
        } else {
            existingValue.add(value);
        }
    }

    @Override
    public void setMetadata(String key, Object value) {
        metadata.put(key, value);
    }

}
