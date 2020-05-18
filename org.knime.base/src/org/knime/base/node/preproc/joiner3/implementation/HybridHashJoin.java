/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by KNIME AG, Zurich, Switzerland
 *  Website: http://www.knime.com; Email: contact@knime.com
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License, Version 3, as
 *  published by the Free Software Foundation.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses>.
 *
 *  Additional permission under GNU GPL version 3 section 7:
 *
 *  KNIME interoperates with ECLIPSE solely via ECLIPSE's plug-in APIs.
 *  Hence, KNIME and ECLIPSE are both independent programs and are not
 *  derived from each other. Should, however, the interpretation of the
 *  GNU GPL Version 3 ("License") under any applicable laws result in
 *  KNIME and ECLIPSE being a combined program, KNIME AG herewith grants
 *  you the additional permission to use and propagate KNIME together with
 *  ECLIPSE with only the license terms in place for ECLIPSE applying to
 *  ECLIPSE and the GNU GPL Version 3 applying for KNIME, provided the
 *  license terms of ECLIPSE themselves allow for the respective use and
 *  propagation of ECLIPSE together with KNIME.
 *
 *  Additional permission relating to nodes for KNIME that extend the Node
 *  Extension (and in particular that are based on subclasses of NodeModel,
 *  NodeDialog, and NodeView) and that only interoperate with KNIME through
 *  standard APIs ("Nodes"):
 *  Nodes are deemed to be separate and independent programs and to not be
 *  covered works.  Notwithstanding anything to the contrary in the
 *  License, the License does not apply to Nodes, you are not required to
 *  license Nodes under the License, and you are granted a license to
 *  prepare and propagate Nodes, in each case even if such Nodes are
 *  propagated with or for interoperation with KNIME.  The owner of a Node
 *  may freely choose the license terms applicable to such Node, including
 *  when such Node is propagated with or for interoperation with KNIME.
 * ---------------------------------------------------------------------
 *
 * History
 *   May 16, 2020 (carlwitt): created
 */
package org.knime.base.node.preproc.joiner3.implementation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.data.util.memory.MemoryAlertSystem.MemoryActionIndicator;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.streamable.StreamableFunction;

/**
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 */
// TODO remove to check serious ones
@SuppressWarnings("javadoc")
public class HybridHashJoin extends JoinImplementation {

    /**
     * @param settings
     * @param tables
     * @throws InvalidSettingsException
     */
    protected HybridHashJoin(final Joiner3Settings settings, final BufferedDataTable[] tables) {
        super(settings, tables);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BufferedDataTable twoWayJoin(final ExecutionContext exec, final BufferedDataTable leftTable,
        final BufferedDataTable rightTable) throws CanceledExecutionException, InvalidSettingsException {

        BufferedDataTable smaller = leftTable.size() <= rightTable.size() ? leftTable : rightTable;
        BufferedDataTable bigger  = leftTable.size() <= rightTable.size() ? rightTable: leftTable;
        return hybridHashJoin(exec, smaller, bigger);
    }

    /**
     *
     *
     * Only hash input buckets move between main memory and disk. They do so only during partitioning the hash input.
     * Probe input rows are either directly processed or go to disk.
     *
     * @param exec
     * @param hashInput
     * @param probeInput
     * @return
     * @throws CanceledExecutionException
     */
    private BufferedDataTable hybridHashJoin(final ExecutionContext exec, final BufferedDataTable hashInput,
        final BufferedDataTable probeInput) throws CanceledExecutionException {

        // use lowest n bits of the hash code to determine the bucket index (this is basically a modulo operation)
        // but, unlike %, the result is positive also for negative inputs (as long as numBits ≤ 31)
        final int numBits = 6;
        assert numBits <= 27; // 100M partitions should be enough. always.

        // this results in 2ˆn buckets
        final int numBuckets = 1 << numBits;
        // mask out n lowest bits from hash code by setting only lowest n bits to 1
        final int bitmask = (1 << numBits) - 1;

        // The row partitions of the hash input. Try to keep in memory, but put on disk if needed.
        // hashBucketsInMemory[i] == null indicates that the bucket is on disk.
        InMemoryBucket[] hashBucketsInMemory =
            Stream.generate(() -> new InMemoryBucket()).limit(numBuckets).toArray(InMemoryBucket[]::new);

        // the buckets of the hash input are kept in memory for offsets firstInMemoryBucket...numBuckets-1
        // the offsets 0...firstInMemoryBucket-1 are stored on disk.
        int firstInMemoryBucket = 0;

        // The row partitions of the hash input that have been migrated to disk due to low heap space.
        // Are initialized as soon as migration to disk is necessary.
        DiskBucket[] hashBucketsOnDisk = new DiskBucket[numBuckets];

        // Row partitions of the probe input. A bucket r is only used if the corresponding hash input bucket s can
        // not be held in main memory. The bucket pair (r, s) is then retrieved from disk later and joined in memory.
        DiskBucket[] probeBuckets =
            Stream.generate(() -> new DiskBucket()).limit(numBuckets).toArray(DiskBucket[]::new);

        MemoryActionIndicator softWarning = MemoryAlertSystem.getInstanceUncollected().newIndicator();

        { // phase 1: partition the hash input into buckets, keep as many buckets as possible in memory
            try (CloseableRowIterator hashRows = hashInput.iterator()) {

                while (hashRows.hasNext()) {

                    exec.checkCanceled();

                    // if memory is running low and there are hash buckets in-memory
                    if (softWarning.lowMemoryActionRequired() && firstInMemoryBucket < numBuckets) {
                        // migrate the next in-memory hash bucket to disk
                        // convert to disk bucket, release heap memory
                        hashBucketsOnDisk[firstInMemoryBucket] = hashBucketsInMemory[firstInMemoryBucket].toDisk();
                        firstInMemoryBucket++;
                    }

                    DataRow hashRow = hashRows.next();
                    JoinTuple joinAttributeValues = getJoinTuple(hashInput, hashRow);
                    int bucket = joinAttributeValues.hashCode() & bitmask;

                    if (bucket >= firstInMemoryBucket) {
                        // if the hash bucket is in memory, add and index row
                        hashBucketsInMemory[bucket].add(joinAttributeValues, hashRow, true);
                    } else {
                        // if the bucket is on disk store for joining later
                        hashBucketsOnDisk[bucket].add(joinAttributeValues, hashRow);
                    }

                }
            }
        }

        BufferedDataContainer resultContainer = exec.createDataContainer(m_outputSpec);

        { // phase 2: partitioning and probing of the probe input

            // process probe input: process rows immediately for which the matching bucket is in memory;
            // otherwise put the row into a disk bucket
            try (CloseableRowIterator iterator = probeInput.iterator()) {

                while (iterator.hasNext()) {

                    exec.checkCanceled();

                    DataRow probeRow = iterator.next();
                    JoinTuple joinAttributeValues = getJoinTuple(probeInput, probeRow);
                    int bucket = joinAttributeValues.hashCode() & bitmask;

                    if (bucket >= firstInMemoryBucket) {
                        // if hash bucket is in memory, output result directly
                        hashBucketsInMemory[bucket].joinSingleRow(joinAttributeValues, probeRow, resultContainer);
                    } else {
                        // if hash bucket is on disk, process this row later, when joining the matching buckets
                        probeBuckets[bucket].add(joinAttributeValues, probeRow);
                    }
                }
            }
        }

        { // phase 3: joining the rows that went to disk because not enough memory was available

            // join only the pairs of buckets that haven't been processed in memory
            for (int i = 0; i < firstInMemoryBucket; i++) {
                probeBuckets[i].join(hashBucketsOnDisk[i], resultContainer, exec);
            }

        }

        resultContainer.close();
        return resultContainer.getTable();

    }


    class DiskBucket  {
        BufferedDataTable rows;

        public void add(final JoinTuple joinAttributes, final DataRow row) {


        }

        public void join(final DiskBucket other, final DataContainer resultContainer, final ExecutionContext exec){
            // use nested loop if doesn't fit in-memory
            // read and index otherwise
        }
    }

    class InMemoryBucket {

        // these two lists store the keys and data rows paired. (don't want to create jointuple objects again; don't want to create Pair objects to hold jointuple and datarow together;         // - maybe this is premature optimization, I don't know
        // m_joinAttributes(i) stores the jointuple for m_rows(i), always retrieve both together
        final List<JoinTuple> m_joinAttributes = new ArrayList<>();
        final List<DataRow> m_rows = new ArrayList<>();

        final HashMap<JoinTuple, List<DataRow>> m_index = new HashMap<>();

        public void add(final JoinTuple joinAttributes, final DataRow row, final boolean index) {
            if (!index) {
                m_joinAttributes.add(joinAttributes);
                m_rows.add(row);
            } else {
                m_index
                    .computeIfAbsent(joinAttributes, k -> new LinkedList<DataRow>())
                    .add(row);
            }

        }

        public void joinSingleRow(final JoinTuple joinAttributeValues, final DataRow probeRow, final BufferedDataContainer resultContainer) {

            List<DataRow> matches = m_index.get(joinAttributeValues);

            if (matches == null) {
                // this row from the bigger table has no matching row in the other table
                // if we're performing an outer join, include the row in the result
                // if we're performing an inner join, ignore the row
//                unmatched.accept(m_bigger, row);
            } else {
                for (DataRow hashRow : matches) {
                    DataRow outer = getOuter(probeRow, hashRow);
                    DataRow inner = getInner(probeRow, hashRow);

                    RowKey newRowKey = concatRowKeys(outer, inner);
                    resultContainer.addRowToTable(new JoinedRow(newRowKey, outer, inner));
                }
            }
        }

//        public List<DataRow> joinMaterialize(final InMemoryBucket other) {
//            Iterator<JoinTuple> keyIterator = m_joinAttributes.iterator();
//
//            List<DataRow> result = new ArrayList<DataRow>(m_rows.size());
//
//            for (DataRow probeRow : m_rows) {
//
//                JoinTuple query = keyIterator.next();
//
//                joinSingleRow(query, probeRow, resultContainer);
//
//
//            }
//            return result;
//        }

        public void join(final InMemoryBucket other, final DataContainer resultContainer, final ExecutionContext exec) throws CanceledExecutionException {

//            try (CloseableRowIterator bigger = m_bigger.iterator()) {

            Iterator<JoinTuple> keyIterator = m_joinAttributes.iterator();

            for(DataRow probeRow : m_rows) {

                    exec.checkCanceled();

                    JoinTuple query = keyIterator.next();

                    List<DataRow> matches = other.m_index.get(query);

                    if (matches == null) {
                        // this row from the bigger table has no matching row in the other table
                        // if we're performing an outer join, include the row in the result
                        // if we're performing an inner join, ignore the row
//                        unmatched.accept(m_bigger, row);
                        continue;
                    }

//                    updateProgress(exec, m_bigger, rowIndex);

                    for (DataRow hashRow : matches) {
                        DataRow outer = getOuter(probeRow, hashRow);
                        DataRow inner = getInner(probeRow, hashRow);

                        RowKey newRowKey = concatRowKeys(outer, inner);
                        resultContainer.addRowToTable(new JoinedRow(newRowKey, outer, inner));
                    }

//                    rowIndex++;
                }
            }

            DiskBucket toDisk() {
                //FIXME
                m_rows.clear();
                m_joinAttributes.clear();
                return null;
            }

        }


    // is a closeable row iterator really the best solution?
    // we're assuming the stuff fits in memory right?
    // not sure about that...
    // it's pretty clear that going full iterator will bring some overhead...
    // for instance, if every row has two join partners, creating an iterator for two elements seems costly
    // and doesn't really save memory, right?
    // maybe add some threshold for iteration and materialization?
    // is this, again premature optimization?
    // another disadvantage of the full iterator approach is that we have to maintain a lot of state.
//            return new CloseableRowIterator() {
//
////                private Iterator<Entry<JoinTuple, List<DataRow>>> m_indexEntries = rows.entrySet().iterator();
//
//                // they go paired
//                private Iterator<DataRow> rowIterator = m_rows.iterator();
//                private Iterator<JoinTuple> keyIterator = m_joinAttributes.iterator();
//                private Iterator<DataRow> matchIterator;
//
//                // whether we're in the middle of iterating a row's matches in the index
//                //
//                private boolean midStream = false;
//
//                private Iterator<DataRow> matchIteator;
//
//                @Override
//                public DataRow next() {
//
//                    // all matches of the previous row have been processed
//                    // advance the probe input iterator
//                    if (!midStream) {
//                        JoinTuple nextKey = keyIterator.next();
//                        DataRow nextRow = rowIterator.next();
//                        List<DataRow> matches = m_index.get(nextKey);
//
//                        if (matches == null) {
//                            // this row from the bigger table has no matching row in the other table
//                            // if we're performing an outer join, include the row in the result
//                            // if we're performing an inner join, ignore the row
//                            //                        unmatched.accept(m_bigger, row);
//                            continue;
//                        }
//
//                    } else {
//                        DataRow nextMatch = matchIterator.next();
//                    }
//
//                    //                    Entry<JoinTuple, List<DataRow>> next = m_indexEntries.next();
//                    // if there is only one row with the given join attributes, return the next of its join partners
//
//
//
//                    for (DataRow match : matches) {
//                        DataRow outer = getOuter(row, match);
//                        DataRow inner = getInner(row, match);
//
//                        RowKey newRowKey = concatRowKeys(outer, inner);
//                        result.addRowToTable(new JoinedRow(newRowKey, outer, inner));
//                    }
//
//                }
//
//                @Override
//                public boolean hasNext() {
//                    return rowIterator.hasNext();
//                }
//
//                @Override
//                public void close() {
//                }
//            };


    // helper methods like in AbstractTableSorter to open and close tables for iteration?
    //Iterator<DataRow>
//    Map<BufferedDataTable, CloseableRowIterator> iterators = new HashMap<>();
//    private CloseableRowIterator open(final BufferedDataTable table) {
//        if(iterators.containsKey(table)) {
//            throw new IllegalStateException(String.format("Can not open table %s again.", table));
//        }
//        CloseableRowIterator iterator = table.iterator();
//        iterators.put(table, iterator);
//        return iterator;
//
//    }
//    private void close(final BufferedDataTable table) {
//        CloseableRowIterator iterator = iterators.remove(table);
//        if(iterator == null) {
//            throw new IllegalStateException(String.format("Can not close table %s, it has not been opened.", table));
//        }
//        iterator.close();
//    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected StreamableFunction getStreamableFunction() {
        // TODO Auto-generated method stub
        return null;
    }


}
