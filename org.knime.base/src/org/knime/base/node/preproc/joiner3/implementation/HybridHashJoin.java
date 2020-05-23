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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeProgressMonitor;
import org.knime.core.node.streamable.StreamableFunction;

import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;

/**
 *
 * Classic hybrid hash join, as described for instance in [1]. Degrades gracefully from in-memory hash join to
 * disk-based hash join depending on how much main memory is available by partitioning input tables and flushing only
 * those partitions to disk that do not fit into memory. <br/>
 *
 * The algorithm proceeds in three phases:
 * <ol>
 * <li>Single pass over the smaller table (hash input), which is partitioned and indexed in memory. If memory runs low,
 * partitions are flushed to disk. See {@link InMemoryBucket#toDisk(ExecutionContext)}.</li>
 * <li>Single pass over the bigger table (probe input). Rows that hash to a partition whose counterpart is held in
 * memory are processed directly. Other rows are flushed to disk.</li>
 * <li>Load and join matching partitions that have been flushed to disk.</li>
 * </ol>
 *
 * [1] Garcia-Molina, Hector, Jeffrey D. Ullman, and Jennifer Widom. Database system implementation.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 */
// TODO remove to check serious ones
@SuppressWarnings("javadoc")
public class HybridHashJoin extends JoinImplementation {

    ProgressMonitor progress;

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

        int numBuckets = m_settings.getMaxOpenFiles();

        // The row partitions of the hash input. Try to keep in memory, but put on disk if needed.
        // hashBucketsInMemory[i] == null indicates that the bucket is on disk.
        InMemoryBucket[] hashBucketsInMemory =
            Stream.generate(() -> new InMemoryBucket(hashInput)).limit(numBuckets).toArray(InMemoryBucket[]::new);

        // the buckets of the hash input are kept in memory for offsets firstInMemoryBucket...numBuckets-1
        // the offsets 0...firstInMemoryBucket-1 are stored on disk.
        int firstInMemoryBucket = 0;

        // The row partitions of the hash input that have been migrated to disk due to low heap space.
        // Are initialized as soon as migration to disk is necessary.
        DiskBucket[] hashBucketsOnDisk = new DiskBucket[numBuckets];

        // Row partitions of the probe input. A bucket r is only used if the corresponding hash input bucket s can
        // not be held in main memory. The bucket pair (r, s) is then retrieved from disk later and joined in memory.
        DiskBucket[] probeBuckets =
            Stream.generate(() -> new DiskBucket(probeInput)).limit(numBuckets).toArray(DiskBucket[]::new);

        // if the execution is canceled, discard all disk buckets (release data containers)
        CancelHandler closeDiskBuckets = () -> {
            Arrays.stream(hashBucketsOnDisk).filter(Objects::nonNull).forEach(DiskBucket::discard);
            Arrays.stream(probeBuckets).forEach(DiskBucket::discard);
        };

        progress = progress == null ? new ProgressMonitor(exec, closeDiskBuckets)  : progress;
        progress.numBuckets = numBuckets;

        { // phase 1: partition the hash input into buckets, keep as many buckets as possible in memory

            progress.setMessage("Phase 1/3: Processing smaller table");
            progress.reset();

            TableSettings tableSettings = m_tableSettings.get(hashInput);
            // materialize only columns that either make it into the output or are needed for joining
            int[] workingColumns = tableSettings.workingTableColumnIndices;
            try (CloseableRowIterator hashRows = hashInput.filter(TableFilter.materializeCols(workingColumns)).iterator()) {

                long rowOffset = 0;
                while (hashRows.hasNext()) {

                    progress.incProgressAndCheckCanceled(0.33/hashInput.size());

                    // if memory is running low and there are hash buckets in-memory
                    // TODO 300ms could be too long -- continuing to fill the heap could cause an error
                    boolean someBucketsAreInMemory = firstInMemoryBucket < numBuckets;
                    if (progress.isMemoryLow(m_settings.getMemoryLimitPercent(), 300) && someBucketsAreInMemory) {
                        // migrate the next in-memory hash bucket to disk
                        hashBucketsOnDisk[firstInMemoryBucket] = hashBucketsInMemory[firstInMemoryBucket].toDisk(exec);
                        firstInMemoryBucket++;
                        progress.setNumPartitionsOnDisk(firstInMemoryBucket);
                    }

                    DataRow hashRow = tableSettings.workingRow(rowOffset, hashRows.next());
                    JoinTuple joinAttributeValues = tableSettings.getJoinTuple(hashRow);
                    int bucket = Math.abs(joinAttributeValues.hashCode() % numBuckets);

                    if (bucket >= firstInMemoryBucket) {
                        // if the hash bucket is in memory, add and index row
                        hashBucketsInMemory[bucket].add(joinAttributeValues, hashRow, true);
                    } else {
                        // if the bucket is on disk store for joining later
                        hashBucketsOnDisk[bucket].add(hashRow, exec);
                    }

                    rowOffset++;
                }
            }

            // hash input has been processed completely, close buckets that have been migrated to disk (hopefully none)
            Arrays.stream(hashBucketsOnDisk).filter(Objects::nonNull).forEach(DiskBucket::close);

        }

        BufferedDataContainer inMemoryResults = exec.createDataContainer(m_outputSpec);
//        DataContainer results = new DataContainer(m_outputSpec, DataContainerSettings.getDefault().withForceSequentialRowHandling(true));

        { // phase 2: partitioning and probing of the probe input

            progress.setMessage("Phase 2/3: Processing larger table");

            // process probe input: process rows immediately for which the matching bucket is in memory;
            // otherwise put the row into a disk bucket
            // materialize only columns that either make it into the output or are needed for joining

            TableSettings tableSettings = m_tableSettings.get(probeInput);
            // materialize only columns that either make it into the output or are needed for joining
            int[] workingColumns = tableSettings.workingTableColumnIndices;
            try (CloseableRowIterator probeRows = probeInput.filter(TableFilter.materializeCols(workingColumns)).iterator()) {

                long rowOffset = 0;
                while (probeRows.hasNext()) {

                    progress.incProgressAndCheckCanceled(0.33/probeInput.size());

                    DataRow workingRow = tableSettings.workingRow(rowOffset, probeRows.next());

                    JoinTuple joinAttributeValues = tableSettings.getJoinTuple(workingRow);
                    int bucket = Math.abs(joinAttributeValues.hashCode() % numBuckets);

                    if (bucket >= firstInMemoryBucket) {
                        // if hash bucket is in memory, output result directly
                        hashBucketsInMemory[bucket].joinSingleRow(joinAttributeValues, workingRow, inMemoryResults, this::joinRows);
                        progress.incProbeRowsProcessedInMemory();
                    } else {
                        // if hash bucket is on disk, process this row later, when joining the matching buckets
                        probeBuckets[bucket].add(workingRow, exec);
                        progress.incProbeRowsProcessedFromDisk();
                    }
                    rowOffset++;
                }
            }

            // probe input partitioning is done; no more rows will be added to buckets
            Arrays.stream(probeBuckets).forEach(DiskBucket::close);

            inMemoryResults.close();

            // If the entire probe input has been processed in memory, we're done.
            boolean allInMemory = firstInMemoryBucket == 0;
            if(allInMemory) {
                return inMemoryResults.getTable();
            }
        }

        { // phase 3: optionally join the rows that went to disk because not enough memory was available

            progress.setMessage("Phase 3/3: Processing data on disk");

            BufferedDataContainer sortedOutput = exec.createDataContainer(m_outputSpec);

            // we have k + 1 tables; one table for each of the k bucket pairs on disk + one (possibly empty) table from in-memory processing
            // since each table is sorted according to natural join order, we can do a k+1-way merge of the partial results.

            BufferedDataTable[] sortedBuckets = new BufferedDataTable[firstInMemoryBucket + 1];

            // join only the pairs of buckets that haven't been processed in memory
            for (int i = 0; i < firstInMemoryBucket; i++) {

                progress.incProgressAndCheckCanceled(0.33/firstInMemoryBucket);

                // obtain disk-based table handles for each bucket
                sortedBuckets[i] = probeBuckets[i].join(hashBucketsOnDisk[i], this::joinRows, unmatchedProbeRow -> {}, unmatchedHashRow -> {}, exec);

            }

            sortedBuckets[firstInMemoryBucket] = inMemoryResults.getTable();

            SortMerge.merge(Comparator.comparingLong(row -> ((LongCell)row.getCell(0)).getLongValue()), sortedBuckets);

            sortedOutput.close();
            return sortedOutput.getTable();
        }

    }

    static class SortMerge {

        static BufferedDataTable merge(final Comparator<WorkingRow> comparator, final BufferedDataTable[] sortedBuckets) {

            // row offsets are contiguous, it's always clear which comes next
            long nextRowOffset = 0;

            // cycle over iterators
            int nextIterator = 0;


            PeekingIterator<WorkingRow>[] iterators = //new PeekingIterator[] {Iterators.peekingIterator(sortedBuckets[0].iterator())};
            Arrays.stream(sortedBuckets).map(BufferedDataTable::iterator).map(cri -> Iterators.peekingIterator(cri)).toArray(PeekingIterator[]::new);

//            PeekingIterator<DataRow>[] iterators = Arrays.stream(sortedBuckets)
//                .map(BufferedDataTable::iterator).map(Iterators::<DataRow>peekingIterator).toArray(PeekingIterator[]::new);

            BitSet iteratorsWithNext = new BitSet(iterators.length);
            IntStream.range(0, iterators.length).filter(i -> iterators[i].hasNext())
                .forEach(i -> iteratorsWithNext.set(i));

            while (!iteratorsWithNext.isEmpty()) {

                // rotate iterators until next row offset is found
                int iteratorOffset = 0;
                PeekingIterator<WorkingRow> it;
                do {
                    int next = iteratorsWithNext.nextSetBit(iteratorOffset);
                    it = iterators[next];
                    iteratorOffset = next;
                } while (it.peek().getOuterRowIndex() != nextRowOffset);

            }
            return null;
        }
    }

//
//    class PeekIterator implements AutoCloseable {
//
//        CloseableRowIterator iterator;
//        DataRow next;
//
//        /**
//         * must only be called if hasNext is true
//         * @return the element that's up next in the iterable
//         */
//        DataRow peek() {
//            if(next == null) {
//                next = iterator.next();
//            }
//            return next;
//        }
//        PeekingIterator<E>
//        boolean hasNext() {
//            return iterator.hasNext();
//        }
//        DataRow next() {
//            DataRow result = next;
//            next = null;
//            return next;
//        }
//
//
//        @Override
//        public void close() throws Exception {
//            // TODO Auto-generated method stub
//
//        }
//
//    }


    /**
     * @param probeRow
     * @param hashRow
     * @return the output row containing the included and from two matching rows from a working table.
     */
    DataRow joinRows(final DataRow probeRow, final DataRow hashRow) {

        DataRow leftRow = getLeft(probeRow, hashRow);
        DataRow rightRow = getRight(probeRow, hashRow);

        DataCell[] dataCells = new DataCell[m_outputSpec.getNumColumns()];
        { // fill data cells

            int nextCell = 0;

            int[] leftOutputIndices = m_tableSettings.get(m_leftTable).workingTableIncludeColumnIndices;
            for (int j = 0; j < leftOutputIndices.length; j++) {
                dataCells[nextCell++] = leftRow.getCell(leftOutputIndices[j]);
            }

            int[] rightOutputIndices = m_tableSettings.get(m_rightTable).workingTableIncludeColumnIndices;
            for (int j = 0; j < rightOutputIndices.length; j++) {
                dataCells[nextCell++] = rightRow.getCell(rightOutputIndices[j]);
            }

        }

        RowKey newRowKey = concatRowKeys(leftRow, rightRow);

        return new DefaultRow(newRowKey, dataCells);
    }

    class DiskBucket {
        BufferedDataTable rows;

        // TODO this should only write the columns to disk that are used not projected out in the end
        BufferedDataContainer m_container;

        final TableSettings tableSettings;

        // stores a part of m_forTable on disk.
        BufferedDataTable m_tablePartition;

        DiskBucket(final BufferedDataTable forTable) {
            tableSettings = m_tableSettings.get(forTable);
        }

        void add(final DataRow row, final ExecutionContext exec) {
            // lazy initialization of the data container, only when rows are actually added
            if (m_container == null) {
                m_container = exec.createDataContainer(tableSettings.workingTableSpec);
            }
            m_container.addRowToTable(row);
        }

        BufferedDataTable join(final DiskBucket other, final BiFunction<DataRow, DataRow, DataRow> matches,
            final Consumer<DataRow> unmatchedProbeRows, final Consumer<DataRow> unmatchedHashRows,
            final ExecutionContext exec) {
            // use nested loop if doesn't fit in-memory
            // read and index otherwise
            // TODO refactor
            //           NestedHashJoin.join(m_settings, m_tablePartition, other.m_tablePartition, resultContainer);

            BufferedDataTable leftPartition = m_tablePartition;
            BufferedDataTable rightPartition = other.m_tablePartition;

            // what's in the bucket could be too big to process in memory.
            // however, we should try.
            // nested hash join should degrade gracefully.

            TableSettings otherSettings = m_tableSettings.get(other.tableSettings.m_forTable);

            // TODO close this iterator?
            Map<JoinTuple, List<DataRow>> index = StreamSupport.stream(rightPartition.spliterator(), false)
                .collect(Collectors.groupingBy(row -> otherSettings.getJoinTuple(row)));

            // factor out to drop-in replace with something that scales to long
            BitSet matchedHashRows = new BitSet(rightPartition.getRowCount());

            TableSettings tsettings = m_tableSettings.get(tableSettings.m_forTable);

            BufferedDataContainer result = exec.createDataContainer(m_outputSpec);

            try (CloseableRowIterator probeRows = leftPartition.iterator()) {

                while (probeRows.hasNext()) {

                    exec.checkCanceled();

                    DataRow workingRow = probeRows.next();
                    JoinTuple probeTuple = tsettings.getJoinTuple(workingRow);

                    List<DataRow> matching = index.get(probeTuple);

                    if (matching == null) {
                        // this row from the bigger table has no matching row in the other table
                        // if we're performing an outer join, include the row in the result
                        // if we're performing an inner join, ignore the row
                        //                unmatched.accept(m_bigger, row);
                        unmatchedProbeRows.accept(workingRow);
                    } else {
                        for (DataRow hashRow : matching) {
//                            matchedHashRows.set(hashRow.offset);
                            result.addRowToTable(matches.apply(workingRow, hashRow));
                        }
                    }

                }
            } catch (CanceledExecutionException e) {

            }

            result.close();
            return result.getTable();

        }

        /**
         * Called after the last row has been added to the bucket. Converts to a {@link BufferedDataTable} internally.
         */
        void close() {
            if(m_container != null) {
                m_container.close();
                m_tablePartition = m_container.getTable();
                m_container = null;
            }
        }

        /**
         * Called when the execution is canceled. Releases open containers and/or tables.
         */
        void discard() {
            if(m_tablePartition != null) {
                // if the table has already been created (this bucket was closed earlier), discard table
                m_tablePartition = null;
            } else {
                // otherwise close container
                m_container.close();
                m_container = null;
            }

        }

    }

    class InMemoryBucket {

        final BufferedDataTable m_forTable;

        // these two lists store the keys and data rows paired. (don't want to create jointuple objects again; don't want to create Pair objects to hold jointuple and datarow together;         // - maybe this is premature optimization, I don't know
        // m_joinAttributes(i) stores the jointuple for m_rows(i), always retrieve both together
        List<JoinTuple> m_joinAttributes = new ArrayList<>();

        List<DataRow> m_rows = new ArrayList<>();

        HashMap<JoinTuple, List<DataRow>> m_index = new HashMap<>();

        InMemoryBucket(final BufferedDataTable forTable) {
            m_forTable = forTable;
        }

        public void add(final JoinTuple joinAttributes, final DataRow row, final boolean index) {
            if (!index) {
                m_joinAttributes.add(joinAttributes);
                m_rows.add(row);
            } else {
                m_index.computeIfAbsent(joinAttributes, k -> new LinkedList<DataRow>()).add(row);
            }

        }

        public void joinSingleRow(final JoinTuple joinAttributeValues, final DataRow probeRow,
            final DataContainer resultContainer, final BiFunction<DataRow, DataRow, DataRow> joinRows) {

            List<DataRow> matching = m_index.get(joinAttributeValues);

            if (matching == null) {
                // this row from the bigger table has no matching row in the other table
                // if we're performing an outer join, include the row in the result
                // if we're performing an inner join, ignore the row
                //                unmatched.accept(m_bigger, row);
                // TODO outer join
            } else {
                for (DataRow hashRow : matching) {
                    resultContainer.addRowToTable(joinRows.apply(probeRow, hashRow));
                }
            }
        }

        DiskBucket toDisk(final ExecutionContext exec) {

            // release memory
            m_joinAttributes = null;
            m_index = null;

            DiskBucket result = new DiskBucket(m_forTable);

            for (Iterator<DataRow> iterator = m_rows.iterator(); iterator.hasNext();) {

                DataRow row = iterator.next();
                result.add(row, exec);
                // free memory from the in-memory data structure
                iterator.remove();
            }

            // release memory
            m_rows = null;

            return result;
        }

    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected StreamableFunction getStreamableFunction() {
        // TODO Auto-generated method stub
        return null;
    }

    private interface CancelHandler {
        void handle();
    }

    class ProgressMonitor{

        CancelHandler m_cancelHandler;

        NodeProgressMonitor m_monitor;
        final HybridHashJoinMBean m_bean = new HybridHashJoinMBean(this);
        final ExecutionContext m_exec;

        int numHashPartitionsOnDisk = 0;
        long probeRowsProcessedInMemory = 0;
        long probeRowsProcessedFromDisk = 0;
        int numBuckets;
        boolean m_recommendedUsingMoreMemory = false;

        public ProgressMonitor(final ExecutionContext exec, final CancelHandler handler) {

            m_cancelHandler = handler;
            m_monitor = exec.getProgressMonitor();
            m_exec = exec;

            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            try {
                ObjectName name = new ObjectName("org.knime.base.node.preproc.joiner3.jmx:type=HybridHashJoin");
                try { server.unregisterMBean(name); } catch(InstanceNotFoundException e) {}
                server.registerMBean(m_bean, name);
            } catch (Exception e) { System.err.println(e.getMessage()); }
        }


        /**
         * Earliest time to actually check memory utilization in {@link #isMemoryLow(int, long)}
         */
        long checkBackAfter = 0;

        /**
         * For testing only: if true, triggers flushing to disk behavior in the hybrid hash join
         */
        boolean assumeMemoryLow = false;

        /**
         * Polling the state of the memory system is an expensive operation and should be performed only every 100 rows or so.
         * @param memoryLimitPercent the threshold to check against
         * @param checkBackAfter
         * @return
         */
        public boolean isMemoryLow(final int memoryLimitPercent, final long checkBackInMs) {
            if (assumeMemoryLow || System.currentTimeMillis() >= checkBackAfter) {
                checkBackAfter = System.currentTimeMillis() + checkBackInMs;
                return MemoryAlertSystem.getUsage() > memoryLimitPercent / 100.;
            }
            return false;
        }


        public void reset() {
            m_monitor.setProgress(0);

            numHashPartitionsOnDisk = 0;
            probeRowsProcessedInMemory = 0;
            probeRowsProcessedFromDisk = 0;
        }

        public void incProbeRowsProcessedInMemory() { probeRowsProcessedInMemory++; }
        public void incProbeRowsProcessedFromDisk() { probeRowsProcessedFromDisk++; }

        public void setNumPartitionsOnDisk(final int n) {
            if (!m_recommendedUsingMoreMemory && n > 0) {
                LOGGER.warn(String.format(
                    "The smaller table is too large to execute the join in memory. Run KNIME with more main memory to speed up the join."));
                m_recommendedUsingMoreMemory = true;
            }
            numHashPartitionsOnDisk = n;
        }

        private void checkCanceled() throws CanceledExecutionException {
            try {
                m_exec.checkCanceled();
            } catch (CanceledExecutionException e) {
                m_cancelHandler.handle();
                throw e;
            }
        }

        public void incProgressAndCheckCanceled(final double d) throws CanceledExecutionException {
            checkCanceled();
            m_monitor.setProgress(m_monitor.getProgress() + d);
        }

        public void setMessage(final String message) { m_monitor.setMessage(message); }

    }

    public static interface FillMemoryForTestingMXBean{
        void fillHeap(float targetPercentage);
        void releaseTestAllocations();
    }
    public static class FillMemoryForTesting implements FillMemoryForTestingMXBean {

        List<double[]> memoryConsumer = new LinkedList<>();

        @Override
        public void fillHeap(final float targetPercentage) {
            while (MemoryAlertSystem.getUsage() < targetPercentage) {
                // allocate 50 MB
                memoryConsumer.add(new double[6_250_000]);
            }
        }

        @Override
        public void releaseTestAllocations() {
            memoryConsumer.clear();
        }
    }

    static {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName("org.knime.base.node.preproc.joiner3.jmx:type=MemoryFiller");
            try { server.unregisterMBean(name); } catch(InstanceNotFoundException e) {}
            server.registerMBean(new FillMemoryForTesting(), name);
        } catch (Exception e) { System.err.println(e.getMessage()); }
    }

    public interface IHybridHashJoinMXBean {

        int getNumPartitionsOnDisk();
        long getProbeRowsProcessedInMemory();
        long getProbeRowsProcessedFromDisk();
        int getNumBuckets();

//        void toggleAssumeMemoryIsLow();
//        boolean getAssumeMemoryIsLow();
    }

    public class HybridHashJoinMBean implements IHybridHashJoinMXBean {

        MemoryAlertSystem memoryAlertSystem = MemoryAlertSystem.getInstanceUncollected();

        ProgressMonitor monitor;

        public HybridHashJoinMBean(final ProgressMonitor progressMonitor) {
            monitor = progressMonitor;
        }

        @Override public int getNumBuckets() { return monitor.numBuckets; }
        @Override public int getNumPartitionsOnDisk() { return monitor.numHashPartitionsOnDisk; }

        @Override public long getProbeRowsProcessedInMemory() { return monitor.probeRowsProcessedInMemory; }
        @Override public long getProbeRowsProcessedFromDisk() { return monitor.probeRowsProcessedFromDisk; }

//        @Override public void toggleAssumeMemoryIsLow() { monitor.assumeMemoryLow  = !monitor.assumeMemoryLow; }
//        @Override public boolean getAssumeMemoryIsLow() { return monitor.assumeMemoryLow; }
    }

}
