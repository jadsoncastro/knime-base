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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.knime.base.data.filter.column.FilterColumnRow;
import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.append.AppendedColumnRow;
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

    @Override
    public BufferedDataTable twoWayJoin(final ExecutionContext exec) throws CanceledExecutionException, InvalidSettingsException {

        return hybridHashJoin(exec, m_probe, m_hash);
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
    private BufferedDataTable hybridHashJoin(final ExecutionContext exec, final BufferedDataTable probeInput,
        final BufferedDataTable hashInput) throws CanceledExecutionException {

        // the number of disk buckets is limited by the number of rows in the hash table (for each row, at most one bucket is migrated to disk)
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

            JoinTableSettings tableSettings = m_tableSettings.get(hashInput);
            // materialize only columns that either make it into the output or are needed for joining
            try (CloseableRowIterator hashRows =
                hashInput.filter(TableFilter.materializeCols(tableSettings.materializeColumnIndices)).iterator()) {

                while (hashRows.hasNext()) {

                    progress.incProgressAndCheckCanceled(0.33/hashInput.size());

                    // if memory is running low and there are hash buckets in-memory
                    // TODO 100ms could be too long -- continuing to fill the heap could cause an error
                    boolean someBucketsAreInMemory = firstInMemoryBucket < numBuckets;
                    if (progress.isMemoryLow(m_settings.getMemoryLimitPercent(), 100) && someBucketsAreInMemory) {
                        // migrate the next in-memory hash bucket to disk
                        hashBucketsOnDisk[firstInMemoryBucket] = hashBucketsInMemory[firstInMemoryBucket].toDisk(exec);
                        firstInMemoryBucket++;
                        progress.setNumPartitionsOnDisk(firstInMemoryBucket);
                    }

                    DataRow hashRow = hashRows.next();
                    JoinTuple joinAttributeValues = tableSettings.getJoinTuple(hashRow);
                    int bucket = Math.abs(joinAttributeValues.hashCode() % numBuckets);

                    if (bucket >= firstInMemoryBucket) {
                        // if the hash bucket is in memory, add and index row
                        hashBucketsInMemory[bucket].add(joinAttributeValues, hashRow);
                    } else {
                        // if the bucket is on disk store for joining later
                        // hash rows don't need a row offset, they are already sorted.
                        hashBucketsOnDisk[bucket].add(-1, hashRow, exec);
                    }

                } // all hash input rows processed
            } // close hash input row iterator

            // hash input has been processed completely, close buckets that have been migrated to disk (ideally none)
            Arrays.stream(hashBucketsOnDisk).filter(Objects::nonNull).forEach(DiskBucket::close);

        } // phase 1: index building

        // if the hash input fits in memory, we can (stream) process the probe input in a single pass
        // if not, some rows from the probe input will be written to disk and final result will need sorting to
        // deliver a deterministic output order that does not depend on how much buckets we were able to hold in memory
        boolean needsSorting = firstInMemoryBucket > 0;

        // this table spec is either
        // - the output spec (contains outer included columns + inner included columns)
        // - sorted chunks table spec (contains probe row offset + outer included columns + inner included columns)
        DataTableSpec inMemorySpec = needsSorting ? outerSettings.sortedChunksTableWith(innerSettings) : m_outputSpec;

        BufferedDataContainer inMemoryResults = exec.createDataContainer(inMemorySpec);

        { // phase 2: partitioning and probing of the probe input

            progress.setMessage("Phase 2/3: Processing larger table");

            // process probe input: process rows immediately for which the matching bucket is in memory;
            // otherwise put the row into a disk bucket
            // materialize only columns that either make it into the output or are needed for joining

            JoinTableSettings tableSettings = m_tableSettings.get(probeInput);
            // materialize only columns that either make it into the output or are needed for joining
            try (CloseableRowIterator probeRows =
                probeInput.filter(TableFilter.materializeCols(tableSettings.materializeColumnIndices)).iterator()) {

                long rowOffset = 0;
                while (probeRows.hasNext()) {

                    progress.incProgressAndCheckCanceled(0.33/probeInput.size());

                    DataRow probeRow = probeRows.next();

                    JoinTuple joinAttributeValues = tableSettings.getJoinTuple(probeRow);
                    int bucket = Math.abs(joinAttributeValues.hashCode() % numBuckets);

                    if (bucket >= firstInMemoryBucket) {
                        // if hash bucket is in memory, output result directly
                        if(needsSorting) {
                            // output row in sortable format containing the outer row offset
                            AppendedColumnRow probeRowWithOffset = new AppendedColumnRow(probeRow, new LongCell(rowOffset));
                            hashBucketsInMemory[bucket].joinSingleRow(joinAttributeValues,
                                probeRowWithOffset, inMemoryResults,
                                this::joinUnmaterializedRowsToIntermediate);
                        } else {
                            // if all in memory, output row in output table format
                            hashBucketsInMemory[bucket].joinSingleRow(joinAttributeValues, probeRow, inMemoryResults,
                                this::joinRowsDirect);
                        }
                        progress.incProbeRowsProcessedInMemory();
                    } else {
                        // if hash bucket is on disk, process this row later, when joining the matching buckets
                        probeBuckets[bucket].add(rowOffset, probeRow, exec);
                        progress.incProbeRowsProcessedFromDisk();
                    }
                    rowOffset++;
                }
            } // all probe input rows processed

            // probe input partitioning is done; no more rows will be added to buckets
            Arrays.stream(probeBuckets).forEach(DiskBucket::close);

            inMemoryResults.close();

            // not sorting is needed if the entire probe input has been processed in memory
            if(!needsSorting) {
                return inMemoryResults.getTable();
            }
        } // phase 2

        { // phase 3: optionally join the rows that went to disk because not enough memory was available

            // since each table is sorted according to natural join order, we can do a n-way merge of the partial results
            progress.setMessage("Phase 3/3: Processing data on disk");

            // we have k + 1 (possibly empty) tables; one table for each of the k bucket pairs on disk...
            final List<BufferedDataTable> sortedBuckets = new ArrayList<BufferedDataTable>(1 + firstInMemoryBucket);
            // ... and one table from in-memory processing
            sortedBuckets.add(inMemoryResults.getTable());

            progress.setBucketSizes(probeBuckets, hashBucketsOnDisk);

            // join only the pairs of buckets that haven't been processed in memory
            for (int i = 0; i < firstInMemoryBucket; i++) {

                progress.incProgressAndCheckCanceled(0.16/firstInMemoryBucket);

                // obtain disk-based table handles for each bucket
                final Optional<BufferedDataTable> sortedChunk = probeBuckets[i].join(hashBucketsOnDisk[i],
                    this::joinWorkingRowToIntermediate, unmatchedProbeRow -> {}, unmatchedHashRow -> {}, exec);
                sortedChunk.ifPresent(sortedBuckets::add);

            }

            // n-way merge of the joined result
            // TODO make this optional, because this adds one round trip to disk: bucket pairs are read into memory, joined and buffered back to disk only
            // to be read back again and output in correct order.
            // in contrast to the classical hybrid hash join, which has worst case I/O of 3(R+S) this has 5(R+S)
            final BufferedDataContainer finale = exec.createDataContainer(m_outputSpec);
            sortRowsNatural(sortedBuckets, finale);
            finale.close();
            return finale.getTable();

        }

    }

    /**
     * @param sortedBuckets
     * @param output
     */
    private static void sortRowsNatural(final List<BufferedDataTable> sortedBuckets, final BufferedDataContainer output) {
        final DataTableSpec sortedChunkSpec = sortedBuckets.get(0).getDataTableSpec();

        // create iterators that allow looking at the next row without polling it
        // needed for sorting tables according to output row order of their first row
        @SuppressWarnings("unchecked")
        final PeekingIterator<DataRow>[] iterators = sortedBuckets.stream()
                .map(BufferedDataTable::iterator)
                .map(Iterators::peekingIterator)
                .toArray(PeekingIterator[]::new);

        ToLongFunction<PeekingIterator<DataRow>> getRowOffset = pi ->  pi.hasNext() ? ((LongCell)pi.peek().getCell(0)).getLongValue() : -1;
        Comparator<PeekingIterator<DataRow>> probeRowOffset = Comparator.comparingLong(getRowOffset);

        final int[] allButFirst = IntStream.range(1, sortedChunkSpec.getNumColumns()).toArray();

        boolean hasNext = false;
        do {
            // take rows from table with smallest probe row offset
            Arrays.sort(iterators, probeRowOffset);
            hasNext = false;
            for (int i = 0; i < iterators.length; i++) {
                if (!iterators[i].hasNext()) {
                    continue;
                }
                // take rows from current table as long as their row offsets are contiguous
                long lastProbeRowOffset = getRowOffset.applyAsLong(iterators[i]);
                long nextProbeRowOffset;
                do {
                    // add the result row, remove the offset column
                    output.addRowToTable(new FilterColumnRow(iterators[i].next(), allButFirst));
                    nextProbeRowOffset = getRowOffset.applyAsLong(iterators[i]);
                } while (nextProbeRowOffset == lastProbeRowOffset + 1);
                hasNext = hasNext | iterators[i].hasNext();
            }
        } while (hasNext);
    }


    /**
     * Joins two rows to an sorted chunk output row format
     * TODO maybe add a parameter instead of a cell to probe row
     * @param probeRow full probe row with row offset appended
     * @param hashRow full hash row
     * @return
     */
    DataRow joinUnmaterializedRowsToIntermediate(final DataRow probeRowWithOffset, final DataRow hashRow) {

        // probe row has offset appended, but is inner table
        DataRow outerRow = m_outerIsProbe ? probeRowWithOffset : hashRow;
        DataRow innerRow = m_outerIsProbe ? hashRow : probeRowWithOffset;

        // compute another working row from two working rows
        DataCell[] cells = new DataCell[1 + outerSettings.includeColumns.length + innerSettings.includeColumns.length];

        // maintain row offset from outer row
        cells[0] = probeRowWithOffset.getCell(probeRowWithOffset.getNumCells()-1);

        int cell = 1;
        for (int i = 0; i < outerSettings.includeColumns.length; i++) {
            cells[cell++] = outerRow.getCell(outerSettings.includeColumns[i]);
        }
        for (int i = 0; i < innerSettings.includeColumns.length; i++) {
            cells[cell++] = innerRow.getCell(innerSettings.includeColumns[i]);
        }

        // TODO the separator could be specified in Joiner 2
        String joinedKey = outerRow.getKey().getString().concat("_").concat(innerRow.getKey().getString());
        return new DefaultRow(joinedKey, cells);

    }

    DataRow joinWorkingRowToIntermediate(final DataRow probeRow, final DataRow hashRow) {

        DataRow outerRow = m_outerIsProbe ? probeRow : hashRow;
        DataRow innerRow = m_outerIsProbe ? hashRow : probeRow;

        // compute another working row from two working rows
        DataCell[] cells = new DataCell[1 + outerSettings.includeColumns.length + innerSettings.includeColumns.length];

        // maintain row offset from outer row
        cells[0] = probeRow.getCell(0);

        int cell = 1;
        for (int i = 0; i < outerSettings.includeColumns.length; i++) {
            cells[cell++] = outerRow.getCell(outerSettings.includeColumnsWorkingTable[i]);
        }
        for (int i = 0; i < innerSettings.includeColumns.length; i++) {
            cells[cell++] = innerRow.getCell(innerSettings.includeColumnsWorkingTable[i]);
        }

        // TODO the separator could be specified in Joiner 2
        String joinedKey = outerRow.getKey().getString().concat("_").concat(innerRow.getKey().getString());
        return new DefaultRow(joinedKey, cells);

    }

    /**
     * @param probeRow full row
     * @param hashRow full row
     * @return the output row according to m_outputspec
     */
    DataRow joinRowsDirect(final DataRow probeRow, final DataRow hashRow) {

        DataRow outerRow = m_outerIsProbe ? probeRow : hashRow;
        DataRow innerRow = m_outerIsProbe ? hashRow : probeRow;

        DataCell[] dataCells = new DataCell[m_outputSpec.getNumColumns()];
        { // fill data cells

            int nextCell = 0;

            int[] outerIncludes = m_tableSettings.get(m_outer).includeColumns;
            for (int i = 0; i < outerIncludes.length; i++) {
                dataCells[nextCell++] = outerRow.getCell(outerIncludes[i]);
            }

            int[] innerIncludes = m_tableSettings.get(m_inner).includeColumns;
            for (int i = 0; i < innerIncludes.length; i++) {
                dataCells[nextCell++] = innerRow.getCell(innerIncludes[i]);
            }

        }

        // about the same speed
//        String newRowKey = outerRow.getKey().getString().concat("_").concat(innerRow.getKey().getString());
        return new DefaultRow(concatRowKeys(outerRow, innerRow), dataCells);
    }

    class DiskBucket {

        final JoinTableSettings joinTable;

        /**
         * Buffer for flushing working rows to disk. Is null until the first row is added.
         */
        BufferedDataContainer m_container = null;

        /**
         * Is created in {@link #close()} if at least one row has been added. The table contains a subset of the rows of
         * the table this bucket is created for {@link JoinTableSettings#m_forTable}.</br>
         * A disk buckets is used in phase 3 to read the rows that have been written to disk in phase 2 back into
         * memory.
         */
        Optional<BufferedDataTable> m_tablePartition = Optional.empty();

        DiskBucket(final BufferedDataTable forTable) {
            joinTable = m_tableSettings.get(forTable);
        }

        /**
         * This is called in phase 2 to flush rows to disk that do not fit into memory.
         * @param rowOffset
         * @param row
         * @param exec
         */
        void add(final long rowOffset, final DataRow row, final ExecutionContext exec) {
            // lazy initialization of the data container, only when rows are actually added
            if (m_container == null) {
                m_container = exec.createDataContainer(joinTable.workingTableSpec);
            }
            m_container.addRowToTable(joinTable.workingRow(rowOffset, row));
        }

        /**
         * Called at the end of phase 2, after the last row has been added to the bucket.
         */
        void close() {
            if(m_container != null) {
                m_container.close();
                m_tablePartition = Optional.of(m_container.getTable());
                m_container = null;
            }
        }

        /**
         * Called during phase 3, to reads the working rows back from disk and outputs partial results in sorted chunk
         * row format.
         *
         * @param other
         * @param matches
         * @param unmatchedProbeRows
         * @param unmatchedHashRows
         * @param exec
         * @return
         */
        Optional<BufferedDataTable> join(final DiskBucket other, final BiFunction<DataRow, DataRow, DataRow> matches,
            final Consumer<DataRow> unmatchedProbeRows, final Consumer<DataRow> unmatchedHashRows,
            final ExecutionContext exec) {

            // if either bucket is empty, we're done joining those buckets.
            if(! m_tablePartition.isPresent() && ! other.m_tablePartition.isPresent()) {
                return Optional.empty();
            }

            BufferedDataTable probeBucket = m_tablePartition.get();
            BufferedDataTable hashBucket = other.m_tablePartition.get();

            // TODO nested loop fallback

            JoinTableSettings otherTable = other.joinTable;

            // TODO close this iterator?
            Map<JoinTuple, List<DataRow>> index = StreamSupport.stream(hashBucket.spliterator(), false)
                .collect(Collectors.groupingBy(otherTable::getJoinTupleWorkingRow));

            // factor out to drop-in replace with something that scales to long
//            BitSet matchedHashRows = new BitSet(hashBucket.getRowCount());

            DataTableSpec joinedBucketsSpec = outerSettings.sortedChunksTableWith(innerSettings);

            BufferedDataContainer result = exec.createDataContainer(joinedBucketsSpec);

            try (CloseableRowIterator probeRows = probeBucket.iterator()) {

                while (probeRows.hasNext()) {

                    exec.checkCanceled();

                    DataRow probeRow = probeRows.next();
                    JoinTuple probeTuple = joinTable.getJoinTupleWorkingRow(probeRow);

                    List<DataRow> matching = index.get(probeTuple);

                    if (matching == null) {
                        // this row from the bigger table has no matching row in the other table
                        // if we're performing an outer join, include the row in the result
                        // if we're performing an inner join, ignore the row
                        //                unmatched.accept(m_bigger, row);
                        unmatchedProbeRows.accept(probeRow);
                    } else {
                        for (DataRow hashRow : matching) {
//
                            result.addRowToTable(matches.apply(probeRow, hashRow));
                        }
                    }

                }
            } catch (CanceledExecutionException e) {

            }

            result.close();
            return Optional.of(result.getTable());

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

        List<DataRow> m_rows = new ArrayList<>();
        // TODO this should be an intarraylist
//        List<Long> m_rowOffsets = new ArrayList<>();

        HashMap<JoinTuple, List<DataRow>> m_index = new HashMap<>();

        InMemoryBucket(final BufferedDataTable forTable) {
            m_forTable = forTable;
        }

        public void add(final JoinTuple joinAttributes, final DataRow row) {
            m_index.computeIfAbsent(joinAttributes, k -> new LinkedList<DataRow>()).add(row);
//            m_rowOffsets.add(rowOffset);
            m_rows.add(row);
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
                    DataRow joinedRow = joinRows.apply(probeRow, hashRow);
                    resultContainer.addRowToTable(joinedRow);
                }
            }
        }

        /**
         * Only hash buckets can go to disk, probe rows are put into DiskBuckets directly
         * @param exec
         * @return
         */
        DiskBucket toDisk(final ExecutionContext exec) {

            // release memory
            m_index = null;

            DiskBucket result = new DiskBucket(m_forTable);

            // this should be an IntIterator
//            Iterator<Long> rowOffsets = m_rowOffsets.iterator();

            for (Iterator<DataRow> rows = m_rows.iterator(); rows.hasNext();) {
                result.add(-1, rows.next(), exec);
                // free memory from the in-memory data structure
                rows.remove();
//                rowOffsets.remove();
            }

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

    @FunctionalInterface private interface CancelHandler {
        void handle();
    }

    class ProgressMonitor implements HybridHashJoinMXBean{

        CancelHandler m_cancelHandler;

        NodeProgressMonitor m_monitor;
        final ExecutionContext m_exec;

        /**
         * Earliest time to actually check memory utilization in {@link #isMemoryLow(int, long)}
         */
        long checkBackAfter = 0;

        boolean m_recommendedUsingMoreMemory = false;

        /**
         * For testing only: if true, triggers flushing to disk behavior in the hybrid hash join
         */
        boolean assumeMemoryLow = false;

        // For bean inspection only
        int numBuckets;

        int numHashPartitionsOnDisk = 0;
        long probeRowsProcessedInMemory = 0, probeRowsProcessedFromDisk = 0;

        long[] probeBucketSizes, hashBucketSizes;
        double probeBucketSizeAverage, hashBucketSizeAverage;
        double probeBucketSizeCoV, hashBucketSizeCoV;

        public ProgressMonitor(final ExecutionContext exec, final CancelHandler handler) {

            m_cancelHandler = handler;
            m_monitor = exec.getProgressMonitor();
            m_exec = exec;

            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            try {
                ObjectName name = new ObjectName("org.knime.base.node.preproc.joiner3.jmx:type=HybridHashJoin");
                try { server.unregisterMBean(name); } catch(InstanceNotFoundException e) {}
                server.registerMBean(this, name);
            } catch (Exception e) { System.err.println(e.getMessage()); }
        }

        /**
         * Polling the state of the memory system is an expensive operation and should be performed only every 100 rows or so.
         * @param memoryLimitPercent the threshold to check against
         * @param checkBackAfter
         * @return
         */
        public boolean isMemoryLow(final int memoryLimitPercent, final long checkBackInMs) {
            if (assumeMemoryLow || System.currentTimeMillis() >= checkBackAfter) {
                checkBackAfter = System.currentTimeMillis() + checkBackInMs;
                return assumeMemoryLow || MemoryAlertSystem.getUsage() > memoryLimitPercent / 100.;
            }
            return false | assumeMemoryLow;
        }

        public void reset() {
            m_recommendedUsingMoreMemory = false;
            m_monitor.setProgress(0);

            numHashPartitionsOnDisk = 0;
            probeRowsProcessedInMemory = 0;
            probeRowsProcessedFromDisk = 0;
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

        public void setNumPartitionsOnDisk(final int n) {
            if (!m_recommendedUsingMoreMemory && n > 0) {
                LOGGER.warn(String.format(
                    "Run KNIME with more main memory to speed up the join. The smaller table is too large to execute the join in memory. "));
                m_recommendedUsingMoreMemory = true;
            }
            numHashPartitionsOnDisk = n;
        }

        /**
         * Compute bucket size statistics to see how well the hash function distributes groups to buckets.
         * @param probeBuckets
         * @param hashBucketsOnDisk
         */
        public void setBucketSizes(final DiskBucket[] probeBuckets, final DiskBucket[] hashBucketsOnDisk) {

            { // probeBucketSizes
                probeBucketSizes = Arrays.stream(probeBuckets)
                    .mapToLong(b -> b.m_tablePartition.map(BufferedDataTable::size).orElse(0L)).toArray();
                DescriptiveStatistics stats =
                    new DescriptiveStatistics(Arrays.stream(probeBucketSizes).mapToDouble(l -> l).toArray());
                probeBucketSizeAverage = stats.getMean();
                probeBucketSizeCoV = stats.getStandardDeviation() / stats.getMean();
            }
            { // hashBucketSizes
                hashBucketSizes = Arrays.stream(hashBucketsOnDisk)
                    .mapToLong(b -> b.m_tablePartition.map(BufferedDataTable::size).orElse(0L)).toArray();
                DescriptiveStatistics stats =
                    new DescriptiveStatistics(Arrays.stream(hashBucketSizes).mapToDouble(l -> l).toArray());
                hashBucketSizeAverage = stats.getMean();
                hashBucketSizeCoV = stats.getStandardDeviation() / stats.getMean();
            }
        }

        public void setMessage(final String message) { m_monitor.setMessage(message); }

        public void incProbeRowsProcessedInMemory() { probeRowsProcessedInMemory++; }
        public void incProbeRowsProcessedFromDisk() { probeRowsProcessedFromDisk++; }

        @Override public int getNumBuckets() { return numBuckets; }
        @Override public int getNumPartitionsOnDisk() { return numHashPartitionsOnDisk; }
        @Override public long[] getProbeBucketSizes() { return probeBucketSizes; }
        @Override public long[] getHashBucketSizes() { return hashBucketSizes; };
        @Override public double getAverageProbeBucketSize() { return probeBucketSizeAverage; };
        @Override public double getAverageHashBucketSize() { return hashBucketSizeAverage; };
        @Override public double getProbeBucketSizeCoV() { return probeBucketSizeCoV; };
        @Override public double getHashBucketSizeCoV() { return hashBucketSizeCoV; };

        @Override public long getProbeRowsProcessedInMemory() { return probeRowsProcessedInMemory; }
        @Override public long getProbeRowsProcessedFromDisk() { return probeRowsProcessedFromDisk; }

        @Override public void setAssumeMemoryLow(final boolean assume) { assumeMemoryLow = assume; }

    }

    public static class FillMemoryForTesting implements FillMemoryForTestingMXBean {

        static {
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            try {
                ObjectName name = new ObjectName("org.knime.base.node.preproc.joiner3.jmx:type=MemoryFiller");
                try { server.unregisterMBean(name); } catch(InstanceNotFoundException e) {}
                server.registerMBean(new FillMemoryForTesting(), name);
            } catch (Exception e) { System.err.println(e.getMessage()); }
        }

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

    public static interface FillMemoryForTestingMXBean{
        void fillHeap(float targetPercentage);
        void releaseTestAllocations();
    }

    public interface HybridHashJoinMXBean {
        int getNumPartitionsOnDisk();
        long getProbeRowsProcessedInMemory();
        long getProbeRowsProcessedFromDisk();
        int getNumBuckets();
        long[] getProbeBucketSizes();
        long[] getHashBucketSizes();
        double getAverageProbeBucketSize();
        double getAverageHashBucketSize();
        double getProbeBucketSizeCoV();
        double getHashBucketSizeCoV();
        void setAssumeMemoryLow(boolean assume);
    }

}
