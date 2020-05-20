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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.knime.base.data.filter.column.FilterColumnRow;
import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.container.filter.TableFilter;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeProgressMonitor;
import org.knime.core.node.streamable.StreamableFunction;

/**
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
     * Drop columns that don't make it into the output early to save heap space and I/O when forced to flush rows to
     * disk.
     *
     * @param forTable refers to the hash input table (or one of multiple hash input tables) or the probe input table
     * @return The spec of the given table filtered to included and join columns.
     */
    DataTableSpec workingTableSpec(final BufferedDataTable forTable) {
        // only read the columns needed to perform the join (some of the join columns may be filtered out in the end)

        List<String> includeColumns = Arrays.asList(m_tableSettings.get(forTable).includeColumnNames);
        List<String> joinColumns = Arrays.asList(m_tableSettings.get(forTable).joinColumnNames);
        // retain only columns that will make it into the result or on which we join
        DataColumnSpec[] colSpecs = forTable.getDataTableSpec().stream()
            .filter(column -> includeColumns.contains(column.getName()) || joinColumns.contains(column.getName()))
            .toArray(DataColumnSpec[]::new);
        return new DataTableSpec(colSpecs);
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

            // materialize only columns that either make it into the output or are needed for joining
            int[] workingColumns = hashInput.getDataTableSpec().columnsToIndices(workingTableSpec(hashInput).getColumnNames());
            int[] joinColumns = workingTableSpec(hashInput).columnsToIndices(m_tableSettings.get(hashInput).joinColumnNames);
            try (CloseableRowIterator hashRows = hashInput.filter(TableFilter.materializeCols(workingColumns)).iterator()) {

                while (hashRows.hasNext()) {

                    progress.incProgressAndCheckCanceled(0.33/hashInput.size());

                    // if memory is running low and there are hash buckets in-memory
                    // TODO check memory again only after N rows
                    // or check only every 100 rows or so.

                    boolean someBucketsAreInMemory = firstInMemoryBucket < numBuckets;

                    if (progress.isMemoryLow(m_settings.getMemoryLimitPercent(), 10000) && someBucketsAreInMemory) {
                        // migrate the next in-memory hash bucket to disk
                        // convert to disk bucket, release heap memory
                        hashBucketsOnDisk[firstInMemoryBucket] = hashBucketsInMemory[firstInMemoryBucket].toDisk(exec);
                        firstInMemoryBucket++;
                        progress.setNumPartitionsOnDisk(firstInMemoryBucket);
                    }

                    DataRow hashRow = new FilterColumnRow(hashRows.next(), workingColumns);
                    JoinTuple joinAttributeValues = getJoinTuple(joinColumns, hashRow);
                    int bucket = Math.abs(joinAttributeValues.hashCode() % numBuckets);

                    if (bucket >= firstInMemoryBucket) {
                        // if the hash bucket is in memory, add and index row
                        hashBucketsInMemory[bucket].add(joinAttributeValues, hashRow, true);
                    } else {
                        // if the bucket is on disk store for joining later
                        hashBucketsOnDisk[bucket].add(hashRow, exec);
                    }

                }
            }

            // hash input has been processed completely, close buckets that have been migrated to disk (hopefully none)
            Arrays.stream(hashBucketsOnDisk).filter(Objects::nonNull).forEach(DiskBucket::close);

        }

        BufferedDataContainer resultContainer = exec.createDataContainer(m_outputSpec);

        { // phase 2: partitioning and probing of the probe input

            progress.setMessage("Phase 2/3: Processing larger table");

            // process probe input: process rows immediately for which the matching bucket is in memory;
            // otherwise put the row into a disk bucket
            // materialize only columns that either make it into the output or are needed for joining
            int[] workingColumns = probeInput.getDataTableSpec().columnsToIndices(workingTableSpec(probeInput).getColumnNames());
            int[] joinColumns = workingTableSpec(probeInput).columnsToIndices(m_tableSettings.get(probeInput).joinColumnNames);
            try (CloseableRowIterator probeRows = probeInput.filter(TableFilter.materializeCols(workingColumns)).iterator()) {

                while (probeRows.hasNext()) {

                    progress.incProgressAndCheckCanceled(0.33/probeInput.size());

                    DataRow probeRow = new FilterColumnRow(probeRows.next(), workingColumns);


                    JoinTuple joinAttributeValues = getJoinTuple(joinColumns, probeRow);
                    int bucket = Math.abs(joinAttributeValues.hashCode() % numBuckets);

                    if (bucket >= firstInMemoryBucket) {
                        // if hash bucket is in memory, output result directly
                        hashBucketsInMemory[bucket].joinSingleRow(joinAttributeValues, probeRow, resultContainer, this::match);
                        progress.incProbeRowsProcessedInMemory();
                    } else {
                        // if hash bucket is on disk, process this row later, when joining the matching buckets
                        probeBuckets[bucket].add(probeRow, exec);
                        progress.incProbeRowsProcessedFromDisk();
                    }
                }
            }

            // probe input partitioning is done; no more rows will be added to buckets
            Arrays.stream(probeBuckets).forEach(DiskBucket::close);

        }

        { // phase 3: joining the rows that went to disk because not enough memory was available

            progress.setMessage("Phase 3/3: Processing data on disk");

            // join only the pairs of buckets that haven't been processed in memory
            for (int i = 0; i < firstInMemoryBucket; i++) {

                progress.incProgressAndCheckCanceled(0.33/firstInMemoryBucket);

                probeBuckets[i].join(hashBucketsOnDisk[i], this::match, unmatchedProbeRow -> {}, unmatchedHashRow -> {}, exec);

            }

        }

        resultContainer.close();
        return resultContainer.getTable();

    }

    DataRow match(final DataRow probeRow, final DataRow hashRow) {

        DataRow leftRow = getLeft(probeRow, hashRow);
        DataRow rightRow = getRight(probeRow, hashRow);

        DataCell[] dataCells = new DataCell[m_outputSpec.getNumColumns()];
        { // fill data cells

            int nextCell = 0;

            int[] leftOutputIndices = m_tableSettings.get(m_leftTable).includeColumnIndices;
            for (int j = 0; j < leftOutputIndices.length; j++) {
                dataCells[nextCell++] = leftRow.getCell(leftOutputIndices[j]);
            }

            int[] rightOutputIndices = m_tableSettings.get(m_rightTable).includeColumnIndices;
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

        final BufferedDataTable m_forTable;

        // stores a part of m_forTable on disk.
        BufferedDataTable m_tablePartition;

        DiskBucket(final BufferedDataTable forTable) {
            m_forTable = forTable;
        }

        void add(final DataRow row, final ExecutionContext exec) {
            // lazy initialization of the data container, only when rows are actually added
            if (m_container == null) {
                m_container = exec.createDataContainer(workingTableSpec(m_forTable));
            }
            m_container.addRowToTable(row);
        }

        void join(final DiskBucket other, final BiConsumer<DataRow, DataRow> matches,
            final Consumer<DataRow> unmatchedProbeRows, final Consumer<DataRow> unmatchedHashRows,
            final ExecutionContext exec) {
            // use nested loop if doesn't fit in-memory
            // read and index otherwise
            // TODO refactor
            //           NestedHashJoin.join(m_settings, m_tablePartition, other.m_tablePartition, resultContainer);

            BufferedDataTable leftPartition = m_tablePartition;
            BufferedDataTable rightPartition = other.m_tablePartition;

            // build index
            String[] hashJoinColumnNames = m_tableSettings.get(other.m_forTable).joinColumnNames;
            int[] hashJoinColumns = other.m_tablePartition.getDataTableSpec().columnsToIndices(hashJoinColumnNames);

            // TODO close this iterator?
            Map<JoinTuple, List<DataRow>> index = StreamSupport.stream(rightPartition.spliterator(), false).collect(Collectors.groupingBy(row -> getJoinTuple(hashJoinColumns, row)));

            // factor out to drop-in replace with something that scales to long
            BitSet matchedHashRows = new BitSet(rightPartition.getRowCount());

            String[] probeJoinColumnNames = m_tableSettings.get(m_forTable).joinColumnNames;
            int[] joinColumns = m_tablePartition.getDataTableSpec().columnsToIndices(probeJoinColumnNames);

            try (CloseableRowIterator probeRows = leftPartition.iterator()) {

                while (probeRows.hasNext()) {

                    exec.checkCanceled();

                    DataRow probeRow = probeRows.next();
                    JoinTuple probeTuple = getJoinTuple(joinColumns, probeRow);

                    List<DataRow> matching = index.get(probeTuple);

                    if (matching == null) {
                        // this row from the bigger table has no matching row in the other table
                        // if we're performing an outer join, include the row in the result
                        // if we're performing an inner join, ignore the row
                        //                unmatched.accept(m_bigger, row);
                        unmatchedProbeRows.accept(probeRow);
                    } else {
                        for (DataRow hashRow : matching) {
//                            matchedHashRows.set(hashRow.offset);
                            matches.accept(probeRow, hashRow);
                        }
                    }

                }
            } catch (CanceledExecutionException e) {

            }

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

        // TODO add a mechanism to check for cancels only every x progress ticks
//        int skipNextCheckCancels = -1;

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
         * @param memoryLimitPercent
         * @param i
         * @return
         */
        int checkBackIn = 0;
        public boolean isMemoryLow(final int memoryLimitPercent, final int checkBackAfter) {
            if(checkBackIn == 0) {
                checkBackIn = checkBackAfter;
                return MemoryAlertSystem.getUsage() > memoryLimitPercent / 100.;
            } else {
                checkBackIn--;
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

            while(MemoryAlertSystem.getUsage() < targetPercentage) {
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

    }

}
