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
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.Extractor;
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
import org.knime.core.node.NodeProgressMonitor;
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

        NodeProgressMonitor progress = exec.getProgressMonitor();

        MemoryActionIndicator softWarning = MemoryAlertSystem.getInstanceUncollected().newIndicator();

        { // phase 1: partition the hash input into buckets, keep as many buckets as possible in memory

            progress.setMessage("Processing smaller table");
            progress.setProgress(0);

            try (CloseableRowIterator hashRows = hashInput.iterator()) {

                while (hashRows.hasNext()) {

                    exec.checkCanceled();
                    progress.setProgress(progress.getProgress() + 0.33/hashInput.size());

                    // if memory is running low and there are hash buckets in-memory
                    // TODO check memory again only after N rows
                    // or check only every 100 rows or so.
                    boolean someBucketsAreInMemory = firstInMemoryBucket < numBuckets;
                    if (softWarning.lowMemoryActionRequired() && someBucketsAreInMemory) {
                        // migrate the next in-memory hash bucket to disk
                        // convert to disk bucket, release heap memory
                        hashBucketsOnDisk[firstInMemoryBucket] = hashBucketsInMemory[firstInMemoryBucket].toDisk(exec);
                        firstInMemoryBucket++;
                        m_bean.numHashPartitionsOnDisk = firstInMemoryBucket;
                        LOGGER.warn(String.format("Running out of memory. Migrating bucket %s/%s to disk", firstInMemoryBucket, numBuckets));
                    }

                    DataRow hashRow = hashRows.next();
                    JoinTuple joinAttributeValues = getJoinTuple(hashInput, hashRow);
                    int bucket = joinAttributeValues.hashCode() & bitmask;

                    if (bucket >= firstInMemoryBucket) {
                        // if the hash bucket is in memory, add and index row
                        hashBucketsInMemory[bucket].add(joinAttributeValues, hashRow, true);
                    } else {
                        // if the bucket is on disk store for joining later
                        hashBucketsOnDisk[bucket].add(hashRow, exec);
                    }

                }
            }
        }

        BufferedDataContainer resultContainer = exec.createDataContainer(m_outputSpec);

        { // phase 2: partitioning and probing of the probe input

            progress.setMessage("Processing larger table");

            // process probe input: process rows immediately for which the matching bucket is in memory;
            // otherwise put the row into a disk bucket
            try (CloseableRowIterator iterator = probeInput.iterator()) {

                while (iterator.hasNext()) {

                    exec.checkCanceled();
                    progress.setProgress(progress.getProgress() + 0.33/probeInput.size());

                    DataRow probeRow = iterator.next();
                    JoinTuple joinAttributeValues = getJoinTuple(probeInput, probeRow);
                    int bucket = joinAttributeValues.hashCode() & bitmask;

                    if (bucket >= firstInMemoryBucket) {
                        // if hash bucket is in memory, output result directly
                        hashBucketsInMemory[bucket].joinSingleRow(joinAttributeValues, probeRow, resultContainer);
                    } else {
                        // if hash bucket is on disk, process this row later, when joining the matching buckets
                        probeBuckets[bucket].add(probeRow, exec);
                    }
                }
            }

            // partitioning is done; containers can be closed and converted to BufferedDataTables
            Arrays.stream(hashBucketsOnDisk).filter(Objects::nonNull).forEach(DiskBucket::close);
            Arrays.stream(probeBuckets).forEach(DiskBucket::close);

        }

        { // phase 3: joining the rows that went to disk because not enough memory was available

            progress.setMessage("Processing data on disk");
            // join only the pairs of buckets that haven't been processed in memory
            for (int i = 0; i < firstInMemoryBucket; i++) {

                exec.checkCanceled();
                progress.setProgress(progress.getProgress() + 0.33/firstInMemoryBucket);

                probeBuckets[i].join(hashBucketsOnDisk[i], resultContainer, exec);

            }

        }

        resultContainer.close();
        return resultContainer.getTable();

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
                m_container = exec.createDataContainer(m_forTable.getDataTableSpec());
            }
            m_container.addRowToTable(row);
        }

        void join(final DiskBucket other, final DataContainer resultContainer, final ExecutionContext exec) {
            // use nested loop if doesn't fit in-memory
            // read and index otherwise
            // TODO refactor
//           NestedHashJoin.join(m_settings, m_tablePartition, other.m_tablePartition, resultContainer);

            BufferedDataTable leftPartition = m_tablePartition;
            BufferedDataTable rightPartition = other.m_tablePartition;

            // build index
            Extractor joinAttributeExtractorHash = row -> getJoinTuple(other.m_forTable, row);

            // TODO close this iterator?
            Map<JoinTuple, List<DataRow>> index = StreamSupport.stream(rightPartition.spliterator(), false).collect(Collectors.groupingBy(joinAttributeExtractorHash));

            try (CloseableRowIterator probeRows = leftPartition.iterator()) {

                while (probeRows.hasNext()) {

                    exec.checkCanceled();

                    DataRow probeRow = probeRows.next();
                    JoinTuple probeTuple = getJoinTuple(m_forTable, probeRow);

                    List<DataRow> matches = index.get(probeTuple);

                    if (matches == null) {
                        // this row from the bigger table has no matching row in the other table
                        // if we're performing an outer join, include the row in the result
                        // if we're performing an inner join, ignore the row
                        //                unmatched.accept(m_bigger, row);
                        continue;
                    } else {
                        for (DataRow hashRow : matches) {
                            DataRow outer = getOuter(probeRow, hashRow);
                            DataRow inner = getInner(probeRow, hashRow);

                            RowKey newRowKey = concatRowKeys(outer, inner);
                            resultContainer.addRowToTable(new JoinedRow(newRowKey, outer, inner));
                        }
                    }

                }
            } catch (CanceledExecutionException e) {

            }

        }

        void close() {
            if(m_container != null) {
                m_container.close();
                m_tablePartition = m_container.getTable();
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
            final BufferedDataContainer resultContainer) {

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

        public void join(final InMemoryBucket other, final DataContainer resultContainer, final ExecutionContext exec)
            throws CanceledExecutionException {

            //            try (CloseableRowIterator bigger = m_bigger.iterator()) {

            Iterator<JoinTuple> keyIterator = m_joinAttributes.iterator();

            for (DataRow probeRow : m_rows) {

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

    @javax.management.MXBean
    public interface IHybridHashJoinMBean {

//        void setNumBits(int numBits);
        int getNumPartitionsOnDisk();

        void fillHeap(float targetPercentage);

        void releaseTestAllocations();
    }



    final HybridHashJoinMBean m_bean = new HybridHashJoinMBean();
    {
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        try {
            ObjectName name = new ObjectName("org.knime.base.node.preproc.joiner3.jmx:type=HybridHashJoin");
            server.registerMBean(m_bean, name);
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public class HybridHashJoinMBean implements IHybridHashJoinMBean {

        int numHashPartitionsOnDisk;
        List<double[]> memoryConsumer = new LinkedList<>();
        MemoryAlertSystem memoryAlertSystem = MemoryAlertSystem.getInstance();

        @Override
        public int getNumPartitionsOnDisk() {
            return numHashPartitionsOnDisk;
        }

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

}
