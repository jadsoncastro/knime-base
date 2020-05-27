/*
 * ------------------------------------------------------------------------
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
 * ------------------------------------------------------------------------
 *
 * History
 *   25.11.2009 (Heiko Hofer): created
 */
package org.knime.base.node.preproc.joiner3.implementation;

import java.util.function.BiFunction;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.LongCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;

/**
 * A Container used to collect DataRows. The container has three
 * categories for inner, left outer and right outer matches.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 * @author Heiko Hofer
 */

public class JoinContainer implements IJoinContainer {

    JoinTableSettings m_leftSettings, m_rightSettings;

    private BufferedDataContainer m_matches;

    private BufferedDataContainer m_leftOuter;

    private BufferedDataContainer m_rightOuter;

    // TODO remove
    private DataTableSpec m_spec;

    private long m_rowCount;

    private final DataTableSpec m_outputSpec;

    private final ExecutionContext m_exec;

    private boolean m_sortable;

    private BiFunction<DataRow, DataRow, DataRow> m_joinRowConstructor;

    /**
     * Create a new instance.
     * @param settings The settings object of the OutputRow.
     * @param settings1 one join table
     * @param settings2 another join table
     * @throws IllegalArgumentException if
     */
    JoinContainer(final JoinTableSettings settings1, final JoinTableSettings settings2, final ExecutionContext exec, final boolean sortable) {
        DataTableSpec inMemorySpec = sortable ? settings1.outputTableSpecWith(settings2)
            : settings1.sortedChunksTableWith(settings2);
        m_sortable = sortable;
        if(!(settings1.isLeft ^ settings2.isLeft)) {
            throw new IllegalArgumentException(String.format(
                "One join table must be declared as left and the other as right.\n"
                    + "First table is declared as %s, second table is declared as %s.",
                settings1.isLeft ? "left" : "right", settings2.isLeft ? "left" : "right"));
        }
        m_leftSettings = settings1.isLeft ? settings1 : settings2;
        m_rightSettings = settings1.isLeft ? settings2 : settings1;
        m_exec = exec;
        m_outputSpec = settings1.outputTableSpecWith(settings2);
//        m_joinRowConstructor = sortable ? this::joinUnmaterializedRowsToIntermediate : this::joinRowsDirect;
    }

    long getRowCount() {
        return m_rowCount;
    }

    /**
     * {@inheritDoc}
     * @param right
     */
    @Override
    public IJoinContainer addMatch(final DataRow left, final DataRow right) {
        return addMatch(left, right, -1);
    }

    /**
     * {@inheritDoc}
     * @param right
     */
    @Override
    public IJoinContainer addMatch(final DataRow left, final DataRow right, final long order) {

        // if this is sortable, we have to remember to row offset for the probe row
        // if this is not sortable, we can output result format directly
        //

//        if (allInMemory) joinRowsDirect

/*
 *                             // output row in sortable format containing the outer row offset
                            AppendedColumnRow probeRowWithOffset = new AppendedColumnRow(probeRow, new LongCell(rowOffset));
                            // do inMemoryResults.addRow <- ... instead
                            hashBucketsInMemory[bucket].joinSingleRow(joinAttributeValues,
                                probeRowWithOffset, inMemoryResults,
                                this::joinUnmaterializedRowsToIntermediate);
 */

        if (null == m_matches) {
            DataTableSpec spec = m_sortable ?
                m_leftSettings.sortedChunksTableWith(m_rightSettings) :
                m_leftSettings.outputTableSpecWith(m_rightSettings);
            m_matches = m_exec.createDataContainer(spec);
        }
        m_matches.addRowToTable(joinRows(left, right, order));
        m_rowCount++;
        return this;
    }

    /**
     * @param orderRow
     * @param probeRow full row
     * @param hashRow full row
     * @return the output row according to m_outputspec
     */
    DataRow joinRows(final DataRow left, final DataRow right, final long order) {

        // TODO can we avoid this?
        // but we can't release the original rows from heap, if we don't...

        final int numColumns = m_outputSpec.getNumColumns() + (m_sortable ? 1 : 0);

        final DataCell[] dataCells = new DataCell[numColumns];

        int cell = 0;

        if(m_sortable){

            dataCells[cell++] = new LongCell(order);

        }
        { // fill data cells



            int[] outerIncludes = m_leftSettings.includeColumns;
            for (int i = 0; i < outerIncludes.length; i++) {
                dataCells[cell++] = left.getCell(outerIncludes[i]);
            }

            int[] innerIncludes = m_rightSettings.includeColumns;
            for (int i = 0; i < innerIncludes.length; i++) {
                dataCells[cell++] = right.getCell(innerIncludes[i]);
            }

        }

        // about the same speed
//        String newRowKey = outerRow.getKey().getString().concat("_").concat(innerRow.getKey().getString());
        // TODO move JoinImplementation.concatRowKeys here?
        return new DefaultRow(JoinImplementation.concatRowKeys(left, right), dataCells);
    }

    @Override
    public IJoinContainer addRightOuter(final DataRow row) {
        if (null == m_rightOuter) {
            m_rightOuter = m_exec.createDataContainer(m_spec);
        }
        m_rightOuter.addRowToTable(row);

        m_rowCount++;
        return this;
    }

    @Override
    public IJoinContainer addLeftOuter(final DataRow row) {
        if (null == m_leftOuter) {
            m_leftOuter = m_exec.createDataContainer(m_spec);
        }
        m_leftOuter.addRowToTable(row);

        m_rowCount++;
        return this;
    }

    @Override
    public BufferedDataTable getMatches() {
        // TODO make optional
        return null != m_matches ? m_matches.getTable() : null;
    }

    @Override
    public BufferedDataTable getRightOuter() {
        return null != m_rightOuter ? m_rightOuter.getTable() : null;
    }

    @Override
    public BufferedDataTable getLeftOuter() {
        return null != m_leftOuter ? m_leftOuter.getTable() : null;
    }

    @Override
    public JoinContainer close() {
        if (null != m_matches) {
            m_matches.close();
        }
        if (null != m_rightOuter) {
            m_rightOuter.close();
        }
        if (null != m_leftOuter) {
            m_leftOuter.close();
        }
        return this;
    }


    /**
     * @param inMemoryResults
     */
    public void setMatches(final BufferedDataContainer inMemoryResults) {
        // TODO Auto-generated method stub

    }

//    /**
//     * @param other
//     * @return
//     */
//    public BiFunction<DataRow, DataRow, DataRow> outputConstructorWith(final JoinTableSettings other) {
//        // the input row layout we get is
//        // join + include columns + optionally row index if set to true
//        // the output row layout is outer include columns + inner include columns
//        return (outer, inner) -> {
//            DataCell[] cells = new DataCell[includeColumns.length + other.includeColumns.length];
//            int cell = 0;
//            for (int i = 0; i < includeColumnsWorkingTable.length; i++) {
//                cells[cell++] = outer.getCell(includeColumnsWorkingTable[i]);
//            }
//            for (int i = 0; i < other.includeColumnsWorkingTable.length; i++) {
//                cells[cell++] = inner.getCell(other.includeColumnsWorkingTable[i]);
//            }
//            return new DefaultRow(concatRowKeys(outer, inner), cells);
//        };
//    }

    /**
     * Joins two rows to an sorted chunk output row format
     * TODO maybe add a parameter instead of a cell to probe row
     * @param probeRow full probe row with row offset appended
     * @param hashRow full hash row
     * @return
     */
//    DataRow joinUnmaterializedRowsToIntermediate(final DataRow left, final DataRow right) {
//
//        // probe row has offset appended, but is inner table
//        // compute another working row from two working rows
//        DataCell[] cells = new DataCell[1 + m_leftSettings.includeColumns.length + m_rightSettings.includeColumns.length];
//
//        // maintain row offset from probe row
//        cells[0] = probeRowWithOffset.getCell(probeRowWithOffset.getNumCells()-1);
//
//        int cell = 1;
//        for (int i = 0; i < m_leftSettings.includeColumns.length; i++) {
//            cells[cell++] = left.getCell(m_leftSettings.includeColumns[i]);
//        }
//        for (int i = 0; i < m_rightSettings.includeColumns.length; i++) {
//            cells[cell++] = right.getCell(m_rightSettings.includeColumns[i]);
//        }
//
//        // TODO the separator could be specified in Joiner 2
//        String joinedKey = left.getKey().getString().concat("_").concat(right.getKey().getString());
//        return new DefaultRow(joinedKey, cells);
//
//    }

//    DataRow joinWorkingRowToIntermediate(final DataRow probeRow, final DataRow hashRow) {
//
//        DataRow outerRow = m_probeIsOuter ? probeRow : hashRow;
//        DataRow innerRow = m_probeIsOuter ? hashRow : probeRow;
//
//        // compute another working row from two working rows
//        DataCell[] cells = new DataCell[1 + outerSettings.includeColumns.length + innerSettings.includeColumns.length];
//
//        // maintain row offset from outer row
//        cells[0] = probeRow.getCell(0);
//
//        int cell = 1;
//        for (int i = 0; i < outerSettings.includeColumns.length; i++) {
//            cells[cell++] = outerRow.getCell(outerSettings.includeColumnsWorkingTable[i]);
//        }
//        for (int i = 0; i < innerSettings.includeColumns.length; i++) {
//            cells[cell++] = innerRow.getCell(innerSettings.includeColumnsWorkingTable[i]);
//        }
//
//        // TODO the separator could be specified in Joiner 2
//        String joinedKey = outerRow.getKey().getString().concat("_").concat(innerRow.getKey().getString());
//        return new DefaultRow(joinedKey, cells);
//
//    }

    /**
     * Signals that everything added since last sortedChunkEnd() was in sorted order
     */
    public void sortedChunkEnd() {
        // TODO Auto-generated method stub
        BufferedDataContainer currentSortedChunk = null;

//        // we have k + 1 (possibly empty) tables; one table for each of the k bucket pairs on disk...
//        final List<BufferedDataTable> sortedBuckets = new ArrayList<>();
//
//        @Override
//        public void addMatch(final DataRow row) {
//            if (currentSortedChunk == null) {
//                currentSortedChunk = exec.createDataContainer(outerSettings.sortedChunksTableWith(innerSettings));
//            }
//            currentSortedChunk.addRowToTable(row);
//        }
//
//        @Override
//        public void close() {
//            if (currentSortedChunk != null) {
//                currentSortedChunk.close();
//                sortedBuckets.add(currentSortedChunk.getTable());
//                currentSortedChunk = null;
//            }
//        }
//
//        @Override
//        public BufferedDataTable getMatches() {
//            // n-way merge of the joined result
//            // TODO make this optional, because this adds one round trip to disk: bucket pairs are read into memory, joined and buffered back to disk only
//            // to be read back again and output in correct order.
//            // in contrast to the classical hybrid hash join, which has worst case I/O of 3(R+S) this has 5(R+S)
//            final BufferedDataContainer finale = exec.createDataContainer(m_outputSpec);
//            final DataTableSpec sortedChunkSpec = sortedBuckets.get(0).getDataTableSpec();
//
//            // create iterators that allow looking at the next row without polling it
//            // needed for sorting tables according to output row order of their first row
//            @SuppressWarnings("unchecked")
//            final PeekingIterator<DataRow>[] iterators = sortedBuckets.stream().map(BufferedDataTable::iterator)
//                .map(Iterators::peekingIterator).toArray(PeekingIterator[]::new);
//
//            ToLongFunction<PeekingIterator<DataRow>> getRowOffset =
//                pi -> pi.hasNext() ? ((LongCell)pi.peek().getCell(0)).getLongValue() : -1;
//            Comparator<PeekingIterator<DataRow>> probeRowOffset = Comparator.comparingLong(getRowOffset);
//
//            final int[] allButFirst = IntStream.range(1, sortedChunkSpec.getNumColumns()).toArray();
//
//            boolean hasNext = false;
//            do {
//                // take rows from table with smallest probe row offset
//                Arrays.sort(iterators, probeRowOffset);
//                hasNext = false;
//                for (int i = 0; i < iterators.length; i++) {
//                    if (!iterators[i].hasNext()) {
//                        continue;
//                    }
//                    // take rows from current table as long as their row offsets are contiguous
//                    long lastProbeRowOffset = getRowOffset.applyAsLong(iterators[i]);
//                    long nextProbeRowOffset;
//                    do {
//                        // add the result row, remove the offset column
//                        finale.addRowToTable(new FilterColumnRow(iterators[i].next(), allButFirst));
//                        nextProbeRowOffset = getRowOffset.applyAsLong(iterators[i]);
//                    } while (nextProbeRowOffset == lastProbeRowOffset + 1);
//                    hasNext = hasNext | iterators[i].hasNext();
//                }
//            } while (hasNext);
//            finale.close();
//            return finale.getTable();
//        }
    }


    /**
     * @return
     */
    public BufferedDataTable getSingleTable() {
        // TODO Auto-generated method stub
        return null;
    }
}
