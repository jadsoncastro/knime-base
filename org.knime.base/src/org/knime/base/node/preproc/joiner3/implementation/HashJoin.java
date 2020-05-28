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
 *   Apr 27, 2020 (carlwitt): created
 */
package org.knime.base.node.preproc.joiner3.implementation;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.JoinMode;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.streamable.StreamableFunction;

/**
 *
 * TODO write docs.
 * This currently supports conjunctive (rows must match on every column pair) equijoins only.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 *
 */
public class HashJoin extends JoinImplementation {

    private interface RowHandler extends BiConsumer<BufferedDataTable, DataRow> {
    }

    private final static RowHandler IGNORE_ROW = (table, row) -> { };

    private final Map<BufferedDataTable, List<DataRow>> m_unmatched = new HashMap<>();

    //    Map<Joiner3Settings.JoinMode, RowHandler> unmatchedHandlers = new HashMap<Joiner3Settings.JoinMode, RowHandler>(){{
    //        // when performing an inner join, unmatched rows do not contribute to the output
    //        put(Joiner3Settings.JoinMode.InnerJoin, IGNORE_ROW);
    //        put(Joiner3Settings.JoinMode.LeftOuterJoin, HashJoin.this::handleUnmatchedInner);
    //        put(Joiner3Settings.JoinMode.RightOuterJoin, HashJoin.this::handleUnmatchedInner);
    //        put(Joiner3Settings.JoinMode.FullOuterJoin, HashJoin.this::handleUnmatchedInner);
    //    }};

    private DataTableSpec m_joinedTableSpec;

    HashJoin(final Joiner3Settings settings, final BufferedDataTable... tables) {
        // FIXME
        super(settings, tables[0], tables[1]);
    }

    //    Map<BufferedDataTable, BitSet> markedRows = new HashMap<>();
    //
    //    /**
    //     * TODO to extract a strategy pattern here, one could use Function<BufferedDataTable, LongConsumer> to generate
    //     * the function that accepts the long row index of the matched row.
    //     *
    //     * Indicates that the row
    //     * @param table
    //     * @param rowIndex
    //     */
    //    protected void markMatched(final BufferedDataTable table, final int rowIndex) {
    //
    //        BitSet marked = markedRows.computeIfAbsent(table, t -> new BitSet(t.getRowCount()));
    //        marked.set(rowIndex);
    //
    //    }

    //    Map<BufferedDataTable, List<DataRow>> m_unmatched = new HashMap();
    //
    //    protected void noteUnmatched(final BufferedDataTable table, final DataRow row) {
    //
    //        List<DataRow> unmatched = m_unmatched.computeIfAbsent(table, t -> new LinkedList<>());
    //        unmatched.add(row);
    //
    //
    //    }

    @Override
    public JoinContainer join(final ExecutionContext exec) throws CanceledExecutionException, InvalidSettingsException {

        RowHandler unmatchedRowHandler =
            m_settings.getJoinMode() == JoinMode.InnerJoin ? IGNORE_ROW : this::handleUnmatched;

        // FIXME
        return null; //_twoWayJoin_(exec, unmatchedRowHandler);

    }



    //    Map<Joiner3Settings.JoinMode, RowHandler> unmatchedHandlers = new HashMap<Joiner3Settings.JoinMode, RowHandler>(){{
    //        // when performing an inner join, unmatched rows do not contribute to the output
    //        put(Joiner3Settings.JoinMode.InnerJoin, IGNORE_ROW);
    //        put(Joiner3Settings.JoinMode.LeftOuterJoin, HashJoin.this::handleUnmatchedInner);
    //        put(Joiner3Settings.JoinMode.RightOuterJoin, HashJoin.this::handleUnmatchedInner);
    //        put(Joiner3Settings.JoinMode.FullOuterJoin, HashJoin.this::handleUnmatchedInner);
    //    }};

    private void handleUnmatched(final BufferedDataTable table, final DataRow row) {
        List<DataRow> unmatchedForTable = m_unmatched.computeIfAbsent(table, k -> new LinkedList<DataRow>());
        unmatchedForTable.add(row);
    }

    /**
     */
//    private BufferedDataTable _twoWayJoin_(final ExecutionContext exec, final BufferedDataTable leftTable,
//        final BufferedDataTable rightTable, final RowHandler unmatched)
//        throws CanceledExecutionException, InvalidSettingsException {
//
//        // TODO remove timing
//        long before;
//        long after;
//
//        // This does some input data checking, too
//        //        DataTableSpec joinedTableSpec = createSpec(new DataTableSpec[] {
//        //                leftTable.getDataTableSpec(),
//        //                rightTable.getDataTableSpec()}, m_settings, IGNORE_WARNINGS);
//
//        // TODO maybe the join tuple can be stripped and DataCell[] used directly, depends on whether custom comparison logic is needed
//
//        // FIXME check sizes here
//        BufferedDataTable probeInput = leftTable.size() > rightTable.size() ? leftTable : rightTable;
//        BufferedDataTable hashInput = leftTable.size() > rightTable.size() ? rightTable : leftTable;
//
//        //---------------------------------------------
//        // build index
//        //---------------------------------------------
//
//        exec.setProgress("Building Hash Table");
//        // FIXME swapping tables
//        Map<JoinTuple, List<DataRow>> index = StreamSupport.stream(leftTable.spliterator(), false)
//                .collect(Collectors.groupingBy(m_tableSettings.get(rightTable)::getJoinTupleWorkingRow));
//
//
//        //---------------------------------------------
//        // build table spec
//        //---------------------------------------------
//
//        m_joinedTableSpec = Joiner.createOutputSpec(m_settings, s -> {},
//            leftTable.getSpec(), rightTable.getSpec());
//
//        BufferedDataContainer result = exec.createDataContainer(m_joinedTableSpec);
//
//        //---------------------------------------------
//        // do join
//        //---------------------------------------------
//
//        exec.setProgress("Joining");
//
//        // keep in memory, flush to disk if necessary
//        // blocks adding more rows if it gets too full
//
//        // only get columns that are needed (join attributes and retained
//        //        bigger.filter(TableFilter.materializeCols(1,2,3));
//
//        before = System.currentTimeMillis();
//
//        // FIXME
//        Extractor biggerJoinAttributes = null; //getExtractor(m_bigger);
//
//        long rowIndex = 0;
//
//        try (CloseableRowIterator bigger = probeInput.iterator()) {
//            while (bigger.hasNext()) {
//                DataRow row = bigger.next();
//
//                exec.checkCanceled();
//
//                JoinTuple query = biggerJoinAttributes.apply(row);
//
//                List<DataRow> matches = index.get(query);
//
//                if (matches == null) {
//                    // this row from the bigger table has no matching row in the other table
//                    // if we're performing an outer join, include the row in the result
//                    // if we're performing an inner join, ignore the row
//                    unmatched.accept(m_probe, row);
//                    continue;
//                }
//
//                updateProgress(exec, m_probe, rowIndex);
//
//                for (DataRow match : matches) {
//                    DataRow outer = getLeft(row, match);
//                    DataRow inner = getRight(row, match);
//
//                    RowKey newRowKey = concatRowKeys(outer, inner);
//                    result.addRowToTable(new JoinedRow(newRowKey, outer, inner));
//                }
//
//                rowIndex++;
//            }
//        }
//
//        // does something only for outer joins
//        addUnmatchedRows(result);
//
//        result.close();
//
//        after = System.currentTimeMillis();
//        System.out.println("Joining: " + (after - before));
//        before = System.currentTimeMillis();
//
//        BufferedDataTable bdt = result.getTable();
//
//        return bdt;
//
//        //---------------------------------------------
//        // sort
//        //---------------------------------------------
//
//        //        BufferedDataTableSorter bdts = new BufferedDataTableSorter(bdt, Comparator.comparing((final DataRow r) -> r.getKey().getString()));
//        //
//        //        exec.setProgress("Sorting");
//        //        BufferedDataTable sorted = bdts.sort(exec);
//        //        after = System.currentTimeMillis();
//        //        System.out.println("Sorting: " + (after-before));
//        //        return sorted;
//    }


//    private void addUnmatchedRows(final BufferedDataContainer result) {
//
//        long rowId = result.size();
//
//        // for each table, output the unmatched rows
//        // output unmatched rows for outer table first, then unmatched rows for inner table(s)
//        for(BufferedDataTable table : m_tables) {
//
//            int[] indices = m_projectionColumns.get(table);
//
//            // the offset of the first column to fill with cells from `table`
//            // equals the number of columns in tables before this table
//            int fillBegin = m_tables.stream().limit(m_tables.indexOf(table))
//                .mapToInt(tableBefore -> tableBefore.getDataTableSpec().getNumColumns()).sum();
//
//            List<DataRow> unmatchedRows = m_unmatched.getOrDefault(table, EMPTY_LIST);
//
//            for(DataRow row : unmatchedRows) {
//
//                // key is just a running number
//                RowKey key = new RowKey(Long.toString(rowId));
//
//                // cell data is copied from the source row and padded with missing values
//                DataCell[] cells = new DataCell[m_joinedTableSpec.getNumColumns()];
//                Arrays.fill(cells, DataType.getMissingCell());
//
//                for (int i = 0; i < indices.length; i++) {
//                    cells[i + fillBegin] = row.getCell(indices[i]);
//                }
//
//                // compose and add output row
//                DataRow padded = new DefaultRow(key, cells);
//                result.addRowToTable(padded);
//
//                rowId++;
//            }
//        }
//
//    }

    private void updateProgress(final ExecutionContext exec, final BufferedDataTable m_bigger, final long rowIndex) {
        exec.setProgress(1. * rowIndex / m_bigger.getRowCount());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected StreamableFunction getStreamableFunction() {
        return new StreamableFunction() {

            @Override
            public DataRow compute(final DataRow input) throws Exception {

                return null;
            }

        };
    }

    static void withFallback(final JoinTableSettings table1, final JoinTableSettings table2,
        final JoinContainer container, final ExecutionContext exec) throws CanceledExecutionException {

        boolean table1IsBigger = table1.materializedCells > table2.materializedCells;
        JoinTableSettings probeInput = table1IsBigger ? table1 : table2;
        JoinTableSettings hashInput = table1IsBigger ? table2 : table1;

        // TODO nested loop fallback
        // TODO
        Map<JoinTuple, List<DataRow>> index = StreamSupport.stream(hashInput.m_forTable.spliterator(), false)
            .collect(Collectors.groupingBy(hashInput::getJoinTuple));

        // factor out to drop-in replace with something that scales to long
////        BitSet matchedHashRows = new BitSet(hashBucket.getRowCount());
//
//        DataTableSpec joinedBucketsSpec = outerSettings.sortedChunksTableWith(innerSettings);
//
//        BufferedDataContainer result = exec.createDataContainer(joinedBucketsSpec);
//
        Consumer<DataRow> unmatchedProbeRows = probeInput.isLeft ? container::addLeftOuter : container::addRightOuter;

        try (CloseableRowIterator probeRows = probeInput.m_forTable.iterator()) {

            while (probeRows.hasNext()) {

                exec.checkCanceled();

                DataRow probeRow = probeRows.next();
                JoinTuple probeTuple = probeInput.getJoinTupleWorkingRow(probeRow);

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
//                        DataRow left = table1.isOuter ?
                        // FIXME!! outer / inner -> left / right
                        container.addMatch(probeRow, hashRow);
                    }
                }

        }

    }
    }

}