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

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.knime.base.data.join.JoinedTable;
import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.sort.BufferedDataTableSorter;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;

/**
 *
 * This currently supports conjunctive (rows must match on every column pair) equijoins only.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 *
 */
public class HashJoin extends AbstractJoiner {

    /**
     */
    public HashJoin(final Joiner3Settings settings, final BufferedDataTable outer,
        final BufferedDataTable... innerTables) {
        super(settings, outer, innerTables[0]);
        // TODO Auto-generated constructor stub
    }



    /**
     */
    @Override
    public BufferedDataTable computeJoinTable(final BufferedDataTable leftTable, final BufferedDataTable rightTable,
        final ExecutionContext exec, final Consumer<String> runtimeWarningHandler)
        throws CanceledExecutionException, InvalidSettingsException {

        // This does some input data checking, too
//        DataTableSpec joinedTableSpec = createSpec(new DataTableSpec[] {
//                leftTable.getDataTableSpec(),
//                rightTable.getDataTableSpec()}, m_settings, IGNORE_WARNINGS);



        // build a hash index of the smaller table

        Map<JoinTuple, List<DataRow>> index;
        // TODO maybe the join tuple can be stripped and DataCell[] used directly, depends on whether custom comparison logic is needed

        exec.setProgress("Building Hash Table");

        //---------------------------------------------
        // build index
        //---------------------------------------------

        long before = System.currentTimeMillis();

        // TODO
        // try to estimate index size? probably too difficult, maybe use size estimate of future API
        // or try build index and choose different implementation if failed
        index =
            StreamSupport.stream(m_smaller.spliterator(), false).collect(Collectors.groupingBy(getExtractor(m_smaller)));

        long after = System.currentTimeMillis();
        System.out.println("Indexing: " + (after-before));

        //---------------------------------------------
        // build table spec
        //---------------------------------------------


        // FIXME do the projections
        // just concat the columns of the left table with the right table
        DataTableSpec joinedTableSpec = new JoinedTable(leftTable, rightTable, JoinedTable.METHOD_APPEND_SUFFIX, " (#1)", true).getDataTableSpec();

        //---------------------------------------------
        // do join
        //---------------------------------------------

        exec.setProgress("Joining");

        // keep in memory, flush to disk if necessary
        // blocks adding more rows if it gets too full
        BufferedDataContainer result = exec.createDataContainer(joinedTableSpec);

        long matchNumber = 0;
        long rowIndex = 0;

        // only get columns that are needed (join attributes and retained
//        bigger.filter(TableFilter.materializeCols(1,2,3));

        // very full even after GC
//        MemoryAlertSystem.getInstance().isMemoryLow()
        // heap ca 65% full
//        MemoryAlertSystem.getInstanceUncollected();

        before = System.currentTimeMillis();

        Extractor biggerJoinAttributes = getExtractor(m_bigger);


        for(DataRow row : m_bigger) {

            exec.checkCanceled();

            JoinTuple query = biggerJoinAttributes.apply(row);
            List<DataRow> matches = index.get(query);
            if(matches == null) {
                continue;
            }

            exec.setProgress(1.*rowIndex/m_bigger.getRowCount());

            for(DataRow match : matches) {
                DataRow outer = getOuter(row, match);
                DataRow inner = getInner(row, match);

                RowKey newRowKey = concatRowKeys(outer, inner);
                result.addRowToTable(new JoinedRow(newRowKey, outer, inner));
            }

            rowIndex++;

        }
        result.close();

        after = System.currentTimeMillis();
        System.out.println("Joining: " + (after-before));
        before = System.currentTimeMillis();

        BufferedDataTable bdt = result.getTable();

        return bdt;

        //---------------------------------------------
        // sort
        //---------------------------------------------

//        BufferedDataTableSorter bdts = new BufferedDataTableSorter(bdt, Comparator.comparing((final DataRow r) -> r.getKey().getString()));
//
//        exec.setProgress("Sorting");
//        BufferedDataTable sorted = bdts.sort(exec);
//        after = System.currentTimeMillis();
//        System.out.println("Sorting: " + (after-before));
//        return sorted;
    }

    @Deprecated
    public BufferedDataTable computeJoinTableParallel(final BufferedDataTable leftTable, final BufferedDataTable rightTable,
        final ExecutionContext exec, final Consumer<String> runtimeWarningHandler)
        throws CanceledExecutionException, InvalidSettingsException {

        // This does some input data checking, too
        //    DataTableSpec joinedTableSpec = createSpec(new DataTableSpec[] {
        //            leftTable.getDataTableSpec(),
        //            rightTable.getDataTableSpec()}, m_settings, IGNORE_WARNINGS);

        BufferedDataTable[] ordered = new BufferedDataTable[]{leftTable, rightTable};
        boolean leftIsBigger = leftTable.getRowCount() >= rightTable.getRowCount();
        int biggerIndex = leftIsBigger ? 0 : 1;
        int smallerIndex = 1 - biggerIndex;
        BufferedDataTable bigger = ordered[biggerIndex];
        BufferedDataTable smaller = ordered[smallerIndex];

        // build a hash index of the smaller table
        Map<JoinTuple, List<DataRow>> index;
        // TODO maybe the join tuple can be stripped and DataCell[] used directly, depends on whether custom comparison logic is needed

        Extractor smallerJoinAttributes = getExtractor(smaller);
        Extractor biggerJoinAttributes = getExtractor(bigger);

        exec.setProgress("Building Hash Table");

        index = StreamSupport.stream(smaller.spliterator(), false).collect(Collectors.groupingBy(smallerJoinAttributes));

        // FIXME do the projections
        // just concat the columns of the left table with the right table
        DataTableSpec joinedTableSpec =
            new JoinedTable(leftTable, rightTable, JoinedTable.METHOD_APPEND_SUFFIX, "_right", true).getDataTableSpec();

        exec.setProgress("Joining");

        // add
        //    exec.checkCanceled()
        System.out.println("PARALLEL");
        BufferedDataContainer result =
            StreamSupport.stream(bigger.spliterator(), false).collect(() -> exec.createDataContainer(joinedTableSpec),
                (final BufferedDataContainer partial, final DataRow row) -> {

                    JoinTuple query = biggerJoinAttributes.apply(row);
                    List<DataRow> matches = index.get(query);
                    if (matches != null) {

//                        exec.setProgress(1. * rowIndex / bigger.getRowCount());
                        //        System.out.println(String.format("Progress: %.1f%%", 100.*rowIndex/bigger.getRowCount()));

                        for (DataRow match : matches) {
                            try {
                                exec.checkCanceled();
                            } catch (CanceledExecutionException e) {
                                break;
                            }
                            DataRow left = leftIsBigger ? row : match;
                            DataRow right = leftIsBigger ? match : row;
                            RowKey newRowKey = new RowKey(left.getKey().getString() + right.getKey().getString());
                            partial.addRowToTable(new JoinedRow(newRowKey, left, right));
                        }

                    }

                }, HashJoin::addAll);

        result.close();

        // OUTDATED
        BufferedDataTable bdt = result.getTable();

        BufferedDataTableSorter bdts = new BufferedDataTableSorter(bdt, Comparator.comparing((final DataRow r) -> r.getKey().getString()));

        exec.setProgress("Sorting");
        return bdts.sort(exec);

    }

//    private BufferedDataContainer accumulatorSupplier() {
//        return

    private static void addAll(final BufferedDataContainer master, final BufferedDataContainer partial) {
        if(!partial.isClosed()) {
            partial.close();
        }
        partial.getTable().forEach(row -> master.addRowToTable(row));
    }

}
