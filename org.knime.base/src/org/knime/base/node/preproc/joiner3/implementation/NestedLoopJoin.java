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

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.streamable.StreamableFunction;

/**
 *
 * For each row in the outer table, iterates over the inner table and generates an output column if the rows have
 * equal values in all specified column pairs.
 *
 * This currently supports conjunctive (rows must match on every column pair) equijoins only.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 *
 */
public class NestedLoopJoin extends JoinImplementation {

    /**
     * @param leftTableSpec
     * @param rightTableSpec
     * @param settings
     * @throws InvalidSettingsException
     */
    public NestedLoopJoin(final Joiner3Settings settings, final BufferedDataTable... tables) {
        // FIXME
        super(settings, tables[0], tables[1]);
    }

    /**
     * Joins the <code>leftTable</code> and the <code>rightTable</code>.
     * @param exec The Execution monitor for this execution.
     * @param joiner TODO
     * @return The joined table.
     * @throws CanceledExecutionException when execution is canceled
     * @throws InvalidSettingsException when inconsistent settings are provided
     */
    @Override
    public BufferedDataTable twoWayJoin(final ExecutionContext exec)
        throws CanceledExecutionException, InvalidSettingsException {
        // FIXME
        return null;
//        // Update increment for reporting progress
//        double numPairs = leftTable.size() * rightTable.size();
//
//        DataTableSpec joinedTableSpec =
//            new JoinedTable(leftTable, rightTable, JoinedTable.METHOD_APPEND_SUFFIX, "_right", true).getDataTableSpec();
//        BufferedDataContainer result = exec.createDataContainer(joinedTableSpec);
//
//        Extractor smallerJoinAttributes = getExtractor(m_smaller);
//        Extractor biggerJoinAttributes = getExtractor(m_bigger);
//
//        long before = System.currentTimeMillis();
//
//        // cache right table
//        DataRow[] smallTableCached = new DataRow[m_smaller.getRowCount()];
//        int i = 0;
//        for (DataRow row : m_smaller) {
//            smallTableCached[i] = row;
//            i++;
//        }
//
//        long after = System.currentTimeMillis();
//        System.out.println("Caching: " + (after - before));
//        before = System.currentTimeMillis();
//
//        int counter = 0;
//
//        for (DataRow bigger : m_bigger) {
//
//            for (int j = 0; j < smallTableCached.length; j++) {
//                DataRow smaller = smallTableCached[j];
//
//                exec.getProgressMonitor().setProgress(1. * counter / numPairs);
//                exec.checkCanceled();
//
//                if (smallerJoinAttributes.apply(smaller).equals(biggerJoinAttributes.apply(bigger))) {
//                    DataRow outer = getLeft(bigger, smaller);
//                    DataRow inner = getRight(bigger, smaller);
//                    JoinedRow outputRow = new JoinedRow(outer, inner);
//                    result.addRowToTable(outputRow);
//                }
//
//                counter++;
//            }
//
//        }
//
//        after = System.currentTimeMillis();
//        System.out.println("Joining: " + (after - before));
//
//        result.close();
//        return result.getTable();


    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected StreamableFunction getStreamableFunction() {
        // TODO Auto-generated method stub
        return null;
    }



}
