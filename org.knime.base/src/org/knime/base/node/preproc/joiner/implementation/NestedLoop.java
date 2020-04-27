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
package org.knime.base.node.preproc.joiner.implementation;

import java.util.Comparator;
import java.util.HashSet;

import org.knime.base.data.sort.SortedTable;
import org.knime.base.node.preproc.joiner.Joiner2Settings;
import org.knime.base.node.preproc.joiner.Joiner2Settings.CompositionMode;
import org.knime.base.node.preproc.joiner.implementation.OutputRow.Settings;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;

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
public class NestedLoop extends AbstractJoiner {

    /**
     * @param leftTableSpec
     * @param rightTableSpec
     * @param settings
     */
    public NestedLoop(final DataTableSpec leftTableSpec, final DataTableSpec rightTableSpec, final Joiner2Settings settings) {
        super(leftTableSpec, rightTableSpec, settings);
        // TODO Auto-generated constructor stub
    }

    /**
     * Joins the <code>leftTable</code> and the <code>rightTable</code>.
     *
     * @param joiner TODO
     * @param leftTable The left input table.
     * @param rightTable The right input table.
     * @param exec The Execution monitor for this execution.
     * @return The joined table.
     * @throws CanceledExecutionException when execution is canceled
     * @throws InvalidSettingsException when inconsistent settings are provided
     */
    @Override
    public BufferedDataTable computeJoinTable(final BufferedDataTable leftTable, final BufferedDataTable rightTable, final ExecutionContext exec)
    throws CanceledExecutionException, InvalidSettingsException {

        m_runtimeWarnings.clear();
        m_leftRowKeyMap.clear();
        m_rightRowKeyMap.clear();

        // This does some input data checking, too
        DataTableSpec joinedTableSpec = createSpec(new DataTableSpec[] {
                leftTable.getDataTableSpec(),
                rightTable.getDataTableSpec()});

        BufferedDataTable outerTable = rightTable;
        BufferedDataTable innerTable = leftTable;

        //  TODO split logic to a disjunctive part. if multipleMatchCanOccur is true, to rows can be match more than
        // once. This is in general met with the MatchAny Option but only if
        // there are more than one join column.
        m_matchAny = m_settings.getCompositionMode()
            .equals(CompositionMode.MatchAny)
            && m_settings.getLeftJoinColumns().length > 1;
           // TODO split logic with outer joins?
        if (m_retainLeft && m_matchAny) {
            m_globalLeftOuterJoins = new HashSet<Integer>();
            for (int i = 0; i < leftTable.getRowCount(); i++) {
                m_globalLeftOuterJoins.add(i);
            }
        }

        /* m_joiningIndices HashMap<K,V>  (id=235)
         * {Right=[-1], Left=[-1]}
         * m_matchAny   false
         */
        m_inputDataRowSettings = createInputDataRowSettings(leftTable,
                rightTable);
        /*
         * [0, 1, 2, 3, 4]
         */
        int[] rightSurvivors = getIndicesOf(rightTable, m_rightSurvivors);
        /* rightTableSurvivors: {
         *  m_spec:
         *      name=default,columns=[0; 1; 2; 3; 4; 5; 6; 7];
         *  m_rightTableSurvivors:
         *      [0, 1, 2, 3, 4]}
         */
        m_outputDataRowSettings = new Settings(
                rightTable.getDataTableSpec(),
                rightSurvivors);

        // stores rows of the composite table
        JoinContainer joinCont = new JoinContainer(
                m_outputDataRowSettings);

        double[] progressIntervals = new double[] {0.6, 0.2, 0.2};
        exec.setProgress(0.0);
        performJoin(innerTable, outerTable,
                joinCont, exec, progressIntervals[0]);


        if (m_retainLeft && m_matchAny) {
            // Add left outer joins
            int c = 0;
            for (Integer index : m_globalLeftOuterJoins) {
                DataRow outRow = OutputRow.createDataRow(c, index, -1,
                        m_outputDataRowSettings);
                joinCont.addLeftOuter(outRow, exec);
                c++;
            }
        }
        joinCont.close();

        // numbers are needed to report progress more precisely
        long totalNumJoins = joinCont.getRowCount();
        long numMatches = null != joinCont.getMatches() ? joinCont.getMatches().size() : 0;
        long numLeftOuter = null != joinCont.getLeftOuter() ? joinCont.getLeftOuter().size() : 0;
        long numRightOuter = null != joinCont.getRightOuter() ? joinCont.getRightOuter().size() : 0;

        exec.setMessage("Sort Joined Partitions");
        Comparator<DataRow> joinComp = OutputRow.createRowComparator();
        SortedTable matches = null != joinCont.getMatches()
        ? new SortedTable(joinCont.getMatches(), joinComp, false,
                exec.createSubExecutionContext(
                        progressIntervals[1] * numMatches / totalNumJoins))
        : null;
        SortedTable leftOuter = null != joinCont.getLeftOuter()
        ? new SortedTable(joinCont.getLeftOuter(), joinComp, false,
                exec.createSubExecutionContext(
                        progressIntervals[1] * numLeftOuter / totalNumJoins))
        : null;
        SortedTable rightOuter = null != joinCont.getRightOuter()
        ? new SortedTable(joinCont.getRightOuter(), joinComp, false,
                exec.createSubExecutionContext(
                        progressIntervals[1] * numRightOuter / totalNumJoins))
        : null;

        exec.setMessage("Merge Joined Partitions");
        // Build sorted table
        int[] leftSurvivors = getIndicesOf(leftTable, m_leftSurvivors);

        DataHiliteOutputContainer oc =
            new DataHiliteOutputContainer(joinedTableSpec,
                    m_settings.getEnableHiLite(), leftTable,
                    leftSurvivors, rightSurvivors,
                    createRowKeyFactory(leftTable, rightTable));
        oc.addTableAndFilterDuplicates(matches,
                exec.createSubExecutionContext(
                        progressIntervals[2] * numMatches / totalNumJoins));
        oc.addTableAndFilterDuplicates(leftOuter,
                exec.createSubExecutionContext(
                        progressIntervals[2] * numLeftOuter / totalNumJoins));
        oc.addTableAndFilterDuplicates(rightOuter,
                exec.createSubExecutionContext(
                        progressIntervals[2] * numRightOuter / totalNumJoins));
        oc.close();

        m_leftRowKeyMap = oc.getLeftRowKeyMap();
        m_rightRowKeyMap = oc.getRightRowKeyMap();

        return oc.getTable();
    }

    /** This method start with reading the partitions of the left table defined
     * in currParts. If memory is low, partitions will be skipped or the
     * number of partitions will be raised which leads to smaller partitions.
     * Successfully read partitions will be joined. The return collection
     * defines the successfully processed partitions.
     *
     * @param leftTable The inner input table.
     * @param rightTable The right input table.
     * @param outputContainer The container used for storing matches.
     * @param pendingParts The parts that are not processed yet.
     * @param exec The execution context.
     * @param progressDiff The difference in the progress monitor.
     * @return The partitions that were successfully processed (read + joined).
     * @throws CanceledExecutionException when execution is canceled
     */
    JoinContainer performJoin(
            final BufferedDataTable leftTable,
            final BufferedDataTable rightTable,
            final JoinContainer outputContainer,
            final ExecutionContext exec,
            final double progressDiff) throws CanceledExecutionException  {
        // Update increment for reporting progress
        double progress = 0;
        double numPairs = leftTable.size() * rightTable.size();
        double inc = 1./numPairs;

        int counter = 0;
        for (DataRow left : leftTable) {

            InputRow leftRow = new InputRow(left, counter,
                InputRow.Settings.InDataPort.Left,
                m_inputDataRowSettings);

            for (DataRow right : rightTable) {
                progress += inc;
                exec.getProgressMonitor().setProgress(progress);
                exec.checkCanceled();

                InputRow rightRow = new InputRow(right, counter,
                    InputRow.Settings.InDataPort.Right,
                    m_inputDataRowSettings);

                if(rightRow.getJoinTuples()[0].equals(leftRow.getJoinTuples()[0])) {

                    DataRow outRow = OutputRow.createDataRow(
                        outputContainer.getRowCount(),
                        leftRow.getIndex(), rightRow.getIndex(),
                        right,
                        m_outputDataRowSettings);
                    outputContainer.addMatch(outRow, exec);
                }


            }

            counter++;
        }

        return outputContainer;
    }


}
