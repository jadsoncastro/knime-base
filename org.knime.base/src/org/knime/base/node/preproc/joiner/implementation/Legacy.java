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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.knime.base.data.sort.SortedTable;
import org.knime.base.node.preproc.joiner.Joiner2Settings;
import org.knime.base.node.preproc.joiner.Joiner2Settings.CompositionMode;
import org.knime.base.node.preproc.joiner.Joiner2Settings.DuplicateHandling;
import org.knime.base.node.preproc.joiner.Joiner2Settings.JoinMode;
import org.knime.base.node.preproc.joiner.implementation.OutputRow.Settings;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.util.memory.MemoryAlertSystem;
import org.knime.core.data.util.memory.MemoryAlertSystem.MemoryActionIndicator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;

/**
 *
 * Moved here by Carl Witt.
 *
 * @author Heiko Hofer
 *
 */
public class Legacy extends AbstractJoiner {

    /**
     * @param leftTableSpec
     * @param rightTableSpec
     * @param settings
     */
    public Legacy(final DataTableSpec leftTableSpec, final DataTableSpec rightTableSpec,
        final Joiner2Settings settings) {
        super(leftTableSpec, rightTableSpec, settings);
    }

    int m_bitMask;

    /**
     * Bits per partition.
     */
    int m_numBits;

    /**
     * The initial number of partitions the rows are read in. If not all partitions fit in main memory, they are joined
     * subsequently using as much memory as possible.
     */
    int m_numBitsInitial = 6;

    /** The maximal number of partitions (changed in testing routines). */
    int m_numBitsMaximal = Integer.SIZE;

    /**
     * {@inheritDoc}
     */
    @Override
    public BufferedDataTable computeJoinTable(final BufferedDataTable leftTable,
        final BufferedDataTable rightTable, final ExecutionContext exec)
        throws CanceledExecutionException, InvalidSettingsException {

        m_runtimeWarnings.clear();
        m_leftRowKeyMap.clear();
        m_rightRowKeyMap.clear();

        // This does some input data checking, too
        DataTableSpec joinedTableSpec =
            createSpec(new DataTableSpec[]{leftTable.getDataTableSpec(), rightTable.getDataTableSpec()});

        if (m_settings.getDuplicateHandling().equals(DuplicateHandling.Filter)) {
            List<String> leftCols = getLeftIncluded(leftTable.getDataTableSpec());
            List<String> rightCols = getRightIncluded(rightTable.getDataTableSpec());
            List<String> duplicates = new ArrayList<String>();
            duplicates.addAll(leftCols);
            duplicates.retainAll(rightCols);
            // Check if duplicated columns have identical data
            compareDuplicates(leftTable, rightTable, duplicates);
        }

        BufferedDataTable outerTable = rightTable;
        BufferedDataTable innerTable = leftTable;

        m_retainRight = JoinMode.RightOuterJoin.equals(m_settings.getJoinMode())
            || JoinMode.FullOuterJoin.equals(m_settings.getJoinMode());
        m_retainLeft = JoinMode.LeftOuterJoin.equals(m_settings.getJoinMode())
            || JoinMode.FullOuterJoin.equals(m_settings.getJoinMode());

        // if multipleMatchCanOccur is true, to rows can be match more than
        // once. This is in general met with the MatchAny Option but only if
        // there are more than one join column.
        m_matchAny = m_settings.getCompositionMode().equals(CompositionMode.MatchAny)
            && m_settings.getLeftJoinColumns().length > 1;

        if (m_retainLeft && m_matchAny) {
            m_globalLeftOuterJoins = new HashSet<Integer>();
            for (int i = 0; i < leftTable.getRowCount(); i++) {
                m_globalLeftOuterJoins.add(i);
            }
        }

        m_inputDataRowSettings = createInputDataRowSettings(leftTable, rightTable);
        int[] rightSurvivors = getIndicesOf(rightTable, m_rightSurvivors);
        /* rightTableSurvivors: {
         *  m_spec:
         *      name=default,columns=[0; 1; 2; 3; 4; 5; 6; 7];
         *  m_rightTableSurvivors:
         *      [0, 1, 2, 3, 4]}
         */
        m_outputDataRowSettings = new Settings(rightTable.getDataTableSpec(), rightSurvivors);

        /* numBits -> numPartitions
         * 0 -> 1
         * 1 -> 2
         * 2 -> 4
         * 3 -> 8
         * 4 -> 16
         * 5 -> 32
         * 6 -> 64
         * 7 -> 128
         */
        m_numBits = m_numBitsInitial;
        int numPartitions = 0x0001 << m_numBits;
        m_bitMask = 0;
        /*
         * Set the m_numBits lowest order bits to true
         * is just 2**numBits - 1
         */
        for (int i = 0; i < m_numBits; i++) {
            m_bitMask += 0x0001 << i;
        }

        Set<Integer> pendingParts = new TreeSet<Integer>();
        for (int i = 0; i < numPartitions; i++) {
            pendingParts.add(i);
        }

        JoinContainer joinCont = new JoinContainer(m_outputDataRowSettings);

        double[] progressIntervals = new double[]{0.6, 0.2, 0.2};
        exec.setProgress(0.0);
        while (pendingParts.size() > 0) {
            Collection<Integer> processedParts =
                performJoin(innerTable, outerTable, joinCont, pendingParts, exec, progressIntervals[0]);
            pendingParts.removeAll(processedParts);
        }

        if (m_retainLeft && m_matchAny) {
            // Add left outer joins
            int c = 0;
            for (Integer index : m_globalLeftOuterJoins) {
                DataRow outRow = OutputRow.createDataRow(c, index, -1, m_outputDataRowSettings);
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
        SortedTable matches = null != joinCont.getMatches() ? new SortedTable(joinCont.getMatches(), joinComp, false,
            exec.createSubExecutionContext(progressIntervals[1] * numMatches / totalNumJoins)) : null;
        SortedTable leftOuter = null != joinCont.getLeftOuter() ? new SortedTable(joinCont.getLeftOuter(), joinComp,
            false, exec.createSubExecutionContext(progressIntervals[1] * numLeftOuter / totalNumJoins)) : null;
        SortedTable rightOuter = null != joinCont.getRightOuter() ? new SortedTable(joinCont.getRightOuter(), joinComp,
            false, exec.createSubExecutionContext(progressIntervals[1] * numRightOuter / totalNumJoins)) : null;

        exec.setMessage("Merge Joined Partitions");
        // Build sorted table
        int[] leftSurvivors = getIndicesOf(leftTable, m_leftSurvivors);

        DataHiliteOutputContainer oc = new DataHiliteOutputContainer(joinedTableSpec, m_settings.getEnableHiLite(),
            leftTable, leftSurvivors, rightSurvivors, createRowKeyFactory(leftTable, rightTable));
        oc.addTableAndFilterDuplicates(matches,
            exec.createSubExecutionContext(progressIntervals[2] * numMatches / totalNumJoins));
        oc.addTableAndFilterDuplicates(leftOuter,
            exec.createSubExecutionContext(progressIntervals[2] * numLeftOuter / totalNumJoins));
        oc.addTableAndFilterDuplicates(rightOuter,
            exec.createSubExecutionContext(progressIntervals[2] * numRightOuter / totalNumJoins));
        oc.close();

        m_leftRowKeyMap = oc.getLeftRowKeyMap();
        m_rightRowKeyMap = oc.getRightRowKeyMap();

        return oc.getTable();
    }

    /**
     * Join given rows in memory and append joined row to the outputCont.
     *
     * @param leftTableHashed Stores the rows of the left input table in parts.
     * @param leftOuterJoins The same number as found in leftTableHashed used for left outer joins.
     * @param currParts The parts of the outer table that will be joined.
     * @param rightTable The outer table.
     * @param outputCont The joined rows will be added to this container.
     * @param exec The {@link ExecutionContext}
     * @param incProgress The progress increment.
     * @throws CanceledExecutionException When execution is canceled
     */
    private void joinInMemory(final Map<Integer, Map<JoinTuple, Set<Integer>>> leftTableHashed,
        final Map<Integer, Set<Integer>> leftOuterJoins, final Collection<Integer> currParts,
        final BufferedDataTable rightTable, final JoinContainer outputCont, final ExecutionContext exec,
        final double incProgress) throws CanceledExecutionException {
        double progress = exec.getProgressMonitor().getProgress();
        int counter = 0;
        for (DataRow dataRow : rightTable) {
            progress += incProgress;
            exec.getProgressMonitor().setProgress(progress);
            exec.checkCanceled();

            InputRow rightRow =
                new InputRow(dataRow, counter, InputRow.Settings.InDataPort.Right, m_inputDataRowSettings);

            boolean matchFoundForRightRow = false;
            boolean deferMatch = false;

            for (JoinTuple joinTuple : rightRow.getJoinTuples()) {
                int partition = joinTuple.hashCode() & m_bitMask;
                if (!currParts.contains(partition)) {
                    deferMatch = true;
                    // skip and defer non-match when partition is not in the current partitions
                    continue;
                }

                Map<JoinTuple, Set<Integer>> leftTuples = leftTableHashed.get(partition);
                if (null == leftTuples) {
                    // skip and check for outer join when the left table does not have rows that fall
                    // in this partition
                    deferMatch = false;
                    continue;
                }

                Set<Integer> localLeftOuterJoins = null;
                if (m_retainLeft && !m_matchAny) {
                    localLeftOuterJoins = leftOuterJoins.get(partition);
                }

                Set<Integer> leftRows = leftTuples.get(joinTuple);
                if (null != leftRows) {
                    matchFoundForRightRow = true;
                    for (Integer leftRowIndex : leftRows) {
                        // add inner join
                        DataRow outRow = OutputRow.createDataRow(outputCont.getRowCount(), leftRowIndex,
                            rightRow.getIndex(), dataRow, m_outputDataRowSettings);
                        outputCont.addMatch(outRow, exec);
                        if (m_retainLeft && !m_matchAny) {
                            localLeftOuterJoins.remove(leftRowIndex);
                        }
                        if (m_retainLeft && m_matchAny) {
                            m_globalLeftOuterJoins.remove(leftRowIndex);
                        }
                    }
                }
            }

            if (m_retainRight && !matchFoundForRightRow && !deferMatch) {
                long outRowIndex = outputCont.getRowCount();
                // add right outer join
                DataRow outRow =
                    OutputRow.createDataRow(outRowIndex, -1, rightRow.getIndex(), dataRow, m_outputDataRowSettings);
                outputCont.addRightOuter(outRow, exec);
            }
            counter++;
        }

        if (m_retainLeft && !m_matchAny) {
            for (int partition : leftOuterJoins.keySet()) {
                for (Integer row : leftOuterJoins.get(partition)) {
                    // add left outer join
                    DataRow outRow =
                        OutputRow.createDataRow(outputCont.getRowCount(), row, -1, m_outputDataRowSettings);
                    outputCont.addLeftOuter(outRow, exec);
                }
            }
        }

    }

    /**
     * This method start with reading the partitions of the left table defined in currParts. If memory is low,
     * partitions will be skipped or the number of partitions will be raised which leads to smaller partitions.
     * Successfully read partitions will be joined. The return collection defines the successfully processed partitions.
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
    Collection<Integer> performJoin(final BufferedDataTable leftTable, final BufferedDataTable rightTable,
        final JoinContainer outputContainer, final Collection<Integer> pendingParts, final ExecutionContext exec,
        final double progressDiff) throws CanceledExecutionException {
        // Update increment for reporting progress
        double progress = exec.getProgressMonitor().getProgress();
        double numRows = leftTable.size() + rightTable.size();
        double inc = (progressDiff - progress) / numRows;

        Collection<Integer> currParts = new ArrayList<Integer>();
        currParts.addAll(pendingParts);
        setMessage("Read", exec, pendingParts, currParts);

        // Partition left table
        Map<Integer, Map<JoinTuple, Set<Integer>>> leftTableHashed =
            new HashMap<Integer, Map<JoinTuple, Set<Integer>>>();
        // This is only used when m_leftRetain is true and m_matchAny is false.
        // It holds the row indices of the left table that do not match to
        // any row of the right table
        Map<Integer, Set<Integer>> leftOuterJoins = new HashMap<Integer, Set<Integer>>();

        MemoryActionIndicator memIndicator = MemoryAlertSystem.getInstance().newIndicator();

        int counter = 0;
        long rowsAdded = 0;
        CloseableRowIterator leftIter = leftTable.iterator();
        while (leftIter.hasNext()) {
            exec.checkCanceled();
            boolean saveToAddMoreRows = !memIndicator.lowMemoryActionRequired() && ((m_rowsAddedBeforeForcedOOM == 0)
                || (rowsAdded % m_rowsAddedBeforeForcedOOM != (m_rowsAddedBeforeForcedOOM - 1)));

            if (saveToAddMoreRows) {
                DataRow row = leftIter.next();
                InputRow inputDataRow =
                    new InputRow(row, counter, InputRow.Settings.InDataPort.Left, m_inputDataRowSettings);

                for (JoinTuple tuple : inputDataRow.getJoinTuples()) {
                    int partition = tuple.hashCode() & m_bitMask;
                    if (currParts.contains(partition)) {
                        addRow(leftTableHashed, leftOuterJoins, partition, tuple, inputDataRow);
                        rowsAdded++;
                    }
                }
                counter++;
                // report progress
                progress += inc;
                exec.getProgressMonitor().setProgress(progress);
            } else {
                rowsAdded++;

                // Build list of partitions that are not empty
                List<Integer> nonEmptyPartitions = new ArrayList<Integer>();
                for (Integer i : currParts) {
                    if (null != leftTableHashed.get(i)) {
                        nonEmptyPartitions.add(i);
                    }
                }
                int numNonEmpty = nonEmptyPartitions.size();
                if (numNonEmpty > 1) {
                    // remove input partitions to free memory
                    List<Integer> removeParts = new ArrayList<Integer>();
                    for (int i = 0; i < numNonEmpty / 2; i++) {
                        removeParts.add(nonEmptyPartitions.get(i));
                    }
                    // remove collected data of the no longer processed
                    for (int i : removeParts) {
                        leftTableHashed.remove(i);
                        if (m_retainLeft && !m_matchAny) {
                            leftOuterJoins.remove(i);
                        }
                    }
                    currParts.removeAll(removeParts);
                    AbstractJoiner.LOGGER.debug("Skip partitions while " + "reading inner table. Currently Processed: "
                        + currParts + ". Skip: " + removeParts);
                    // update increment for reporting progress
                    numRows += leftTable.size() + rightTable.size();
                    inc = (progressDiff - progress) / numRows;

                    setMessage("Read", exec, pendingParts, currParts);
                } else if (nonEmptyPartitions.size() == 1) {
                    if (m_numBits < m_numBitsMaximal) {
                        AbstractJoiner.LOGGER.debug("Increase number of partitions while " + "reading inner table. Currently "
                            + "Processed: " + nonEmptyPartitions);

                        // increase number of partitions
                        m_numBits = m_numBits + 1;
                        m_bitMask = m_bitMask | (0x0001 << (m_numBits - 1));
                        Set<Integer> pending = new TreeSet<Integer>();
                        pending.addAll(pendingParts);
                        pendingParts.clear();
                        for (int i : pending) {
                            pendingParts.add(i);
                            int ii = i | (0x0001 << (m_numBits - 1));
                            pendingParts.add(ii);
                        }

                        int currPart = nonEmptyPartitions.iterator().next();
                        currParts.clear();
                        currParts.add(currPart);
                        // update chunk size
                        retainPartitions(leftTableHashed, leftOuterJoins, currPart);
                        // update increment for reporting progress
                        numRows += leftTable.size() + rightTable.size();
                        inc = (progressDiff - progress) / numRows;

                        setMessage("Read", exec, pendingParts, currParts);
                    } else {
                        // We have now 2^32 partitions.
                        // We can only keep going and hope that other nodes
                        // may free some memory.
                        AbstractJoiner.LOGGER.warn("Memory is low. " + "I have no chance to free memory. This may "
                            + "cause an endless loop.");
                    }
                } else if (nonEmptyPartitions.size() < 1) {
                    // We have only empty partitions.
                    // Other node consume to much memory,
                    // we cannot free more memory
                    AbstractJoiner.LOGGER.warn(
                        "Memory is low. " + "I have no chance to free memory. This may " + "cause an endless loop.");
                }
            }
        }

        setMessage("Join", exec, pendingParts, currParts);
        // Join with outer table
        joinInMemory(leftTableHashed, leftOuterJoins, currParts, rightTable, outputContainer, exec, inc);

        // Log which parts were successfully joined
        for (int part : currParts) {
            int numTuples = leftTableHashed.get(part) != null ? leftTableHashed.get(part).values().size() : 0;
            AbstractJoiner.LOGGER.debug("Joined " + part + " with " + numTuples + " tuples.");
        }

        // Garbage collector has problems without this explicit clearance.
        leftTableHashed.clear();
        leftOuterJoins.clear();

        // return successfully joined parts
        return currParts;
    }

    /**
     * Called when the number of partitions is doubled. The innerHash is traversed and only those entries that are in
     * the given part are retained. The innerIndexMap is build up so that it contains only the entries that are in the
     * given part.
     *
     * @param joiner TODO
     * @param innerHash TODO
     * @param innerIndexMap TODO
     * @param part TODO
     */
    void retainPartitions(final Map<Integer, Map<JoinTuple, Set<Integer>>> innerHash,
        final Map<Integer, Set<Integer>> innerIndexMap, final int part) {
        innerIndexMap.clear();

        Map<JoinTuple, Set<Integer>> thisInnerHash = innerHash.get(part);
        for (Iterator<JoinTuple> iter = thisInnerHash.keySet().iterator(); iter.hasNext();) {
            JoinTuple tuple = iter.next();
            int index = tuple.hashCode() & m_bitMask;
            if (index != part) {
                iter.remove();
            } else if (m_retainLeft && !m_matchAny) {
                Set<Integer> thisInnerIndexMap = innerIndexMap.get(index);
                if (null == thisInnerIndexMap) {
                    thisInnerIndexMap = new HashSet<Integer>();
                    innerIndexMap.put(index, thisInnerIndexMap);
                }
                for (Integer rowIndex : thisInnerHash.get(tuple)) {
                    thisInnerIndexMap.add(rowIndex);
                }
            }
        }
    }

    /**
     * Add a row to the index data structure mapping partition ids to partition indices. Each partition index maps a
     * specific combination of values in the join columns to the set of row indices having that values in their join
     * columns. It's essentially a group-by.
     *
     * @param partition The index of the partition.
     * @param joinTuple The join tuples of the row.
     * @param row The row to be added.
     */
    void addRow(final Map<Integer, Map<JoinTuple, Set<Integer>>> leftTableHashed,
        final Map<Integer, Set<Integer>> leftOuterJoins, final int partition, final JoinTuple joinTuple,
        final InputRow row) {
        if (m_retainLeft && !m_matchAny) {
            Set<Integer> indices = leftOuterJoins.get(partition);
            if (null == indices) {
                indices = new HashSet<Integer>();
                leftOuterJoins.put(partition, indices);
            }
            indices.add(row.getIndex());
        }

        Map<JoinTuple, Set<Integer>> partTuples = leftTableHashed.get(partition);
        if (null == partTuples) {
            partTuples = new HashMap<JoinTuple, Set<Integer>>();
            leftTableHashed.put(partition, partTuples);
        }

        Set<Integer> c = partTuples.get(joinTuple);
        if (null != c) {
            c.add(row.getIndex());
        } else {
            Set<Integer> list = new HashSet<Integer>();
            list.add(row.getIndex());
            partTuples.put(joinTuple, list);
        }

    }


}
