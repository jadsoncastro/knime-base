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
 *   23.11.2009 (Heiko Hofer): created
 */
package org.knime.base.node.preproc.joiner3.implementation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.DuplicateHandling;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.JoinMode;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.util.ConvenienceMethods;
import org.knime.core.util.UniqueNameGenerator;

/**
 * The joiner implements a database like join of two tables.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 * @author Heiko Hofer
 *
 */
public abstract class AbstractJoiner implements Joiner {
    /** Logger to print debug info to. */
    static final NodeLogger LOGGER = NodeLogger
    .getLogger(AbstractJoiner.class);

    private final DataTableSpec m_leftDataTableSpec;
    private final DataTableSpec m_rightDataTableSpec;

    final Joiner3Settings m_settings;

    /** True for Left Outer Join and Full Outer Join. */
    boolean m_retainLeft;
    /** True for Right Outer Join and Full Outer Join. */
    boolean m_retainRight;

    /**
     * True when in the dialog to option 'Match any of the following' is
     * selected and when there are more than two joining columns.
     */
    boolean m_matchAny;

    /**
     * This field is only used when (m_retainLeft && m_matchAny) is true. It
     * holds the row indices of the left table that did not match to a row
     * of the right table.
     */
    Set<Integer> m_globalLeftOuterJoins;

    InputRow.Settings m_inputDataRowSettings;
    OutputRow.Settings m_outputDataRowSettings;


    HashMap<RowKey, Set<RowKey>> m_leftRowKeyMap;
    HashMap<RowKey, Set<RowKey>> m_rightRowKeyMap;

    /**
     * The names of the columns
     */
    List<String> m_leftSurvivors;
    List<String> m_rightSurvivors;

    private final List<String> m_configWarnings;
    final List<String> m_runtimeWarnings;

    /** Only used for testcases, simulates an out-of-memory event after that many rows added in memory. */
    int m_rowsAddedBeforeForcedOOM;

    /**
     *
     */
    protected AbstractJoiner(final Joiner3Settings settings,final BufferedDataTable outer, final BufferedDataTable...innerTables) {
        m_leftDataTableSpec = outer.getDataTableSpec();
        //FIXME more than one inner table
        m_rightDataTableSpec = innerTables[0].getDataTableSpec();
        m_settings = settings;

        m_leftRowKeyMap = new HashMap<RowKey, Set<RowKey>>();
        m_rightRowKeyMap = new HashMap<RowKey, Set<RowKey>>();

        m_configWarnings = new ArrayList<String>();
        m_runtimeWarnings = new ArrayList<String>();

    }

    /**
     * Creates a spec for the output table by taking care of duplicate columns.
     *
     * @param specs the specs of the two input tables
     * @return the spec of the output table
     * @throws InvalidSettingsException when settings are not supported
     */
    DataTableSpec createSpec(final DataTableSpec[] specs)
    throws InvalidSettingsException {
        validateSettings(m_settings);

        m_configWarnings.clear();
        List<String> leftCols = getLeftIncluded(specs[0]);
        List<String> rightCols = getRightIncluded(specs[1]);

        List<String> duplicates = new ArrayList<String>();
        duplicates.addAll(leftCols);
        duplicates.retainAll(rightCols);

        if (m_settings.getDuplicateHandling().equals(
                DuplicateHandling.DontExecute)
                && !duplicates.isEmpty()) {
            throw new InvalidSettingsException(
                    "Found duplicate columns, won't execute. Fix it in "
                    + "\"Column Selection\" tab");
        }

        if (m_settings.getDuplicateHandling().equals(
                DuplicateHandling.Filter)) {

            for (String duplicate : duplicates) {
                DataType leftType = specs[0].getColumnSpec(duplicate).getType();
                DataType rightType =
                    specs[1].getColumnSpec(duplicate).getType();
                if (!leftType.equals(rightType)) {
                    m_configWarnings.add("The column \"" + duplicate
                            + "\" can be found in "
                            + "both input tables but with different data type. "
                            + "Only the one in the top input table will show "
                            + "up in the output table. Please change the "
                            + "Duplicate Column Handling if both columns "
                            + "should show up in the output table.");
                }
            }

            rightCols.removeAll(leftCols);
        }

        if ((!duplicates.isEmpty()) && m_settings.getDuplicateHandling().equals(DuplicateHandling.AppendSuffix)
                && (m_settings.getDuplicateColumnSuffix() == null
                || m_settings.getDuplicateColumnSuffix().equals(""))) {
            throw new InvalidSettingsException("No suffix for duplicate columns provided.");
        }

        // check if data types of joining columns do match
        for (int i = 0; i < m_settings.getLeftJoinColumns().length; i++) {
            String leftJoinAttr = m_settings.getLeftJoinColumns()[i];
            boolean leftJoinAttrIsRowKey =
                Joiner3Settings.ROW_KEY_IDENTIFIER.equals(leftJoinAttr);
            DataType leftType = leftJoinAttrIsRowKey
            ? StringCell.TYPE
                    : specs[0].getColumnSpec(leftJoinAttr).getType();
            String rightJoinAttr = m_settings.getRightJoinColumns()[i];
            boolean rightJoinAttrIsRowKey =
                Joiner3Settings.ROW_KEY_IDENTIFIER.equals(rightJoinAttr);
            DataType rightType = rightJoinAttrIsRowKey
            ? StringCell.TYPE
                    : specs[1].getColumnSpec(rightJoinAttr).getType();
            if (!leftType.equals(rightType)) {
                String left = leftJoinAttrIsRowKey ? "Row ID" : leftJoinAttr;
                String right = rightJoinAttrIsRowKey ? "Row ID" : rightJoinAttr;
                // check different cases here to give meaningful error messages
                if (leftType.equals(DoubleCell.TYPE)
                        && rightType.equals(IntCell.TYPE)) {
                    throw new InvalidSettingsException("Type mismatch found of "
                            + "Joining Column Pair \""
                            + left + "\" and \"" + right + "\"."
                            + " Use \"Double to Int node\" to "
                            + "convert the type of \""
                            + left + "\" to integer.");
                } else if (leftType.equals(IntCell.TYPE)
                        && rightType.equals(DoubleCell.TYPE)) {
                    throw new InvalidSettingsException("Type mismatch found of "
                            + "Joining Column Pair \""
                            + left + "\" and \"" + right + "\"."
                            + " se \"Double to Int node\" to "
                            + "convert the type of \""
                            + right + "\" to integer.");
                } else if (leftType.isCompatible(DoubleValue.class)
                        && rightType.equals(StringCell.TYPE)) {
                    throw new InvalidSettingsException("Type mismatch found of "
                            + "Joining Column Pair \""
                            + left + "\" and \"" + right + "\"."
                            + " Use \"Number to String node\" to "
                            + "convert the type of \""
                            + left + "\" to string.");
                } else if (leftType.equals(StringCell.TYPE)
                        && rightType.isCompatible(DoubleValue.class)) {
                    throw new InvalidSettingsException("Type mismatch found of "
                            + "Joining Column Pair \""
                            + left + "\" and \"" + right + "\"."
                            + " Use \"Number to String node\" to "
                            + "convert the type of \""
                            + right + "\" to string.");
                } else if (leftType.getPreferredValueClass() != rightType.getPreferredValueClass()) {
                    // if both don't have the same preferred class they can't be equals, see DataCell#equals

                    throw new InvalidSettingsException("Type mismatch found of "
                            + "Joining Column Pair \""
                            + left + "\" and \"" + right + "\"."
                            + "This causes an empty output table.");
                }
            }
        }

        @SuppressWarnings("unchecked")
        UniqueNameGenerator nameGen = new UniqueNameGenerator(
            Collections.EMPTY_SET);
        m_leftSurvivors = new ArrayList<String>();
        List<DataColumnSpec> outColSpecs = new ArrayList<DataColumnSpec>();
        for (int i = 0; i < specs[0].getNumColumns(); i++) {
            DataColumnSpec columnSpec = specs[0].getColumnSpec(i);
            if (leftCols.contains(columnSpec.getName())) {
                outColSpecs.add(columnSpec);
                nameGen.newName(columnSpec.getName());
                m_leftSurvivors.add(columnSpec.getName());
            }
        }

        m_rightSurvivors = new ArrayList<String>();
        for (int i = 0; i < specs[1].getNumColumns(); i++) {
            DataColumnSpec columnSpec = specs[1].getColumnSpec(i);
            if (rightCols.contains(columnSpec.getName())) {
                if (m_settings.getDuplicateHandling().equals(DuplicateHandling.AppendSuffix)) {
                    if (m_leftSurvivors.contains(columnSpec.getName())
                            || m_rightSurvivors.contains(columnSpec.getName())) {
                        String newName = columnSpec.getName();
                        do {
                            newName += m_settings.getDuplicateColumnSuffix();
                        } while (m_leftSurvivors.contains(newName)
                                || m_rightSurvivors.contains(newName));

                        DataColumnSpecCreator dcsc =
                            new DataColumnSpecCreator(columnSpec);
                        dcsc.removeAllHandlers();
                        dcsc.setName(newName);
                        outColSpecs.add(dcsc.createSpec());
                        rightCols.add(newName);
                    } else {
                        outColSpecs.add(columnSpec);
                    }
                } else {
                    String newName = nameGen.newName(columnSpec.getName());
                    if (newName.equals(columnSpec.getName())) {
                        outColSpecs.add(columnSpec);
                    } else {
                        DataColumnSpecCreator dcsc =
                            new DataColumnSpecCreator(columnSpec);
                        dcsc.removeAllHandlers();
                        dcsc.setName(newName);
                        outColSpecs.add(dcsc.createSpec());
                    }

                }
                m_rightSurvivors.add(columnSpec.getName());
            }
        }

        return new DataTableSpec(outColSpecs.toArray(
                new DataColumnSpec[outColSpecs.size()]));
    }

    /**
     * @param dataTableSpec input spec of the left DataTable
     * @return the names of all columns to include from the left input table
     * @throws InvalidSettingsException if the input spec is not compatible with the settings
     * @since 2.12
     */
    List<String> getLeftIncluded(final DataTableSpec dataTableSpec)
    throws InvalidSettingsException {
        List<String> leftCols = new ArrayList<String>();
        for (DataColumnSpec column : dataTableSpec) {
            leftCols.add(column.getName());
        }
        // Check if left joining columns are in table spec
        Set<String> leftJoinCols = new HashSet<String>();
        leftJoinCols.addAll(Arrays.asList(m_settings.getLeftJoinColumns()));
        leftJoinCols.remove(Joiner3Settings.ROW_KEY_IDENTIFIER);
        if (!leftCols.containsAll(leftJoinCols)) {
            leftJoinCols.removeAll(leftCols);
            throw new InvalidSettingsException("The top input table has "
               + "changed. Some joining columns are missing: "
               + ConvenienceMethods.getShortStringFrom(leftJoinCols, 3));
        }

        if (!m_settings.getLeftIncludeAll()) {
            List<String> leftIncludes =
                Arrays.asList(m_settings.getLeftIncludeCols());
            leftCols.retainAll(leftIncludes);
        }
        if (m_settings.getRemoveLeftJoinCols()) {
            leftCols.removeAll(Arrays.asList(m_settings.getLeftJoinColumns()));
        }
        return leftCols;
    }

    /**
     * @param dataTableSpec input spec of the right DataTable
     * @return the names of all columns to include from the left input table
     * @throws InvalidSettingsException if the input spec is not compatible with the settings
     * @since 2.12
     */
    List<String> getRightIncluded(final DataTableSpec dataTableSpec)
    throws InvalidSettingsException {
        List<String> rightCols = new ArrayList<String>();
        for (DataColumnSpec column : dataTableSpec) {
            rightCols.add(column.getName());
        }
        // Check if right joining columns are in table spec
        Set<String> rightJoinCols = new HashSet<String>();
        rightJoinCols.addAll(Arrays.asList(m_settings.getRightJoinColumns()));
        rightJoinCols.remove(Joiner3Settings.ROW_KEY_IDENTIFIER);
        if (!rightCols.containsAll(rightJoinCols)) {
            rightJoinCols.removeAll(rightCols);
            throw new InvalidSettingsException("The bottom input table has "
                    + "changed. Some joining columns are missing: "
                    + ConvenienceMethods.getShortStringFrom(rightJoinCols, 3));
        }

        if (!m_settings.getRightIncludeAll()) {
            List<String> rightIncludes =
                Arrays.asList(m_settings.getRightIncludeCols());
            rightCols.retainAll(rightIncludes);
        }
        if (m_settings.getRemoveRightJoinCols()) {
            rightCols
            .removeAll(Arrays.asList(m_settings.getRightJoinColumns()));
        }
        return rightCols;
    }

    public DataTableSpec getOutputSpec() throws InvalidSettingsException {

        return createSpec(new DataTableSpec[]{m_leftDataTableSpec, m_rightDataTableSpec});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public abstract BufferedDataTable computeJoinTable(final BufferedDataTable leftTable,
        final BufferedDataTable rightTable, final ExecutionContext exec, Consumer<String> runtimeWarningHandler)
        throws CanceledExecutionException, InvalidSettingsException;

    /**
     * @param exec
     * @param pendingParts
     * @param currParts
     */
    void setMessage(final String activity,
            final ExecutionContext exec,
            final Collection<Integer> pendingParts,
            final Collection<Integer> currParts) {
        exec.setMessage(activity + " " + currParts.size()
                + " parts | Pending : "
                + (pendingParts.size() - currParts.size())
                + " parts | Total: "
                + " parts.");
    }


    private List<Integer> getLeftJoinIndices(
            final BufferedDataTable leftTable) {
        // Create list of indices for the joining columns (Element of the list
        // is -1 if RowKey should be joined).
        int numJoinAttributes = m_settings.getLeftJoinColumns().length;
        List<Integer> leftTableJoinIndices =
            new ArrayList<Integer>(numJoinAttributes);
        for (int i = 0; i < numJoinAttributes; i++) {
            String joinAttribute = m_settings.getLeftJoinColumns()[i];
            leftTableJoinIndices.add(
                    leftTable.getDataTableSpec().findColumnIndex(
                            joinAttribute));
        }
        return leftTableJoinIndices;
    }


    private List<Integer> getRightJoinIndices(
            final BufferedDataTable rightTable) {
        // Create list of indices for the joining columns (Element of the list
        // is -1 if RowKey should be joined).
        int numJoinAttributes = m_settings.getLeftJoinColumns().length;
        List<Integer> rightTableJoinIndices =
            new ArrayList<Integer>(numJoinAttributes);
        for (int i = 0; i < numJoinAttributes; i++) {
            String joinAttribute = m_settings.getRightJoinColumns()[i];
            rightTableJoinIndices.add(rightTable.getDataTableSpec()
                    .findColumnIndex(joinAttribute));
        }
        return rightTableJoinIndices;
    }

    JoinedRowKeyFactory createRowKeyFactory(
            final BufferedDataTable leftTable,
            final BufferedDataTable rightTable) {


        if (useSingleRowKeyFactory(leftTable, rightTable)) {
            // This is the special case of row key match row key
            return new UseSingleRowKeyFactory();
        } else {
            return new ConcatenateJoinedRowKeyFactory(
                    m_settings.getRowKeySeparator());
        }
    }

    /**
     * Gives true when the SingleRowKeyFactory should be used.
     */
    private boolean useSingleRowKeyFactory(
           final BufferedDataTable leftTable,
           final BufferedDataTable rightTable) {
        List<Integer> leftTableJoinIndices = getLeftJoinIndices(leftTable);
        List<Integer> rightTableJoinIndices = getRightJoinIndices(rightTable);

        boolean joinRowIdsOnly = true;
        boolean joinRowIds = false;
        for (int i = 0; i < leftTableJoinIndices.size(); i++) {
            joinRowIdsOnly = joinRowIdsOnly
                && leftTableJoinIndices.get(i) == -1
                && rightTableJoinIndices.get(i) == -1;
            joinRowIds = joinRowIds
                || (leftTableJoinIndices.get(i) == -1
                && rightTableJoinIndices.get(i) == -1);
        }
        return joinRowIdsOnly
            || (joinRowIds && !m_matchAny
                && m_settings.getJoinMode().equals(JoinMode.InnerJoin)
                && m_settings.useEnhancedRowIdHandling());
    }

    InputRow.Settings createInputDataRowSettings(
            final BufferedDataTable leftTable,
            final BufferedDataTable rightTable) {
        List<Integer> leftTableJoinIndices = getLeftJoinIndices(leftTable);
        List<Integer> rightTableJoinIndices = getRightJoinIndices(rightTable);



        // Build m_inputDataRowSettings
        Map<InputRow.Settings.InDataPort,
        List<Integer>> joiningIndicesMap =
            new HashMap<InputRow.Settings.InDataPort, List<Integer>>();
        joiningIndicesMap.put(InputRow.Settings.InDataPort.Left,
                leftTableJoinIndices);
        joiningIndicesMap.put(InputRow.Settings.InDataPort.Right,
                rightTableJoinIndices);


        InputRow.Settings inputDataRowSettings = new InputRow.Settings(
                joiningIndicesMap, m_matchAny);

        return inputDataRowSettings;
    }


    /**
     * Validates the settings in the passed <code>NodeSettings</code> object.
     * The specified settings is checked for completeness and
     * consistency.
     *
     * @param s The settings to validate.
     * @throws InvalidSettingsException If the validation of the settings
     *             failed.
     */
    public static void validateSettings(final Joiner3Settings s)
    throws InvalidSettingsException {
        if (s.getDuplicateHandling() == null) {
            throw new InvalidSettingsException(
                "No duplicate handling method selected");
        }
        if (s.getJoinMode() == null) {
            throw new InvalidSettingsException("No join mode selected");
        }
        if ((s.getLeftJoinColumns() == null)
                || s.getLeftJoinColumns().length < 1
                || s.getRightJoinColumns() == null
                || s.getRightJoinColumns().length < 1) {
            throw new InvalidSettingsException(
                "Please define at least one joining column pair.");
        }
        if (s.getLeftJoinColumns() != null
                && s.getRightJoinColumns() != null
                && s.getLeftJoinColumns().length
                != s.getRightJoinColumns().length) {
            throw new InvalidSettingsException(
                    "Number of columns selected from the top table and from "
                    + "the bottom table do not match");
        }

        if (s.getDuplicateHandling().equals(DuplicateHandling.AppendSuffix)
            && (s.getDuplicateColumnSuffix() == null
            || s.getDuplicateColumnSuffix().isEmpty())) {
            throw new InvalidSettingsException(
            "No suffix for duplicate columns provided");
        }
        if (s.getMaxOpenFiles() < 3) {
            throw new InvalidSettingsException(
            "Maximum number of open files must be at least 3.");
        }

    }

    /**
     * @param leftTable The left input table
     * @param rightTable The right input table
     * @param duplicates The list of columns to test for identity
     */
    void compareDuplicates(final BufferedDataTable leftTable,
            final BufferedDataTable rightTable, final List<String> duplicates) {

        int[] leftIndex = getIndicesOf(leftTable, duplicates);
        int[] rightIndex = getIndicesOf(rightTable, duplicates);

        String[] messages = new String[duplicates.size()];

        CloseableRowIterator leftIter = leftTable.iterator();
        CloseableRowIterator rightIter = rightTable.iterator();
        while (leftIter.hasNext()) {
            if (!rightIter.hasNext()) {
                // right table has less rows
                m_runtimeWarnings.add("Possible problem in configuration "
                        + "found. The \"Duplicate Column Handling\" is "
                        + "configured to  filter duplicates, but the "
                        + "duplicate columns are not equal since the "
                        + "top table has more elements than the bottom "
                        + "table.");
                break;
            }
            DataRow left = leftIter.next();
            DataRow right = rightIter.next();
            for (int i = 0; i < duplicates.size(); i++) {
                if (null == messages[i]
                                     && !left.getCell(leftIndex[i]).equals(
                                             right.getCell(rightIndex[i]))) {
                    // Two cells do not match
                    messages[i] = "The column \"" + duplicates.get(i)
                    + "\" can be found in "
                    + "both input tables but the content is not "
                    + "equal. "
                    + "Only the one in the top input table will show "
                    + "up in the output table. Please change the "
                    + "Duplicate Column Handling if both columns "
                    + "should show up in the output table.";
                }
            }

        }
        if (rightIter.hasNext()) {
            // right table has more rows
            m_runtimeWarnings.add("Possible problem in configuration found. "
                    + "The \"Duplicate Column Handling\" is configured to "
                    + "filter duplicates, but the duplicate columns are not "
                    + "equal since the bottom table has more elements than the "
                    + "top table.");
        }
        for (int i = 0; i < duplicates.size(); i++) {
            if (null != messages[i]) {
                m_runtimeWarnings.add(messages[i]);
            }
        }
    }

    /**
     * Used in compareDuplicates.
     * @param table A DataTable
     * @param cols Columns of the table
     * @return the indices of the given columns in the table.
     */
    static int[] getIndicesOf(final BufferedDataTable table,
            final List<String> cols) {
        int[] indices = new int[cols.size()];
        int c = 0;

        for (String col : cols) {
            for (int i = 0; i < table.getDataTableSpec().getNumColumns(); i++) {
                if (table.getSpec().getColumnSpec(i).getName().equals(col)) {
                    indices[c] = i;
                }
            }
            c++;
        }
        return indices;
    }

    /**
     * @return the rowKeyMap
     */
    public HashMap<RowKey, Set<RowKey>> getLeftRowKeyMap() {
        return m_leftRowKeyMap;
    }

    /**
     * @return the rowKeyMap
     */
    public HashMap<RowKey, Set<RowKey>> getRightRowKeyMap() {
        return m_rightRowKeyMap;
    }

    /**
     * Used for testing, only. Simulates an out-of-memory event after that many rows have been added to memory.
     *
     * @param maxRows the maximum number of rows before an event
     */
    void setRowsAddedBeforeOOM(final int maxRows) {
        m_rowsAddedBeforeForcedOOM = maxRows;
    }

    /**
     * @param settings
     * @param inSpecs
     * @param warningMessageHandler a consumer that accepts warnings created during configuration.
     * @return
     */
    public static DataTableSpec[] createOutputSpec(final Joiner3Settings settings, final DataTableSpec[] inSpecs, final Consumer<String> warningMessageHandler) {
        //FIXME
        assert false;
        return null;
    }

}

