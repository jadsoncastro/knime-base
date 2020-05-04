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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.DuplicateHandling;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.JoinMode;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.RowKey;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.data.def.DefaultCellIterator;
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
 *
 */
public abstract class AbstractJoiner implements Joiner {
    /** Logger to print debug info to. */
    static final NodeLogger LOGGER = NodeLogger.getLogger(AbstractJoiner.class);

    /**
     * Defines a mapping from a row in a table to the representation of the values of their join columns.
     */
    protected interface Extractor extends Function<DataRow, JoinTuple> {
    }

    /**
     *
     * @param table
     * @return
     */
    protected Extractor getExtractor(final BufferedDataTable table) {

        final int[] indices = getJoinColumnIndices(table);
        return row -> {

                final DataCell[] cells = new DataCell[indices.length];
                // using a stream here would be more readable, but has severe performance costs
                for (int i = 0; i < indices.length; i++) {
                    if (indices[i] >= 0) {
                        cells[i] = row.getCell(indices[i]);
                    } else {
                        // It is allowed to use the row ids to join tables.
                        // However, row keys are not stored in a column of a table, so the convention
                        // is used
                        cells[i] = new StringCell(row.getKey().getString());
                    }
                }
                return new JoinTuple(cells);
            };
    }


    private final DataTableSpec m_leftDataTableSpec;

    private final DataTableSpec m_rightDataTableSpec;

    private final DataTableSpec[] m_dataTableSpecs;

    private final HashMap<BufferedDataTable, String[]> m_joinColumns;

    /**
     * If true, {@link #m_bigger} references the outer table and {@link #m_smaller} references the inner table. If
     * false, it's the other way around.
     */
    protected final boolean m_outerIsBigger;

    /**
     * References the table (inner or outer) with fewer rows.
     */
    final BufferedDataTable m_smaller;

    /**
     * References the table (either inner table or outer table) with more rows.
     */
    final BufferedDataTable m_bigger;

    final Joiner3Settings m_settings;

    /** True for Left Outer Join and Full Outer Join. */
    boolean m_retainLeft;

    /** True for Right Outer Join and Full Outer Join. */
    boolean m_retainRight;

    /**
     * True when in the dialog to option 'Match any of the following' is selected and when there are more than two
     * joining columns.
     */
    boolean m_matchAny;

    /**
     * This field is only used when (m_retainLeft && m_matchAny) is true. It holds the row indices of the left table
     * that did not match to a row of the right table.
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
    protected AbstractJoiner(final Joiner3Settings settings, final BufferedDataTable outer,
        final BufferedDataTable inner) {

        BufferedDataTable[] ordered = new BufferedDataTable[]{outer, inner};
        m_outerIsBigger = outer.size() >= inner.size();
        final int biggerIndex = m_outerIsBigger ? 0 : 1;
        final int smallerIndex = 1 - biggerIndex;
        m_bigger = ordered[biggerIndex];
        m_smaller = ordered[smallerIndex];

        m_leftDataTableSpec = outer.getDataTableSpec();
        //FIXME more than one inner table
        m_rightDataTableSpec = inner.getDataTableSpec();
        m_settings = settings;
        m_dataTableSpecs = new DataTableSpec[]{outer.getDataTableSpec(), inner.getDataTableSpec()};

        // Map.of is Java 9
        m_joinColumns = new HashMap<>();
        m_joinColumns.put(outer, settings.getLeftJoinColumns());
        m_joinColumns.put(inner, settings.getRightJoinColumns());

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
    DataTableSpec createSpec(final DataTableSpec[] specs) throws InvalidSettingsException {
        return createSpec(specs, m_settings, IGNORE_WARNINGS);
    }

    /**
     * Creates a spec for the output table by taking care of duplicate columns.
     *
     * @param specs the specs of the two input tables
     * @param settings TODO
     * @return the spec of the output table
     * @throws InvalidSettingsException when settings are not supported
     */
    static DataTableSpec createSpec(final DataTableSpec[] specs, final Joiner3Settings settings,
        final Consumer<String> configurationWarningHandler) throws InvalidSettingsException {
        validateSettings(settings);

        List<String> leftCols = getLeftIncluded(specs[0], settings);
        List<String> rightCols = getRightIncluded(specs[1], settings);

        List<String> duplicates = new ArrayList<String>();
        duplicates.addAll(leftCols);
        duplicates.retainAll(rightCols);

        if (settings.getDuplicateHandling().equals(DuplicateHandling.DontExecute) && !duplicates.isEmpty()) {
            throw new InvalidSettingsException(
                "Found duplicate columns, won't execute. Fix it in " + "\"Column Selection\" tab");
        }

        if (settings.getDuplicateHandling().equals(DuplicateHandling.Filter)) {

            for (String duplicate : duplicates) {
                DataType leftType = specs[0].getColumnSpec(duplicate).getType();
                DataType rightType = specs[1].getColumnSpec(duplicate).getType();
                if (!leftType.equals(rightType)) {
                    configurationWarningHandler.accept("The column \"" + duplicate + "\" can be found in "
                        + "both input tables but with different data type. "
                        + "Only the one in the top input table will show "
                        + "up in the output table. Please change the " + "Duplicate Column Handling if both columns "
                        + "should show up in the output table.");
                }
            }

            rightCols.removeAll(leftCols);
        }

        if ((!duplicates.isEmpty()) && settings.getDuplicateHandling().equals(DuplicateHandling.AppendSuffix)
            && (settings.getDuplicateColumnSuffix() == null || settings.getDuplicateColumnSuffix().equals(""))) {
            throw new InvalidSettingsException("No suffix for duplicate columns provided.");
        }

        // check if data types of joining columns do match
        for (int i = 0; i < settings.getLeftJoinColumns().length; i++) {
            String leftJoinAttr = settings.getLeftJoinColumns()[i];
            boolean leftJoinAttrIsRowKey = Joiner3Settings.ROW_KEY_IDENTIFIER.equals(leftJoinAttr);
            DataType leftType = leftJoinAttrIsRowKey ? StringCell.TYPE : specs[0].getColumnSpec(leftJoinAttr).getType();
            String rightJoinAttr = settings.getRightJoinColumns()[i];
            boolean rightJoinAttrIsRowKey = Joiner3Settings.ROW_KEY_IDENTIFIER.equals(rightJoinAttr);
            DataType rightType =
                rightJoinAttrIsRowKey ? StringCell.TYPE : specs[1].getColumnSpec(rightJoinAttr).getType();
            if (!leftType.equals(rightType)) {
                String left = leftJoinAttrIsRowKey ? "Row ID" : leftJoinAttr;
                String right = rightJoinAttrIsRowKey ? "Row ID" : rightJoinAttr;
                // check different cases here to give meaningful error messages
                if (leftType.equals(DoubleCell.TYPE) && rightType.equals(IntCell.TYPE)) {
                    throw new InvalidSettingsException(
                        "Type mismatch found of " + "Joining Column Pair \"" + left + "\" and \"" + right + "\"."
                            + " Use \"Double to Int node\" to " + "convert the type of \"" + left + "\" to integer.");
                } else if (leftType.equals(IntCell.TYPE) && rightType.equals(DoubleCell.TYPE)) {
                    throw new InvalidSettingsException(
                        "Type mismatch found of " + "Joining Column Pair \"" + left + "\" and \"" + right + "\"."
                            + " se \"Double to Int node\" to " + "convert the type of \"" + right + "\" to integer.");
                } else if (leftType.isCompatible(DoubleValue.class) && rightType.equals(StringCell.TYPE)) {
                    throw new InvalidSettingsException(
                        "Type mismatch found of " + "Joining Column Pair \"" + left + "\" and \"" + right + "\"."
                            + " Use \"Number to String node\" to " + "convert the type of \"" + left + "\" to string.");
                } else if (leftType.equals(StringCell.TYPE) && rightType.isCompatible(DoubleValue.class)) {
                    throw new InvalidSettingsException("Type mismatch found of " + "Joining Column Pair \"" + left
                        + "\" and \"" + right + "\"." + " Use \"Number to String node\" to " + "convert the type of \""
                        + right + "\" to string.");
                } else if (leftType.getPreferredValueClass() != rightType.getPreferredValueClass()) {
                    // if both don't have the same preferred class they can't be equals, see DataCell#equals

                    throw new InvalidSettingsException("Type mismatch found of " + "Joining Column Pair \"" + left
                        + "\" and \"" + right + "\"." + "This causes an empty output table.");
                }
            }
        }

        List<DataColumnSpec> outColSpecs = new ArrayList<DataColumnSpec>();

        survivors(specs, settings, leftCols, rightCols, outColSpecs);

        return new DataTableSpec(outColSpecs.toArray(new DataColumnSpec[outColSpecs.size()]));
    }

    /**
     * @param specs
     * @param settings
     * @param rightCols
     * @param nameGen
     * @param leftSurvivors
     * @param outColSpecs
     */
    protected static List<List<String>> survivors(final DataTableSpec[] specs, final Joiner3Settings settings,
        final List<String> leftCols, final List<String> rightCols, final List<DataColumnSpec> outColSpecs) {

        @SuppressWarnings("unchecked")
        UniqueNameGenerator nameGen = new UniqueNameGenerator(Collections.EMPTY_SET);
        List<String> leftSurvivors = new ArrayList<String>();
        for (int i = 0; i < specs[0].getNumColumns(); i++) {
            DataColumnSpec columnSpec = specs[0].getColumnSpec(i);
            if (leftCols.contains(columnSpec.getName())) {
                outColSpecs.add(columnSpec);
                nameGen.newName(columnSpec.getName());
                leftSurvivors.add(columnSpec.getName());
            }
        }

        List<String> rightSurvivors = new ArrayList<String>();
        for (int i = 0; i < specs[1].getNumColumns(); i++) {
            DataColumnSpec columnSpec = specs[1].getColumnSpec(i);
            if (rightCols.contains(columnSpec.getName())) {
                if (settings.getDuplicateHandling().equals(DuplicateHandling.AppendSuffix)) {
                    if (leftSurvivors.contains(columnSpec.getName()) || rightSurvivors.contains(columnSpec.getName())) {
                        String newName = columnSpec.getName();
                        do {
                            newName += settings.getDuplicateColumnSuffix();
                        } while (leftSurvivors.contains(newName) || rightSurvivors.contains(newName));

                        DataColumnSpecCreator dcsc = new DataColumnSpecCreator(columnSpec);
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
                        DataColumnSpecCreator dcsc = new DataColumnSpecCreator(columnSpec);
                        dcsc.removeAllHandlers();
                        dcsc.setName(newName);
                        outColSpecs.add(dcsc.createSpec());
                    }

                }
                rightSurvivors.add(columnSpec.getName());
            }
        }
        List<List<String>> result = new ArrayList<>();
        result.add(leftSurvivors);
        result.add(rightSurvivors);
        return result;
    }

    /**
     * @param dataTableSpec input spec of the left DataTable
     * @return the names of all columns to include from the left input table
     * @throws InvalidSettingsException if the input spec is not compatible with the settings
     * @since 2.12
     */
    static List<String> getLeftIncluded(final DataTableSpec dataTableSpec, final Joiner3Settings settings)
        throws InvalidSettingsException {
        List<String> leftCols = new ArrayList<String>();
        for (DataColumnSpec column : dataTableSpec) {
            leftCols.add(column.getName());
        }
        // Check if left joining columns are in table spec
        Set<String> leftJoinCols = new HashSet<String>();
        leftJoinCols.addAll(Arrays.asList(settings.getLeftJoinColumns()));
        leftJoinCols.remove(Joiner3Settings.ROW_KEY_IDENTIFIER);
        if (!leftCols.containsAll(leftJoinCols)) {
            leftJoinCols.removeAll(leftCols);
            throw new InvalidSettingsException(
                "The top input table has " + "changed. Some joining columns are missing: "
                    + ConvenienceMethods.getShortStringFrom(leftJoinCols, 3));
        }

        if (!settings.getLeftIncludeAll()) {
            List<String> leftIncludes = Arrays.asList(settings.getLeftIncludeCols());
            leftCols.retainAll(leftIncludes);
        }
        if (settings.getRemoveLeftJoinCols()) {
            leftCols.removeAll(Arrays.asList(settings.getLeftJoinColumns()));
        }
        return leftCols;
    }

    /**
     * @param dataTableSpec input spec of the right DataTable
     * @return the names of all columns to include from the left input table
     * @throws InvalidSettingsException if the input spec is not compatible with the settings
     * @since 2.12
     */
    static List<String> getRightIncluded(final DataTableSpec dataTableSpec, final Joiner3Settings settings)
        throws InvalidSettingsException {
        List<String> rightCols = new ArrayList<String>();
        for (DataColumnSpec column : dataTableSpec) {
            rightCols.add(column.getName());
        }
        // Check if right joining columns are in table spec
        Set<String> rightJoinCols = new HashSet<String>();
        rightJoinCols.addAll(Arrays.asList(settings.getRightJoinColumns()));
        rightJoinCols.remove(Joiner3Settings.ROW_KEY_IDENTIFIER);
        if (!rightCols.containsAll(rightJoinCols)) {
            rightJoinCols.removeAll(rightCols);
            throw new InvalidSettingsException(
                "The bottom input table has " + "changed. Some joining columns are missing: "
                    + ConvenienceMethods.getShortStringFrom(rightJoinCols, 3));
        }

        if (!settings.getRightIncludeAll()) {
            List<String> rightIncludes = Arrays.asList(settings.getRightIncludeCols());
            rightCols.retainAll(rightIncludes);
        }
        if (settings.getRemoveRightJoinCols()) {
            rightCols.removeAll(Arrays.asList(settings.getRightJoinColumns()));
        }
        return rightCols;
    }

    public DataTableSpec getOutputSpec(final DataTableSpec[] dataTableSpecs, final Joiner3Settings settings,
        final Consumer<String> configurationWarningHandler) throws InvalidSettingsException {

        return createSpec(dataTableSpecs, settings, configurationWarningHandler);
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
    void setMessage(final String activity, final ExecutionContext exec, final Collection<Integer> pendingParts,
        final Collection<Integer> currParts) {
        exec.setMessage(activity + " " + currParts.size() + " parts | Pending : "
            + (pendingParts.size() - currParts.size()) + " parts | Total: " + " parts.");
    }

    /**
     * Look up join column names. TODO maybe pass around table references instead of their offsets or logical roles
     * (LEFT, RIGHT, INNER, OUTER) because the indirection can be done at the reference level then:
     *
     * BDT smaller = rightTable; Then use the BDT reference to obtain information: getJoinColumnIndices(smaller)
     *
     * TODO check MCSM ColumnAccessor data structure
     *
     * @param inputTableIndex
     * @return
     */
    protected int[] getJoinColumnIndices(final BufferedDataTable table) {
        return Arrays.stream(m_joinColumns.get(table)).mapToInt(table.getDataTableSpec()::findColumnIndex).toArray();
    }

    @Deprecated
    protected List<Integer> getLeftJoinIndices(final BufferedDataTable leftTable) {
        // Create list of indices for the joining columns (Element of the list
        // is -1 if RowKey should be joined).
        int numJoinAttributes = m_settings.getLeftJoinColumns().length;
        List<Integer> leftTableJoinIndices = new ArrayList<Integer>(numJoinAttributes);
        for (int i = 0; i < numJoinAttributes; i++) {
            String joinAttribute = m_settings.getLeftJoinColumns()[i];
            leftTableJoinIndices.add(leftTable.getDataTableSpec().findColumnIndex(joinAttribute));
        }
        return leftTableJoinIndices;
    }

    @Deprecated
    protected List<Integer> getRightJoinIndices(final BufferedDataTable rightTable) {
        // Create list of indices for the joining columns (Element of the list
        // is -1 if RowKey should be joined).
        int numJoinAttributes = m_settings.getLeftJoinColumns().length;
        List<Integer> rightTableJoinIndices = new ArrayList<Integer>(numJoinAttributes);
        for (int i = 0; i < numJoinAttributes; i++) {
            String joinAttribute = m_settings.getRightJoinColumns()[i];
            rightTableJoinIndices.add(rightTable.getDataTableSpec().findColumnIndex(joinAttribute));
        }
        return rightTableJoinIndices;
    }

    JoinedRowKeyFactory createRowKeyFactory(final BufferedDataTable leftTable, final BufferedDataTable rightTable) {

        if (useSingleRowKeyFactory(leftTable, rightTable)) {
            // This is the special case of row key match row key
            return new UseSingleRowKeyFactory();
        } else {
            return new ConcatenateJoinedRowKeyFactory(m_settings.getRowKeySeparator());
        }
    }

    /**
     * Gives true when the SingleRowKeyFactory should be used.
     */
    private boolean useSingleRowKeyFactory(final BufferedDataTable leftTable, final BufferedDataTable rightTable) {
        List<Integer> leftTableJoinIndices = getLeftJoinIndices(leftTable);
        List<Integer> rightTableJoinIndices = getRightJoinIndices(rightTable);

        boolean joinRowIdsOnly = true;
        boolean joinRowIds = false;
        for (int i = 0; i < leftTableJoinIndices.size(); i++) {
            joinRowIdsOnly = joinRowIdsOnly && leftTableJoinIndices.get(i) == -1 && rightTableJoinIndices.get(i) == -1;
            joinRowIds = joinRowIds || (leftTableJoinIndices.get(i) == -1 && rightTableJoinIndices.get(i) == -1);
        }
        return joinRowIdsOnly || (joinRowIds && !m_matchAny && m_settings.getJoinMode().equals(JoinMode.InnerJoin)
            && m_settings.useEnhancedRowIdHandling());
    }

    InputRow.Settings createInputDataRowSettings(final BufferedDataTable leftTable,
        final BufferedDataTable rightTable) {
        List<Integer> leftTableJoinIndices = getLeftJoinIndices(leftTable);
        List<Integer> rightTableJoinIndices = getRightJoinIndices(rightTable);

        // Build m_inputDataRowSettings
        Map<InputRow.Settings.InDataPort, List<Integer>> joiningIndicesMap =
            new HashMap<InputRow.Settings.InDataPort, List<Integer>>();
        joiningIndicesMap.put(InputRow.Settings.InDataPort.Left, leftTableJoinIndices);
        joiningIndicesMap.put(InputRow.Settings.InDataPort.Right, rightTableJoinIndices);

        InputRow.Settings inputDataRowSettings = new InputRow.Settings(joiningIndicesMap, m_matchAny);

        return inputDataRowSettings;
    }

    /**
     * Validates the settings in the passed <code>NodeSettings</code> object. The specified settings is checked for
     * completeness and consistency.
     *
     * @param s The settings to validate.
     * @throws InvalidSettingsException If the validation of the settings failed.
     */
    public static void validateSettings(final Joiner3Settings s) throws InvalidSettingsException {
        if (s.getDuplicateHandling() == null) {
            throw new InvalidSettingsException("No duplicate handling method selected");
        }
        if (s.getJoinMode() == null) {
            throw new InvalidSettingsException("No join mode selected");
        }
        if ((s.getLeftJoinColumns() == null) || s.getLeftJoinColumns().length < 1 || s.getRightJoinColumns() == null
            || s.getRightJoinColumns().length < 1) {
            throw new InvalidSettingsException("Please define at least one joining column pair.");
        }
        if (s.getLeftJoinColumns() != null && s.getRightJoinColumns() != null
            && s.getLeftJoinColumns().length != s.getRightJoinColumns().length) {
            throw new InvalidSettingsException(
                "Number of columns selected from the top table and from " + "the bottom table do not match");
        }

        if (s.getDuplicateHandling().equals(DuplicateHandling.AppendSuffix)
            && (s.getDuplicateColumnSuffix() == null || s.getDuplicateColumnSuffix().isEmpty())) {
            throw new InvalidSettingsException("No suffix for duplicate columns provided");
        }
        if (s.getMaxOpenFiles() < 3) {
            throw new InvalidSettingsException("Maximum number of open files must be at least 3.");
        }

    }

    /**
     * @param leftTable The left input table
     * @param rightTable The right input table
     * @param duplicates The list of columns to test for identity
     */
    void compareDuplicates(final BufferedDataTable leftTable, final BufferedDataTable rightTable,
        final List<String> duplicates) {

        int[] leftIndex = getIndicesOf(leftTable, duplicates);
        int[] rightIndex = getIndicesOf(rightTable, duplicates);

        String[] messages = new String[duplicates.size()];

        CloseableRowIterator leftIter = leftTable.iterator();
        CloseableRowIterator rightIter = rightTable.iterator();
        while (leftIter.hasNext()) {
            if (!rightIter.hasNext()) {
                // right table has less rows
                m_runtimeWarnings
                    .add("Possible problem in configuration " + "found. The \"Duplicate Column Handling\" is "
                        + "configured to  filter duplicates, but the " + "duplicate columns are not equal since the "
                        + "top table has more elements than the bottom " + "table.");
                break;
            }
            DataRow left = leftIter.next();
            DataRow right = rightIter.next();
            for (int i = 0; i < duplicates.size(); i++) {
                if (null == messages[i] && !left.getCell(leftIndex[i]).equals(right.getCell(rightIndex[i]))) {
                    // Two cells do not match
                    messages[i] = "The column \"" + duplicates.get(i) + "\" can be found in "
                        + "both input tables but the content is not " + "equal. "
                        + "Only the one in the top input table will show "
                        + "up in the output table. Please change the " + "Duplicate Column Handling if both columns "
                        + "should show up in the output table.";
                }
            }

        }
        if (rightIter.hasNext()) {
            // right table has more rows
            m_runtimeWarnings
                .add("Possible problem in configuration found. " + "The \"Duplicate Column Handling\" is configured to "
                    + "filter duplicates, but the duplicate columns are not "
                    + "equal since the bottom table has more elements than the " + "top table.");
        }
        for (int i = 0; i < duplicates.size(); i++) {
            if (null != messages[i]) {
                m_runtimeWarnings.add(messages[i]);
            }
        }
    }

    /**
     * Used in compareDuplicates.
     *
     * @param table A DataTable
     * @param cols Columns of the table
     * @return the indices of the given columns in the table.
     */
    static int[] getIndicesOf(final BufferedDataTable table, final List<String> cols) {
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
    public static DataTableSpec[] createOutputSpec(final Joiner3Settings settings, final DataTableSpec[] inSpecs,
        final Consumer<String> warningMessageHandler) {

        return null;
    }

    /**
     * @param bigger a row from the bigger table.
     * @param smaller a row from the smaller table.
     * @return the row that belongs to the outer table.
     */
    protected DataRow getOuter(final DataRow bigger, final DataRow smaller) {
        if (m_outerIsBigger) {
            return bigger;
        } else {
            return smaller;
        }
    }

    /**
     * @param bigger a row from the bigger table.
     * @param smaller a row from the smaller table.
     * @return the row that belongs to the inner table.
     */
    protected DataRow getInner(final DataRow bigger, final DataRow smaller) {
        if (m_outerIsBigger) {
            return smaller;
        } else {
            return bigger;
        }
    }

    /**
     * @return new row key consisting of the concatenation of the {@link RowKey} of the row from the outer table and the
     *         {@link RowKey} of the row from the inner table (in that order).
     */
    protected RowKey concatRowKeys(final DataRow outer, final DataRow inner) {
        final StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(outer.getKey().getString());
        stringBuilder.append("_");
        stringBuilder.append(inner.getKey().getString());
        return new RowKey(stringBuilder.toString());
    }

    class JoinedRow implements DataRow {
        /** Underlying left row. */
        private final DataRow m_left;

        /** And its right counterpart. */
        private final DataRow m_right;

        private final RowKey m_rowKey;

        /**
         * Creates a new row based on two given rows.
         *
         * @param outer The left row providing the head cells
         * @param inner The right row providing the tail cells
         * @throws NullPointerException If either argument is null
         * @throws IllegalArgumentException If row key's ids aren't equal.
         */
        public JoinedRow(final RowKey rowKey, final DataRow outer, final DataRow inner) {
            m_rowKey = rowKey;
            m_left = outer;
            m_right = inner;
        }

        public JoinedRow(final DataRow outer, final DataRow inner) {
            this(concatRowKeys(outer, inner), outer, inner);
        }

        @Override
        public int getNumCells() {
            return m_left.getNumCells() + m_right.getNumCells();
        }

        @Override
        public RowKey getKey() {
            return m_rowKey;
        }

        @Override
        public DataCell getCell(final int index) {
            final int leftCellCount = m_left.getNumCells();
            // I guess both implementation will IndexOutOfBounds if out of range,
            // and so do we.
            if (index < leftCellCount) {
                return m_left.getCell(index);
            } else {
                return m_right.getCell(index - leftCellCount);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Iterator<DataCell> iterator() {

            return new DefaultCellIterator(this);
        }

    }
}
