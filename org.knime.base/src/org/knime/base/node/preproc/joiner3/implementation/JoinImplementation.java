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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.DuplicateHandling;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.Extractor;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.JoinMode;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultCellIterator;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.streamable.StreamableFunction;
import org.knime.core.util.UniqueNameGenerator;

/**
 * A join implementation executes the join by iterating over the provided tables and generating output rows from
 * matching input rows. Implementations differ in their speed/memory-usage tradeoff and also in the type of join
 * predicates they support, e.g., {@link HashJoin} supports only equijoins.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 * TODO remove javadoc warning to fix serious ones.
 */
@SuppressWarnings("javadoc")
public abstract class JoinImplementation {
    /** Logger to print debug info to. */
    static final NodeLogger LOGGER = NodeLogger.getLogger(JoinImplementation.class);

    /**
     * This can change and the auxiliary data structures should be updated accordingly.
     */
    protected Joiner3Settings m_settings;

    /**
     *
     */
    protected final HashMap<DataTable, String[]> m_joinColumns;

    /**
     * If true, {@link #m_bigger} references the outer table and {@link #m_smaller} references the inner table. If
     * false, it's the other way around.
     */
    protected final boolean m_outerIsBigger;

    /**
     * References the table (inner or outer) with fewer rows. This value is empty if tables do not provide row counts to
     * sort them by size.
     *  TODO: make this probe input and hash input -- the smaller will usually be the hash input but
     * in streaming, we're forced to take what the users provides at the non-streaming input
     */
    final BufferedDataTable m_smaller;

    /**
     * References the table (either inner table or outer table) with more rows.
     * This value is empty if tables do not provide row counts to sort them by size.
     */
    final BufferedDataTable m_bigger;

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

    final List<String> m_runtimeWarnings;

    /** Only used for testcases, simulates an out-of-memory event after that many rows added in memory. */
    int m_rowsAddedBeforeForcedOOM;

    /**
     * The tables to be joined, in their logical order, e.g,. if the join is Employee ⨝ Department ⨝ City
     * the tables will be stored as Employee, Department, City in this list.
     */
    protected final List<BufferedDataTable> m_tables;


    private List<String> m_leftSurvivors;
    private List<String> m_rightSurvivors;
    /**
     * Contains the column indices that will make it into the output table for a given input table.
     * E.g., Employee = (LastName, DepartmentID, Age) and the output shall contain Age and LastName.
     * Then the result would be int[]{2, 0}
     *
     */
    protected final Map<BufferedDataTable, int[]> m_projectionColumns = new HashMap<>();

    protected final DataTableSpec m_outputSpec;

    protected final Map<BufferedDataTable, TableSettings> m_tableSettings = new HashMap<>();
    /**
     * @throws InvalidSettingsException
     *
     */
    protected JoinImplementation(final Joiner3Settings settings, final BufferedDataTable... tables) {

        m_settings = settings;

        m_tables = Arrays.asList(tables);


        //FIXME
        BufferedDataTable outer = tables[0];
        BufferedDataTable inner = tables[1];

        // if all tables have row counts, sort them by row count descending
        m_tables.sort(Comparator.comparingLong(BufferedDataTable::size).reversed());
        m_bigger = m_tables.get(0);
        m_smaller = m_tables.get(1);
        m_outerIsBigger = m_bigger == outer;

        // first table is the biggest

        // Map.of is Java 9
        m_joinColumns = new HashMap<>();
        m_tableSettings.put(outer, new TableSettings(settings, outer, Joiner3Settings::getLeftJoinColumns));
        m_tableSettings.put(inner, new TableSettings(settings, inner, Joiner3Settings::getRightJoinColumns));

        m_runtimeWarnings = new ArrayList<String>();

        // FIXME multiple tables
        m_outputSpec = createSpec(Arrays.stream(tables).map(BufferedDataTable::getDataTableSpec).toArray(DataTableSpec[]::new));
//        DataTableSpec joinedTableSpec = Joiner.createOutputSpec(m_settings, s -> {},
//            leftTable.getSpec(), rightTable.getSpec());

    }

    class TableSettings{

        String[] joinColumnNames;
        int[] joinColumnIndices;

        TableSettings(final Joiner3Settings settings, final BufferedDataTable table, final Function<Joiner3Settings, String[]> getter){
            joinColumnNames = getter.apply(settings);
            joinColumnIndices = table.getDataTableSpec().columnsToIndices(joinColumnNames);
        }
    }
    protected JoinTuple getJoinTuple(final BufferedDataTable table, final DataRow row) {
        int[] indices = m_tableSettings.get(table).joinColumnIndices;
        DataCell[] cells = new DataCell[indices.length];
        for (int i = 0; i < cells.length; i++) {
            if (indices[i] >= 0) {
                cells[i] = row.getCell(indices[i]);
            } else {
                // create a StringCell since row IDs may match
                // StringCell's
                cells[i] = new StringCell(row.getKey().getString());
            }
        }
        return new JoinTuple(cells);
    }

//
//
//    public int[] getJoinColumnIndices() {
//        int numJoinAttributes = m_settings.getLeftJoinColumns().length;
//        List<Integer> leftTableJoinIndices =
//            new ArrayList<Integer>(numJoinAttributes);
//        for (int i = 0; i < numJoinAttributes; i++) {
//            String joinAttribute = m_settings.getLeftJoinColumns()[i];
//            leftTableJoinIndices.add(
//                    leftTable.getDataTableSpec().findColumnIndex(
//                            joinAttribute));
//        }
//        return leftTableJoinIndices;
//    }

    /**
     * @param dataTableSpec input spec of the left DataTable
     * @return the names of all columns to include from the left input table
     * @throws InvalidSettingsException if the input spec is not compatible with the settings
     * @since 2.12
     */
    public List<String> getLeftIncluded(final DataTableSpec dataTableSpec)
     {
        List<String> leftCols = new ArrayList<String>();
        for (DataColumnSpec column : dataTableSpec) {
            leftCols.add(column.getName());
        }
        // Check if left joining columns are in table spec
        Set<String> leftJoinCols = new HashSet<String>();
        leftJoinCols.addAll(Arrays.asList(m_settings.getLeftJoinColumns()));
        leftJoinCols.remove(Joiner3Settings.ROW_KEY_IDENTIFIER);


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
    public List<String> getRightIncluded(final DataTableSpec dataTableSpec)
     {
        List<String> rightCols = new ArrayList<String>();
        for (DataColumnSpec column : dataTableSpec) {
            rightCols.add(column.getName());
        }
        // Check if right joining columns are in table spec
        Set<String> rightJoinCols = new HashSet<String>();
        rightJoinCols.addAll(Arrays.asList(m_settings.getRightJoinColumns()));
        rightJoinCols.remove(Joiner3Settings.ROW_KEY_IDENTIFIER);


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
    /**
     * Creates a spec for the output table by taking care of duplicate columns.
     *
     * @param specs the specs of the two input tables
     * @return the spec of the output table
     * @throws InvalidSettingsException when settings are not supported
     */
    protected DataTableSpec createSpec(final DataTableSpec[] specs)
     {

        List<String> leftCols = getLeftIncluded(specs[0]);
        List<String> rightCols = getRightIncluded(specs[1]);

        List<String> duplicates = new ArrayList<String>();
        duplicates.addAll(leftCols);
        duplicates.retainAll(rightCols);


        if (m_settings.getDuplicateHandling().equals(
                DuplicateHandling.Filter)) {

            for (String duplicate : duplicates) {
                DataType leftType = specs[0].getColumnSpec(duplicate).getType();
                DataType rightType = specs[1].getColumnSpec(duplicate).getType();
            }

            rightCols.removeAll(leftCols);
        }


        // check if data types of joining columns do match
//        for (int i = 0; i < m_settings.getLeftJoinColumns().length; i++) {
//            String leftJoinAttr = m_settings.getLeftJoinColumns()[i];
//            boolean leftJoinAttrIsRowKey =
//                    Joiner3Settings.ROW_KEY_IDENTIFIER.equals(leftJoinAttr);
//            DataType leftType = leftJoinAttrIsRowKey
//            ? StringCell.TYPE
//                    : specs[0].getColumnSpec(leftJoinAttr).getType();
//            String rightJoinAttr = m_settings.getRightJoinColumns()[i];
//            boolean rightJoinAttrIsRowKey =
//                Joiner3Settings.ROW_KEY_IDENTIFIER.equals(rightJoinAttr);
//            DataType rightType = rightJoinAttrIsRowKey
//            ? StringCell.TYPE
//                    : specs[1].getColumnSpec(rightJoinAttr).getType();
//            if (!leftType.equals(rightType)) {
//                String left = leftJoinAttrIsRowKey ? "Row ID" : leftJoinAttr;
//                String right = rightJoinAttrIsRowKey ? "Row ID" : rightJoinAttr;
//                // check different cases here to give meaningful error messages
//
//            }
//        }

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


//    public DataTableSpec getOutputSpec(final DataTableSpec[] dataTableSpecs, final Joiner3Settings settings,
//        final Consumer<String> configurationWarningHandler) throws InvalidSettingsException {
//
//        return createSpec(dataTableSpecs, settings, configurationWarningHandler);
//    }


    public abstract BufferedDataTable twoWayJoin(final ExecutionContext exec, final BufferedDataTable leftTable,
        final BufferedDataTable rightTable)
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
    protected int[] getJoinColumnIndices(final DataTable table) {
        return m_tableSettings.get(table).joinColumnIndices;
    }

    @Deprecated
    protected List<Integer> getLeftJoinIndices(final DataTable leftTable) {
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
    protected List<Integer> getRightJoinIndices(final DataTable rightTable) {
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

    JoinedRowKeyFactory createRowKeyFactory(final DataTable leftTable, final DataTable rightTable) {

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
    private boolean useSingleRowKeyFactory(final DataTable leftTable, final DataTable rightTable) {
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

    InputRow.Settings createInputDataRowSettings(final DataTable leftTable,
        final DataTable rightTable) {
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
    void compareDuplicates(final DataTable leftTable, final DataTable rightTable,
        final List<String> duplicates) {

        int[] leftIndex = getIndicesOf(leftTable, duplicates);
        int[] rightIndex = getIndicesOf(rightTable, duplicates);

        String[] messages = new String[duplicates.size()];

        RowIterator leftIter = leftTable.iterator();
        RowIterator rightIter = rightTable.iterator();
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
    static int[] getIndicesOf(final DataTable table, final List<String> cols) {
        int[] indices = new int[cols.size()];
        int c = 0;

        for (String col : cols) {
            for (int i = 0; i < table.getDataTableSpec().getNumColumns(); i++) {
                if (table.getDataTableSpec().getColumnSpec(i).getName().equals(col)) {
                    indices[c] = i;
                }
            }
            c++;
        }
        return indices;
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

    /**
     *
     * @param table
     * @return
     */
    protected Extractor getExtractor(final DataTable table) {

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
            return new JoinTuple(cells); //new JoinTuple(cells);
        };
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


    /**
     * @return
     */
    protected abstract StreamableFunction getStreamableFunction();
}
