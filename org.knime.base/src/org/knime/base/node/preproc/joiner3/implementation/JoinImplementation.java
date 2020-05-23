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
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import org.knime.core.data.RowIterator;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultCellIterator;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.streamable.StreamableFunction;

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
     * If true, {@link #m_bigger} references the outer table and {@link #m_smaller} references the inner table. If
     * false, it's the other way around.
     */
    protected final boolean m_leftIsBigger;

    BufferedDataTable m_leftTable, m_rightTable;

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

        BufferedDataTable outer = tables[0];
        m_leftTable = outer;

        BufferedDataTable inner = tables[1];
        m_rightTable = inner;

        // this is write through, sorting the list sorts the array
        m_tables = Arrays.asList(tables);

        m_tables.sort(Comparator.comparingLong(BufferedDataTable::size).reversed());

        // TODO this can probably be removed
        m_bigger = m_tables.get(0);
        m_smaller = m_tables.get(1);
        m_leftIsBigger = m_bigger == outer;

        // first table is the biggest

        // store join columns, include columns, etc. for each table
        m_tableSettings.put(outer, new TableSettings(outer, Joiner3Settings::getLeftJoinColumns, Joiner3Settings::getLeftIncludeCols));
        m_tableSettings.put(inner, new TableSettings(inner, Joiner3Settings::getRightJoinColumns, Joiner3Settings::getRightIncludeCols));

        m_runtimeWarnings = new ArrayList<String>();

        // FIXME multiple tables
//        m_outputSpec = createSpec(Arrays.stream(tables).map(BufferedDataTable::getDataTableSpec).toArray(DataTableSpec[]::new));
        // TODO remove try catch fix error structure
        m_outputSpec = Joiner.createOutputSpec(m_settings, s -> {}, m_leftTable.getSpec(), m_rightTable.getSpec());

    }

    /**
     * Where column indices are allowed, this one will indicate to use the row key.
     */
    final static int ROW_KEY_COLUMN_INDEX_INDICATOR = -10;

    /**
     * Bundle included columns, join columns, etc. for a given input table.
     *
     * @author Carl Witt, KNIME AG, Zurich, Switzerland
     *
     */
    class TableSettings{

        // original table
        // A B C D E F G   column names
        //   j   j j       join = [B, D, E], e.g., because join clauses are (D = ..., B = ..., B = ..., E = ..., Row Key = ...)
        // i i i   i       include = [A, B, C, E], because that's how the result is supposed to look like
        // r   r           retain = [A, C], because it's the included columns that are not join columns
        // when having to go on disk, retaining only the
        // working row layout is 1) long column for row offset 2) join columns + row key column 3) additional columns

        // the idea behind reordering is to reuse the join tuple array (⌜ 2 ⌝)
        // - extracted to hash and determine partition, both for probe and hash
        // - stored in in-memory data structure
        //
        // 1 ⌜  2  ⌝ ⌜3⌝
        // L B D E R A C    working table column names
        // 0 1 2 3 4 5 6    working table column indices
        // look up columns for the join clauses [2, 1, 1, 3, 4]
        // look up columns for the include columns are [5, 1, 6, 3] (lookup from [A, B, C, E])
        final BufferedDataTable m_forTable;

        // the names of this table's columns involved in join clauses; not unique
        final String[] joinClauses;
        // the set of columns indices to retain for checking whether a pair of rows matches; unique
        final int[] joinColumnIndices;

        // the names of the columns in the result table (doesn't have to cover join columns); unique
        final String[] includeColumnNames;
        // the set of non-join column indices containing additional columns for the output; unique
        final int[] retainColumnIndices;

        // defines which columns of the original table to materialize; join columns and retain columns
        // e.g., hashInput.filter(TableFilter.materializeCols(workingColumns)).iterator()
        final int[] workingTableColumnIndices;

        final int[] workingTableJoinColumnIndices;
        final int[] workingTableIncludeColumnIndices;

        final DataTableSpec workingTableSpec;
        final String workingTableRowOffsetColumnName;

        /**
         * Drop columns that don't make it into the output early to save heap space and I/O when forced to flush rows to
         * disk.
         *
         * @param forTable refers to the hash input table (or one of multiple hash input tables) or the probe input table
         * @return The spec of the given table filtered to included and join columns.
         */
        TableSettings(final BufferedDataTable table, final Function<Joiner3Settings, String[]> joinColumns, final Function<Joiner3Settings, String[]> includeColumns){

            m_forTable = table;

            joinClauses = joinColumns.apply(m_settings);
            Predicate<String> isJoinColumn = s -> Arrays.asList(joinClauses).contains(s);

            joinColumnIndices = Arrays.stream(joinClauses)
                    .filter(m_settings.isRowKeyIndicator().negate())
                    .distinct()
                    .mapToInt(table.getDataTableSpec()::findColumnIndex).toArray();
//            joinColumnIndicesWithIndicator = findJoinColumns(table.getDataTableSpec());

            includeColumnNames = includeColumns.apply(m_settings);
            Predicate<String> isIncludedColumn = s -> Arrays.asList(includeColumnNames).contains(s);

            retainColumnIndices = Arrays.stream(includeColumnNames)
                    .filter(isJoinColumn.negate())
                    .mapToInt(table.getDataTableSpec()::findColumnIndex)
                    .toArray();

            { // determine which columns to keep in the working version of this table

                // retain only the columns needed to perform the join and the columns that make it into the output
                List<DataColumnSpec> colSpecs = table.getDataTableSpec().stream()
                    .filter(colSpec -> isJoinColumn.or(isIncludedColumn).test(colSpec.getName()))
                    .collect(Collectors.toList());

                // to sort according to natural join order, add a column that stores the row's offset in its source table
                workingTableRowOffsetColumnName = DataTableSpec.getUniqueColumnName(table.getDataTableSpec(), "row offset");
                colSpecs.add(new DataColumnSpecCreator(workingTableRowOffsetColumnName, LongCell.TYPE).createSpec());

                workingTableColumnIndices = IntStream.concat(Arrays.stream(joinColumnIndices), Arrays.stream(retainColumnIndices)).toArray();
                workingTableSpec = new DataTableSpec(colSpecs.toArray(new DataColumnSpec[colSpecs.size()]));

                // the columns offsets that make it into the reduced table
//                = Arrays.stream(workingTableSpec.getColumnNames())
//                    .filter(m_settings.isRowKeyIndicator().negate())
//                    .mapToInt(table.getDataTableSpec()::findColumnIndex).toArray();
                workingTableJoinColumnIndices = findJoinColumns(workingTableSpec);
                // the offsets of the included columns in the reduced table
                workingTableIncludeColumnIndices = workingTableSpec.columnsToIndices(includeColumnNames);
            }
        }

        /**
         * Join columns can contain non-existing column names (e.g., $Row Key$) to indicate joining on row keys.
         * @param spec
         * @return
         */
        int[] findJoinColumns(final DataTableSpec spec) {
            return Arrays.stream(joinClauses).mapToInt(name -> {
                int col = spec.findColumnIndex(name);
                return m_settings.getRowKeyIndicator().equals(name) ? ROW_KEY_COLUMN_INDEX_INDICATOR : col;
            }).toArray();
        }

        /**
         * Translate column names to column indices, use the special index {@link ROW_KEY_COLUMN_INDEX_INDICATOR} for
         * the row key column.
         * @param spec the spec in which to look up the names.
         * @return
         */
        int[] joinColumnIndicesWithIndicator(final DataTableSpec spec) {
            return Arrays.stream(joinClauses).mapToInt(name -> {
                int col = spec.findColumnIndex(name);
                return m_settings.getRowKeyIndicator().equals(name) ? ROW_KEY_COLUMN_INDEX_INDICATOR : col;
            }).toArray();
        }
        int[] joinColumnIndicesWithIndicator() { return joinColumnIndicesWithIndicator(m_forTable.getDataTableSpec()); }


        /**
         * Copy selected cells from contents to save heap -- however, this pays off only if the working columns are fewer than the original columns
         * @param rowOffset
         * @param fullRow
         * @return
         */
        public WorkingRow workingRow(final long rowOffset, final DataRow fullRow) {
            // TODO just move this?
            return new WorkingRow(fullRow, joinColumnIndices, includeColumnIndices, rowOffset);
        }

        /**
         * Extract the values from the join columns from the row.
         * @param row
         * @param table
         * @return
         */
        protected JoinTuple getJoinTuple(final DataRow workingRow) {
            // TODO maybe implement a DataRow that allows to reuse the DataCell[] array that stores the join column values by internally concatenating them
            int[] indices = workingTableJoinColumnIndices;
            DataCell[] cells = new DataCell[indices.length];
            for (int i = 0; i < cells.length; i++) {
                if (indices[i] >= 0) {
                    cells[i] = workingRow.getCell(indices[i]);
                } else if (indices[i] == ROW_KEY_COLUMN_INDEX_INDICATOR){
                    // create a StringCell since row IDs may match
                    // StringCell's
                    cells[i] = new StringCell(workingRow.getKey().getString());
                }
            }
            return new JoinTuple(cells);
        }

        /**
        *
        * @param table
        * @return
        */
       protected Extractor getExtractor() {

           final int[] indices = workingTableJoinColumnIndices;
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


    public abstract BufferedDataTable twoWayJoin(final ExecutionContext exec, final BufferedDataTable leftTable,
        final BufferedDataTable rightTable)
        throws CanceledExecutionException, InvalidSettingsException;

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
     * @param probeRow a row from the bigger table.
     * @param hashRow a row from the smaller table.
     * @return the row that belongs to the outer table.
     */
    protected DataRow getLeft(final DataRow probeRow, final DataRow hashRow) {
        if (m_leftIsBigger) {
            return probeRow;
        } else {
            return hashRow;
        }
    }

    /**
     * @param probeRow a row from the bigger table.
     * @param hashRow a row from the smaller table.
     * @return the row that belongs to the inner table.
     */
    protected DataRow getRight(final DataRow probeRow, final DataRow hashRow) {
        if (m_leftIsBigger) {
            return hashRow;
        } else {
            return probeRow;
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


    /**
     * @return
     */
    protected abstract StreamableFunction getStreamableFunction();
}
