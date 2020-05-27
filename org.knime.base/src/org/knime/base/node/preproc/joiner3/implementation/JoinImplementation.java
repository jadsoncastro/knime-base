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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.ColumnNameDisambiguation;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.RowIterator;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultCellIterator;
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

    BufferedDataTable m_left, m_right;


    /** True for Left Outer Join and Full Outer Join. */
    boolean m_retainLeft;

    /** True for Right Outer Join and Full Outer Join. */
    boolean m_retainRight;

    /**
     * True when in the dialog to option 'Match any of the following' is selected and when there are more than two
     * joining columns.
     */
    boolean m_matchAny;

    HashMap<RowKey, Set<RowKey>> m_leftRowKeyMap;

    HashMap<RowKey, Set<RowKey>> m_rightRowKeyMap;

    final List<String> m_runtimeWarnings;

    /**
     * The tables to be joined, in their logical order, e.g,. if the join is Employee ⨝ Department ⨝ City
     * the tables will be stored as Employee, Department, City in this list.
     */
//    protected final List<BufferedDataTable> m_tables;


    /**
     * Contains the column indices that will make it into the output table for a given input table.
     * E.g., Employee = (LastName, DepartmentID, Age) and the output shall contain Age and LastName.
     * Then the result would be int[]{2, 0}
     *
     */

    protected final Map<BufferedDataTable, JoinTableSettings> m_tableSettings = new HashMap<>();

    final JoinTableSettings leftSettings, rightSettings;

    /**
     * @throws InvalidSettingsException
     *
     */
    protected JoinImplementation(final Joiner3Settings settings, final BufferedDataTable... tables) {

        m_settings = settings;

        m_left = tables[0];
        m_right = tables[1];

        leftSettings = new JoinTableSettings(m_settings, true, m_left);
        rightSettings = new JoinTableSettings(m_settings, false, m_right);

        m_tableSettings.put(m_left, leftSettings);
        m_tableSettings.put(m_right, rightSettings);

        // TODO remove this?
        m_runtimeWarnings = new ArrayList<String>();

//        m_outputSpec = createSpec(Arrays.stream(tables).map(BufferedDataTable::getDataTableSpec).toArray(DataTableSpec[]::new));
        // TODO remove try catch fix error structure

    }

    /**
     * Where column indices are allowed, this one will indicate to use the row key.
     */
    final static int ROW_KEY_COLUMN_INDEX_INDICATOR = -1;
    final static int ROW_OFFSET_COLUMN_INDEX_INDICATOR = -2;
    final static String ROW_OFFSET_COLUMN_NAME = "$$ROW OFFSET$$";

    public abstract JoinContainer join(final ExecutionContext exec)
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

        if (s.getDuplicateHandling().equals(ColumnNameDisambiguation.AppendSuffix)
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
     * @return new row key consisting of the concatenation of the {@link RowKey} of the row from the outer table and the
     *         {@link RowKey} of the row from the inner table (in that order).
     */
    protected static RowKey concatRowKeys(final DataRow outer, final DataRow inner) {
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
