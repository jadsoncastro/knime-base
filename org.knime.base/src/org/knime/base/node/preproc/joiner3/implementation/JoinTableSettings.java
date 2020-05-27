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
 *   May 27, 2020 (carlwitt): created
 */
package org.knime.base.node.preproc.joiner3.implementation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;

/**
 * Bundle included columns, join columns, etc. for a given input table.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 *
 */
class JoinTableSettings {

    // original table
    // A B C D E F G   column names
    //   j   j j       join = [B, D, E], e.g., because join clauses are (D = ..., B = ..., B = ..., E = ..., Row Key = ...)
    // i i i   i       include = [A, B, C, E], because that's how the result is supposed to look like
    // r   r           retain = [A, C], because it's the included columns that are not join columns

    //
    //
    // projection layout
    // L A B C D E
    // project down to join | include add row offset column at the beginning
    //
    // shuffle layout
    // working row layout is 1) long column for row offset 2) join columns + row key column 3) additional columns
    // the idea behind reordering is to reuse the join tuple array (⌜ 2 ⌝), which
    // - extracted to hash and determine partition, both for probe and hash
    // - stored in in-memory data structure
    //
    // 1 ⌜ 2 ⌝ ⌜3⌝
    // L B D E A C    working table column names
    // 0 1 2 3 4 5    working table column indices
    // look up columns for the join clauses [2, 1, 1, 3, 4]
    // look up columns for the include columns are [5, 1, 6, 3] (lookup from [A, B, C, E])
    //
    final BufferedDataTable m_forTable;

    // the names of this table's columns involved in one side of the join predicates; not unique
    // e.g., outertable.col2 = innertable.col3
    //       outertable.col2 = innertable.col1
    // would result in [col2, col2] in the outer table's joinClauses and [col3, col1] in the inner tables clauses

    /** list of expressions (either a column name or Row Key); not unique */
    final String[] joinClauses;

    final String[] joinColumnNames;

    //        /** column names (including virtual columns like $Row Key$) that are materialized in column group 2; unique */
    //        final String[] joinColumnNames;
    /** where to get the join column values in the source table, might include negative values for $Row Key$; unique */
    final int[] joinColumnsSource;

    //
    /**
     * Names of the columns to include in the result table; unique Doesn't have to cover join columns. The result table
     * may also include columns from the other input table.
     */
    final String[] includeColumnNames;
    //        /** names of the columns in column set 3 */
    //        final String[] retainColumnNames;
    //        /** where to get columns of set 3 in the original table; unique */
    //        final int[] retainColumnsSource;

    /** Where to get the values of the included columns in the original table. */
    final int[] includeColumns;

    /**
     * Which columns of the original table to materialize; union of join columns and retain columns.
     */
    final int[] materializeColumnIndices;

    //
    //        /** */
    //        final int[] workingTableJoinColumnIndices;
    //        final int[] workingTableIncludeColumnIndices;
    /** Where to get the working table column contents in the original table. */
    final int[] workingTableColumnToSource;

    /** Where to get the join column values in the working table, might include negative value for $Row Key$; */
    final int[] joinColumnsWorkingTable;

    final DataTableSpec workingTableSpec;
    //        final String workingTableRowOffsetColumnName;

    final int[] includeColumnsWorkingTable;

    final long materializedCells;

    final boolean isLeft;

    final Joiner3Settings m_settings;

    final boolean retainUnmatched;

//    boolean storeRowOffsets = false;


    /**
     * Drop columns that don't make it into the output early to save heap space and I/O when forced to flush rows to
     * disk.
     *
     * @param forTable refers to the hash input table (or one of multiple hash input tables) or the probe input table
     * @return The spec of the given table filtered to included and join columns.
     */
    JoinTableSettings(final Joiner3Settings settings, final boolean isLeft, final BufferedDataTable table) {

        m_settings = settings;
        this.isLeft = isLeft;
        m_forTable = table;
        joinClauses = isLeft ? settings.getLeftJoinColumns() : settings.getRightJoinColumns();

        retainUnmatched = isLeft ? settings.isRetainLeftUnmatched() : settings.isRetainRightUnmatched();

        Predicate<String> isRowKey = settings.isRowKeyIndicator();

        { // join and include columns

            joinColumnNames = Arrays.stream(joinClauses).distinct().toArray(String[]::new);
            joinColumnsSource = Arrays.stream(joinColumnNames).mapToInt(colName -> isRowKey.test(colName)
                ? JoinImplementation.ROW_KEY_COLUMN_INDEX_INDICATOR : table.getDataTableSpec().findColumnIndex(colName))
                .toArray();

            includeColumnNames = isLeft ? settings.getLeftIncludeCols() : settings.getRightIncludeCols();

        }
        { // working table specification

            Predicate<String> isIncludedColumn = s -> Arrays.asList(includeColumnNames).contains(s);
            Predicate<String> isJoinColumn = s -> Arrays.asList(joinClauses).contains(s);
            Predicate<String> isRowOffset = s -> JoinImplementation.ROW_OFFSET_COLUMN_NAME.equals(s);

            DataTableSpec spec = table.getDataTableSpec();

            List<DataColumnSpec> workingTableColumns = spec.stream()
                .filter(col -> isIncludedColumn.or(isJoinColumn).test(col.getName())).collect(Collectors.toList());
//            if (storeRowOffsets) {
//                // FIXME make sure this name is not taken
//                DataColumnSpec rowOffsetColumn =
//                    (new DataColumnSpecCreator(JoinImplementation.ROW_OFFSET_COLUMN_NAME, LongCell.TYPE)).createSpec();
//                workingTableColumns.add(0, rowOffsetColumn);
//            }
            workingTableSpec =
                new DataTableSpec(workingTableColumns.toArray(new DataColumnSpec[workingTableColumns.size()]));

            workingTableColumnToSource = workingTableSpec.stream().map(DataColumnSpec::getName)
                .filter(isRowOffset.negate()).mapToInt(spec::findColumnIndex).toArray();
            assert Arrays.stream(workingTableColumnToSource).noneMatch(i -> i < 0);

            joinColumnsWorkingTable = Arrays
                .stream(joinColumnNames).mapToInt(colName -> isRowKey.test(colName)
                    ? JoinImplementation.ROW_KEY_COLUMN_INDEX_INDICATOR : workingTableSpec.findColumnIndex(colName))
                .toArray();

            includeColumnsWorkingTable = workingTableSpec.columnsToIndices(includeColumnNames);

            includeColumns = spec.columnsToIndices(includeColumnNames);
            materializeColumnIndices = IntStream.concat(Arrays.stream(joinColumnsSource), Arrays.stream(includeColumns))
                .distinct().filter(i -> i >= 0).sorted().toArray();

            materializedCells = materializeColumnIndices.length * table.size();
        }

    }

    /**
     * Join columns can contain non-existing column names (e.g., $Row Key$) to indicate joining on row keys.
     *
     * @param spec
     * @return
     */
    //        int[] findJoinColumns(final DataTableSpec spec) {
    //            return Arrays.stream(joinClauses).mapToInt(name -> {
    //                int col = spec.findColumnIndex(name);
    //                return m_settings.getRowKeyIndicator().equals(name) ? ROW_KEY_COLUMN_INDEX_INDICATOR : col;
    //            }).toArray();
    //        }

    /**
     * Translate column names to column indices, use the special index {@link ROW_KEY_COLUMN_INDEX_INDICATOR} for the
     * row key column.
     *
     * @param spec the spec in which to look up the names.
     * @return
     */
    //        int[] joinColumnIndicesWithIndicator(final DataTableSpec spec) {
    //            return Arrays.stream(joinClauses).mapToInt(name -> {
    //                int col = spec.findColumnIndex(name);
    //                return m_settings.getRowKeyIndicator().equals(name) ? ROW_KEY_COLUMN_INDEX_INDICATOR : col;
    //            }).toArray();
    //        }
    //        int[] joinColumnIndicesWithIndicator() { return joinColumnIndicesWithIndicator(m_forTable.getDataTableSpec()); }
    /**
     * @param hashRow
     * @return
     */
    public JoinTuple getJoinTuple(final DataRow hashRow) {
        DataCell[] cells = new DataCell[joinColumnsSource.length];
        for (int i = 0; i < cells.length; i++) {
            cells[i] = joinColumnsSource[i] == JoinImplementation.ROW_KEY_COLUMN_INDEX_INDICATOR
                ? new StringCell(hashRow.getKey().getString()) : hashRow.getCell(joinColumnsSource[i]);
        }
        return new JoinTuple(cells);
    }

    /**
     * Copy selected cells from contents to save heap -- however, this pays off only if the working columns are fewer
     * than the original columns
     *
     * @param rowOffset
     * @param fullRow
     * @return
     */
//    public DataRow workingRow(final long rowOffset, final DataRow fullRow) {
//        DataCell[] cells = new DataCell[workingTableSpec.getNumColumns()];
//        int cell = 0;
//        if (storeRowOffsets) {
//            cells[cell++] = new LongCell(rowOffset);
//        }
//        for (int i = 0; i < workingTableColumnToSource.length; i++) {
//            cells[cell++] = fullRow.getCell(workingTableColumnToSource[i]);
//        }
//        return new DefaultRow(fullRow.getKey(), cells);
//    }

    /**
     * Extract the values from the join columns from the row.
     *
     * @param row
     * @param table
     * @return
     */
    protected JoinTuple getJoinTupleWorkingRow(final DataRow workingRow) {
        // TODO maybe implement a DataRow that allows to reuse the DataCell[] array that stores the join column values by internally concatenating them
        int[] indices = joinColumnsWorkingTable;
        DataCell[] cells = new DataCell[indices.length];
        for (int i = 0; i < cells.length; i++) {
            if (indices[i] >= 0) {
                cells[i] = workingRow.getCell(indices[i]);
            } else if (indices[i] == JoinImplementation.ROW_KEY_COLUMN_INDEX_INDICATOR) {
                // create a StringCell since row IDs may match
                // StringCell's
                cells[i] = new StringCell(workingRow.getKey().getString());
            }
        }
        return new JoinTuple(cells);
    }

    DataTableSpec outputTableSpecWith(final JoinTableSettings other) {
        // look up the data column specifications in the left table by their name
        DataTableSpec leftSpec = this.isLeft ? this.m_forTable.getDataTableSpec() : other.m_forTable.getDataTableSpec();
        Stream<DataColumnSpec> leftColumnSpecs =
            Arrays.stream(m_settings.getLeftIncludeCols()).map(name -> leftSpec.getColumnSpec(name));

        // look up the right data column specifications by name, change names if they clash with column name from left table
        DataTableSpec rightSpec =
            !this.isLeft ? this.m_forTable.getDataTableSpec() : other.m_forTable.getDataTableSpec();
        List<DataColumnSpec> rightColSpecs = new ArrayList<>();

        // look up column specs for names and change column names to getRightTargetColumnNames
        for (int i = 0; i < m_settings.getRightIncludeCols().length; i++) {
            String name = m_settings.getRightIncludeCols()[i];
            DataColumnSpecCreator a = new DataColumnSpecCreator(rightSpec.getColumnSpec(name));
            // make unique against column names that are already taken by the left table
            if (leftSpec.containsName(name)) {
                // change name to disambiguate, e.g., append a suffix
                a.setName(m_settings.transformName(rightSpec, name));
            }
            a.removeAllHandlers();
            rightColSpecs.add(a.createSpec());
        }

        DataTableSpec dataTableSpec =
            new DataTableSpec(Stream.concat(leftColumnSpecs, rightColSpecs.stream()).toArray(DataColumnSpec[]::new));
        return dataTableSpec;
    }

    /**
     * @param innerSettings
     * @return
     */
    DataTableSpec workingTableWith(final JoinTableSettings innerSettings) {
        return new DataTableSpec(
            Stream.concat(workingTableSpec.stream(), innerSettings.workingTableSpec.stream().skip(1))
                .toArray(DataColumnSpec[]::new));
    }

    /**
     * Contains probe row offset in first cell and the result columns
     *
     * @param innerSettings
     * @return
     */
    DataTableSpec sortedChunksTableWith(final JoinTableSettings innerSettings) {
        // TODO factor out
        DataColumnSpec rowOffset =
            (new DataColumnSpecCreator(JoinImplementation.ROW_OFFSET_COLUMN_NAME, LongCell.TYPE)).createSpec();
        return new DataTableSpec(Stream.concat(Stream.of(rowOffset), outputTableSpecWith(innerSettings).stream())
            .toArray(DataColumnSpec[]::new));
    }

    /**
     * @param bufferedDataTable
     * @return
     */
    public JoinTableSettings workingTableSettings(final BufferedDataTable bufferedDataTable) {
        return new JoinTableSettings(m_settings, isLeft, bufferedDataTable);
    }

}