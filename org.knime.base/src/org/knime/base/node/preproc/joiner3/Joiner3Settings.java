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
 * ---------------------------------------------------------------------
 *
 * History
 *   27.07.2007 (thor): created
 */
package org.knime.base.node.preproc.joiner3;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.knime.base.node.preproc.joiner3.implementation.JoinTuple;
import org.knime.base.node.preproc.joiner3.implementation.JoinerFactory;
import org.knime.base.node.preproc.joiner3.implementation.JoinerFactory.JoinAlgorithm;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.util.ConvenienceMethods;

/**
 * This class hold the settings for the joiner node.
 *
 * @author Heiko Hofer
 */
public class Joiner3Settings {

    /**
     * Defines a mapping from a row in a table to a representation of the values of its join columns.
     * Useful for an equijoin where the extracted representation has to be tested with equals()
     */
    public interface Extractor extends Function<DataRow, JoinTuple> {}

    final SettingsModelIntegerBounded m_memoryLimitPercentSettingsModel = new SettingsModelIntegerBounded("memoryLimitPercent", 90, 1, 100);

    final SettingsModelBoolean m_unmatchedRowsToSeparateOutputPort = new SettingsModelBoolean("unmatchedRowsToSeparateOutputPort", false);

    private static final String COPOSITION_MODE = "compositionMode";

    private static final String JOIN_MODE = "joinMode";

    private static final String LEFT_JOINING_COLUMNS = "leftTableJoinPredicate";

    private static final String RIGHT_JOINING_COLUMNS = "rightTableJoinPredicate";

    private static final String DUPLICATE_COLUMN_HANDLING = "duplicateHandling";

    private static final String DUPLICATE_COLUMN_SUFFIX = "suffix";

    private static final String LEFT_INCLUDE_COLUMNS = "leftIncludeCols";

    private static final String RIGHT_INCLUDE_COLUMNS = "rightIncludeCols";

    private static final String MAX_OPEN_FILES = "maxOpenFiles";

    private static final String ROW_KEY_SEPARATOR = "rowKeySeparator";

    private static final String ENABLE_HILITE = "enableHiLite";

    private static final String VERSION = "version";

    private static final String JOIN_ALGORITHM_KEY = "joinAlgorithm";

    private JoinerFactory.JoinAlgorithm m_joinAlgorithm = JoinerFactory.JoinAlgorithm.AUTO;

    /**
     * The version for Joiner nodes until KNIME v2.6.
     *
     * @since 2.7
     */
    public static final String VERSION_1 = "version_1";

    /**
     * The version for Joiner nodes for KNIME v2.7.
     *
     * @since 2.7
     */
    public static final String VERSION_2 = "version_2";

    /**
     * The version for Joiner nodes for KNIME v2.7.1 and later. Partial revert of Bug 3368 (see Bug 3916).
     *
     * @since 2.7
     */
    public static final String VERSION_2_1 = "version_2.1";

    /**
     * The version for Joiner nodes for KNIME v2.8 and later.
     *
     * @since 2.9
     */
    public static final String VERSION_3 = "version_3";

    /**
     * This enum holds all ways of handling duplicate column names in the two
     * input tables.
     *
     * @author Thorsten Meinl, University of Konstanz
     */
    public enum DuplicateHandling {
        /** Filter out duplicate columns from the second table. */
        Filter,
        /** Append a suffix to the columns from the second table. */
        AppendSuffixAutomatic,
        /** Append a custom suffix to the columns from the second table. */
        AppendSuffix,
        /** Don't execute the node. */
        DontExecute;
    }

    /**
     * This enum holds all ways of joining the two tables.
     *
     * @author Thorsten Meinl, University of Konstanz
     */
    public enum JoinMode {
        /** Make an INNER JOIN. */
        InnerJoin("Inner Join"),
        /** Make a LEFT OUTER JOIN. */
        LeftOuterJoin("Left Outer Join"),
        /** Make a RIGHT OUTER JOIN. */
        RightOuterJoin("Right Outer Join"),
        /** Make a FULL OUTER JOIN. */
        FullOuterJoin("Full Outer Join");

        private final String m_text;

        private JoinMode(final String text) {
            m_text = text;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            return m_text;
        }
    }

    /**
     * This enum holds all ways how join attributes can be combined.
     *
     * @author Heiko Hofer
     */
    public enum CompositionMode {
        /** Join when all join attributes match (logical and). */
        MatchAll,
        /** Join when at least one join attribute matches (logical or). */
        MatchAny;
    }


    /** Internally used row key identifier. */
    public static final String ROW_KEY_IDENTIFIER = "$RowID$";



    private DuplicateHandling m_duplicateHandling = DuplicateHandling.AppendSuffixAutomatic;

    private String m_duplicateColSuffix = "(*)";

    private JoinMode m_joinMode = JoinMode.InnerJoin;

    private String[] m_leftJoinColumns = new String[0];
    private String[] m_rightJoinColumns = new String[0];
    private CompositionMode m_compositionMode = CompositionMode.MatchAll;

    private String[] m_leftIncludeCols = new String[0];
    private String[]  m_rightIncludeCols = new String[0];

    private int m_maxOpenFiles = 200;
    private String m_rowKeySeparator = "_";
    private boolean m_enableHiLite = false;

    private String m_version = VERSION_3;


    public void validateSettings() throws InvalidSettingsException {
        if (getDuplicateHandling() == null) {
            throw new InvalidSettingsException("No duplicate handling method selected");
        }
        if (getJoinMode() == null) {
            throw new InvalidSettingsException("No join mode selected");
        }
        if ((getLeftJoinColumns() == null) || getLeftJoinColumns().length < 1 || getRightJoinColumns() == null
            || getRightJoinColumns().length < 1) {
            throw new InvalidSettingsException("Please define at least one joining column pair.");
        }
        if (getLeftJoinColumns() != null && getRightJoinColumns() != null
            && getLeftJoinColumns().length != getRightJoinColumns().length) {
            throw new InvalidSettingsException(
                "Number of columns selected from the top table and from " + "the bottom table do not match");
        }

        if (getDuplicateHandling().equals(DuplicateHandling.AppendSuffix)
            && (getDuplicateColumnSuffix() == null || getDuplicateColumnSuffix().isEmpty())) {
            throw new InvalidSettingsException("No suffix for duplicate columns provided");
        }
        if (getMaxOpenFiles() < 3) {
            throw new InvalidSettingsException("Maximum number of open files must be at least 3.");
        }

    }

    /**
     *
     * @param dataTableSpec input spec of the left DataTable
     * @return the names of all columns to include from the left input table
     * @throws InvalidSettingsException if the input spec is not compatible with the settings
     * getJoinColumns -> getLeftJoinColumns
     * getIncludeColumns -> getLeftIncludeCols
     * includeAllColumns -> getLeftIncludeAll
     * removeJoinColumns -> getRemoveLeftJoinCols
     * @since 2.12
     * TODO replace with multiple tables version
     */
    @Deprecated
    static List<String> getIncluded(final DataTableSpec dataTableSpec, final Joiner3Settings settings,
        final Function<Joiner3Settings, String[]> getJoinColumns,
        final Function<Joiner3Settings, String[]> getIncludeColumns,
        final Predicate<Joiner3Settings> includeAllColumns,
        final Predicate<Joiner3Settings> removeJoinColumns
        )
        throws InvalidSettingsException {

        // add all columns
        List<String> result = dataTableSpec.stream()
                .map(DataColumnSpec::getName)
                .collect(Collectors.toList());

        // Check if left joining columns are in table spec
        Set<String> leftJoinCols = new HashSet<String>();


        String[] joinColumnNames = getJoinColumns.apply(settings);

        //
        leftJoinCols.addAll(Arrays.asList(joinColumnNames));

        //
        leftJoinCols.remove(Joiner3Settings.ROW_KEY_IDENTIFIER);

        if (!result.containsAll(leftJoinCols)) {
            leftJoinCols.removeAll(result);
            throw new InvalidSettingsException(
                "The top input table has " + "changed. Some joining columns are missing: "
                    + ConvenienceMethods.getShortStringFrom(leftJoinCols, 3));
        }

        // if only some columns are included,
        if (!includeAllColumns.test(settings)) {
            List<String> leftIncludes = Arrays.asList(getIncludeColumns.apply(settings));
            result.retainAll(leftIncludes);
        }

        if (removeJoinColumns.test(settings)) {
            result.removeAll(Arrays.asList(joinColumnNames));
        }
        return result;
    }

//    /**
//     * @param dataTableSpec input spec of the left DataTable
//     * @return the names of all columns to include from the left input table
//     * @throws InvalidSettingsException if the input spec is not compatible with the settings
//     * @since 2.12
//     * TODO replace with multiple tables version
//     */
//    @Deprecated
//    public List<String> getLeftIncluded(final DataTableSpec dataTableSpec)
//    throws InvalidSettingsException {
//        List<String> leftCols = new ArrayList<String>();
//        for (DataColumnSpec column : dataTableSpec) {
//            leftCols.add(column.getName());
//        }
//        // Check if left joining columns are in table spec
//        Set<String> leftJoinCols = new HashSet<String>();
//        leftJoinCols.addAll(Arrays.asList(getLeftJoinColumns()));
//        leftJoinCols.remove(Joiner3Settings.ROW_KEY_IDENTIFIER);
//        if (!leftCols.containsAll(leftJoinCols)) {
//            leftJoinCols.removeAll(leftCols);
//            throw new InvalidSettingsException("The top input table has "
//               + "changed. Some joining columns are missing: "
//               + ConvenienceMethods.getShortStringFrom(leftJoinCols, 3));
//        }
//
//        if (!getLeftIncludeAll()) {
//            List<String> leftIncludes =
//                Arrays.asList(getLeftIncludeCols());
//            leftCols.retainAll(leftIncludes);
//        }
//        if (getRemoveLeftJoinCols()) {
//            leftCols.removeAll(Arrays.asList(getLeftJoinColumns()));
//        }
//        return leftCols;
//    }

//    /**
//     * @param dataTableSpec input spec of the right DataTable
//     * @return the names of all columns to include from the left input table
//     * @throws InvalidSettingsException if the input spec is not compatible with the settings
//     * @since 2.12
//     * TODO replace with multiple tables version
//     */
//    @Deprecated
//    public List<String> getRightIncluded(final DataTableSpec dataTableSpec)
//        throws InvalidSettingsException {
//        List<String> rightCols = new ArrayList<String>();
//        for (DataColumnSpec column : dataTableSpec) {
//            rightCols.add(column.getName());
//        }
//        // Check if right joining columns are in table spec
//        Set<String> rightJoinCols = new HashSet<String>();
//        rightJoinCols.addAll(Arrays.asList(getRightJoinColumns()));
//        rightJoinCols.remove(Joiner3Settings.ROW_KEY_IDENTIFIER);
//        if (!rightCols.containsAll(rightJoinCols)) {
//            rightJoinCols.removeAll(rightCols);
//            throw new InvalidSettingsException(
//                "The bottom input table has " + "changed. Some joining columns are missing: "
//                    + ConvenienceMethods.getShortStringFrom(rightJoinCols, 3));
//        }
//
//        if (!getRightIncludeAll()) {
//            List<String> rightIncludes = Arrays.asList(getRightIncludeCols());
//            rightCols.retainAll(rightIncludes);
//        }
//        if (getRemoveRightJoinCols()) {
//            rightCols.removeAll(Arrays.asList(getRightJoinColumns()));
//        }
//        return rightCols;
//    }

    /**
     * TODO generalize to multiple tables
     * @param tableIdx 0: first table (outer/left table in the conventional case); 1: second (first inner), third (second inner), etc
     * @return whether to include unmatched rows from that table in the composite table (with missing values in the
     *         columns of the other tables)
     */
    public boolean outputUnmatchedRows(final int tableIdx) {
        // full outer join: retain unmatched rows from all tables
        // left outer join: retain only unmatched rows from table 0
        // right outer join: retain only unmatched rows from table 1
        // inner join: return no table
        switch (getJoinMode()) {
            case FullOuterJoin:
                return true;
            case LeftOuterJoin:
                return tableIdx == 0;
            case RightOuterJoin:
                return tableIdx == 1;
            case InnerJoin:
                return false;
        }
        // TODO
        throw new IllegalStateException();
    }

    /**
     * Returns true when the enhance RowID handling introduced in KNIME 2.8
     * should be used.
     *
     * @return true when the enhance RowID handling should be used
     * @since 2.9
     *
     */
    public boolean useEnhancedRowIdHandling() {
        return !(m_version.equals(VERSION_1) || m_version.equals(VERSION_2) || m_version.equals(VERSION_2_1));
    }

    /**
     * Set the version either VERSION_1, VERSION_2 or VERSION_3.
     *
     * @param version the version to set
     * @since 2.7
     */
    public void setVersion(final String version) {
        m_version = version;
    }

    /**
     * Returns the columns of the left table used in the join predicate.
     *
     * @return the leftJoinColumns
     */
    public String[] getLeftJoinColumns() {
        return m_leftJoinColumns;
    }

    /**
     * Sets the columns of the left table used in the join predicate.
     *
     * @param leftJoinColumns the leftJoinColumns to set
     */
    public void setLeftJoinColumns(final String[] leftJoinColumns) {
        m_leftJoinColumns = leftJoinColumns;
    }

    /**
     * Returns the columns of the right table used in the join predicate.
     *
     * @return the rightJoinColumns
     */
    public String[] getRightJoinColumns() {
        return m_rightJoinColumns;
    }

    /**
     * Sets the columns of the right table used in the join predicate.
     *
     * @param rightJoinColumns the rightJoinColumns to set
     */
    public void setRightJoinColumns(final String[] rightJoinColumns) {
        m_rightJoinColumns = rightJoinColumns;
    }


    /**
     * @return the compositionMode
     */
    public CompositionMode getCompositionMode() {
        return m_compositionMode;
    }

    /**
     * @param compositionMode the compositionMode to set
     */
    public void setCompositionMode(final CompositionMode compositionMode) {
        m_compositionMode = compositionMode;
    }

    /**
     * Returns how duplicate column names should be handled.
     *
     * @return the duplicate handling method
     */
    public DuplicateHandling getDuplicateHandling() {
        return m_duplicateHandling;
    }

    /**
     * Sets how duplicate column names should be handled.
     *
     * @param duplicateHandling the duplicate handling method
     */
    public void setDuplicateHandling(
            final DuplicateHandling duplicateHandling) {
        m_duplicateHandling = duplicateHandling;
    }

    /**
     * Returns the mode how the two tables should be joined.
     *
     * @return the join mode
     */
    public JoinMode getJoinMode() {
        return m_joinMode;
    }

    /**
     * Sets the mode how the two tables should be joined.
     *
     * @param joinMode the join mode
     */
    public void setJoinMode(final JoinMode joinMode) {
        m_joinMode = joinMode;
    }

    /**
     * Returns the suffix that is appended to duplicate columns from the right
     * table if the duplicate handling method is
     * <code>JoinMode.AppendSuffix</code>.
     *
     * @return the suffix
     */
    public String getDuplicateColumnSuffix() {
        return m_duplicateColSuffix;
    }

    /**
     * Sets the suffix that is appended to duplicate columns from the right
     * table if the duplicate handling method is
     * <code>JoinMode.AppendSuffix</code>.
     *
     * @param suffix the suffix
     */
    public void setDuplicateColumnSuffix(final String suffix) {
        m_duplicateColSuffix = suffix;
    }



    /**
     * Returns the columns of the left table that should be included to the
     * joining table.
     *
     * @return the leftIncludeCols
     */
    public String[] getLeftIncludeCols() {
        return m_leftIncludeCols;
    }

    /**
     * Sets the columns of the left table that should be included to the
     * joining table.
     *
     * @param leftIncludeCols the leftIncludeCols to set
     */
    public void setLeftIncludeCols(final String[] leftIncludeCols) {
        m_leftIncludeCols = leftIncludeCols;
    }

    /**
     * Returns the columns of the right table that should be included to the
     * joining table.
     *
     * @return the rightIncludeCols
     */
    public String[] getRightIncludeCols() {
        return m_rightIncludeCols;
    }

    /**
     * Sets the columns of the right table that should be included to the
     * joining table.
     *
     * @param rightIncludeCols the rightIncludeCols to set
     */
    public void setRightIncludeCols(final String[] rightIncludeCols) {
        m_rightIncludeCols = rightIncludeCols;
    }

    /**
     * Return number of files that are allowed to be openend by the Joiner.
     *
     * @return the maxOpenFiles
     */
    public int getMaxOpenFiles() {
        return m_maxOpenFiles;
    }

    /**
     * Set number of files that are allowed to be openend by the Joiner.
     *
     * @param maxOpenFiles the maxOpenFiles to set
     */
    public void setMaxOpenFiles(final int maxOpenFiles) {
        m_maxOpenFiles = maxOpenFiles;
    }

    /**
     * Return Separator of the RowKeys in the joined table.
     *
     * @return the rowKeySeparator
     */
    public String getRowKeySeparator() {
        return m_rowKeySeparator;
    }

    /**
     * Set Separator of the RowKeys in the joined table.
     *
     * @param rowKeySeparator the rowKeySeparator to set
     */
    public void setRowKeySeparator(final String rowKeySeparator) {
        m_rowKeySeparator = rowKeySeparator;
    }

    /**
     * Returns true when hiliting should be supported.
     *
     * @return the enableHiLite
     */
    public boolean getEnableHiLite() {
        return m_enableHiLite;
    }

    /**
     * Set if hiliting should be supported.
     *
     * @param enableHiLite the enableHiLite to set
     */
    public void setEnableHiLite(final boolean enableHiLite) {
        m_enableHiLite = enableHiLite;
    }

    /**
     * Loads the settings from the node settings object.
     *
     * @param settings a node settings object
     * @throws InvalidSettingsException if some settings are missing
     */
    public void loadSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        // load version introduced in 2.7
        m_joinAlgorithm = JoinAlgorithm.valueOf(settings.getString(JOIN_ALGORITHM_KEY));

        m_version = settings.getString(VERSION, VERSION_1);

        m_memoryLimitPercentSettingsModel.loadSettingsFrom(settings);
        m_unmatchedRowsToSeparateOutputPort.loadSettingsFrom(settings);

        m_duplicateHandling =
                DuplicateHandling.valueOf(settings
                        .getString(DUPLICATE_COLUMN_HANDLING));
        if (m_version.equals(VERSION_2)
                && m_duplicateHandling.equals(DuplicateHandling.AppendSuffix)) {
            m_duplicateHandling = DuplicateHandling.AppendSuffixAutomatic;
        }
        m_compositionMode =
            CompositionMode.valueOf(settings.getString(COPOSITION_MODE));
        m_joinMode = JoinMode.valueOf(settings.getString(JOIN_MODE));


        m_leftJoinColumns =
            settings.getStringArray(LEFT_JOINING_COLUMNS);
        m_rightJoinColumns =
            settings.getStringArray(RIGHT_JOINING_COLUMNS);
        m_duplicateColSuffix = settings.getString(DUPLICATE_COLUMN_SUFFIX);
        m_leftIncludeCols = settings.getStringArray(LEFT_INCLUDE_COLUMNS);
        m_rightIncludeCols = settings.getStringArray(RIGHT_INCLUDE_COLUMNS);
        m_maxOpenFiles = settings.getInt(MAX_OPEN_FILES);
        m_rowKeySeparator = settings.getString(ROW_KEY_SEPARATOR);
        m_enableHiLite = settings.getBoolean(ENABLE_HILITE);


    }

    /**
     * Loads the settings from the node settings object using default values if
     * some settings are missing.
     *
     * @param settings a node settings object
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        // load version introduced in 2.7
        m_version = settings.getString(VERSION, VERSION_1);

        m_joinAlgorithm = JoinAlgorithm.valueOf(settings.getString(JOIN_ALGORITHM_KEY, m_joinAlgorithm.name()));

        try {
            m_memoryLimitPercentSettingsModel.loadSettingsFrom(settings);
            m_unmatchedRowsToSeparateOutputPort.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            System.err.println(e.getLocalizedMessage());
        }

        m_duplicateHandling = DuplicateHandling
            .valueOf(settings.getString(DUPLICATE_COLUMN_HANDLING, DuplicateHandling.AppendSuffix.toString()));
        if (m_version.equals(VERSION_2) && m_duplicateHandling.equals(DuplicateHandling.AppendSuffix)) {
            m_duplicateHandling = DuplicateHandling.AppendSuffixAutomatic;
        }
        m_compositionMode =
            CompositionMode.valueOf(settings.getString(COPOSITION_MODE, CompositionMode.MatchAll.toString()));
        m_joinMode = JoinMode.valueOf(settings.getString(JOIN_MODE, JoinMode.InnerJoin.toString()));
        m_leftJoinColumns =
            settings.getStringArray(LEFT_JOINING_COLUMNS, new String[]{Joiner3Settings.ROW_KEY_IDENTIFIER});
        m_rightJoinColumns =
            settings.getStringArray(RIGHT_JOINING_COLUMNS, new String[]{Joiner3Settings.ROW_KEY_IDENTIFIER});
        m_duplicateColSuffix = settings.getString(DUPLICATE_COLUMN_SUFFIX, "(*)");

        m_leftIncludeCols = settings.getStringArray(LEFT_INCLUDE_COLUMNS, new String[0]);
        m_rightIncludeCols = settings.getStringArray(RIGHT_INCLUDE_COLUMNS, new String[0]);

        m_maxOpenFiles = settings.getInt(MAX_OPEN_FILES, 200);
        m_rowKeySeparator = settings.getString(ROW_KEY_SEPARATOR, "_");
        m_enableHiLite = settings.getBoolean(ENABLE_HILITE, false);


    }

    /**
     * Saves the settings into the node settings object.
     *
     * @param settings a node settings object
     */
    public void saveSettings(final NodeSettingsWO settings) {

        settings.addString(JOIN_ALGORITHM_KEY, m_joinAlgorithm.name());
        m_memoryLimitPercentSettingsModel.saveSettingsTo(settings);
        m_unmatchedRowsToSeparateOutputPort.saveSettingsTo(settings);

        settings.addString(DUPLICATE_COLUMN_HANDLING,
                m_duplicateHandling.toString());
        settings.addString(COPOSITION_MODE, m_compositionMode.toString());
        settings.addString(JOIN_MODE, m_joinMode.name());
        settings.addStringArray(LEFT_JOINING_COLUMNS, m_leftJoinColumns);
        settings.addStringArray(RIGHT_JOINING_COLUMNS, m_rightJoinColumns);
        settings.addString(DUPLICATE_COLUMN_SUFFIX, m_duplicateColSuffix);
        settings.addStringArray(LEFT_INCLUDE_COLUMNS, m_leftIncludeCols);
        settings.addStringArray(RIGHT_INCLUDE_COLUMNS, m_rightIncludeCols);
        settings.addInt(MAX_OPEN_FILES, m_maxOpenFiles);
        settings.addString(ROW_KEY_SEPARATOR, m_rowKeySeparator);
        settings.addBoolean(ENABLE_HILITE, m_enableHiLite);
        // save default values for settings that were removed in 2.5, so that
        // a workflow created with 2.5 can be opened in 2.4.
        // TODO review
        settings.addDouble("usedMemoryThreshold", 0.85);
        settings.addLong("minAvailableMemory", 10000000L);
        settings.addBoolean("memUseCollectionUsage", true);

        // save version introduced in 2.7
        settings.addString(VERSION, m_version);
    }

    public JoinAlgorithm getJoinAlgorithm() {
        return m_joinAlgorithm;
    }

    public void setJoinAlgorithm(final JoinAlgorithm joinAlgorithm) {
        m_joinAlgorithm = joinAlgorithm;
    }

    public int getMemoryLimitPercent() {
        return m_memoryLimitPercentSettingsModel.getIntValue();
    }

    public boolean isUnmatchedRowsToSeparateOutputPort() {
        return m_unmatchedRowsToSeparateOutputPort.getBooleanValue();
    }

    public String[] getRightTargetColumnNames() {
        return getRightIncludeCols();
    }

    // TODO configure this
    // called if the column name is already taken by the outer table, must never return the same name again
    BiFunction<DataTableSpec, String, String> m_nameTransfomer = (s, n) -> n+" (#1)";

    public String transformName(final DataTableSpec tableSpec, final String name) {
        return m_nameTransfomer.apply(tableSpec, name);
    }

    /**
     * This column name is used to indicate a join on a row key (which is not an actual column).
     */
    private final String DEFAULT_ROW_KEY_INDICATOR = "$RowID$";

    public String getRowKeyIndicator() {
        String indicator = DEFAULT_ROW_KEY_INDICATOR;
        // add characters until column name is unique
        while ((Arrays.asList(getLeftIncludeCols()).contains(indicator)
            || Arrays.asList(getRightIncludeCols()).contains(indicator))) {
            indicator = "$" + indicator + "$";
        }
        return indicator;
    }
    public Predicate<String> isRowKeyIndicator(){
        return s -> getRowKeyIndicator().equals(s);
    }
}
