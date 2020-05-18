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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import org.knime.base.node.preproc.joiner3.Joiner3Settings;
import org.knime.base.node.preproc.joiner3.Joiner3Settings.DuplicateHandling;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.streamable.StreamableFunction;
import org.knime.core.util.UniqueNameGenerator;

/**
 *
 * Chooses join implementations based on {@link Joiner3Settings} and input data.
 * An instance of this class serves
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 *
 */
public class Joiner {

    /**
     * The algorithm used to perform the join.
     */
    JoinImplementation m_joinStrategy;

//    // TODO legacy
//    List<String> m_leftSurvivors;
//    // TODO legacy
//    List<String> m_rightSurvivors;

    public Joiner() {

    }

    /**
     * @param settings
     * @param warningMessageHandler a consumer that accepts warnings created during configuration.
     * @param specs
     * @param inSpecs
     * @return
     * @throws InvalidSettingsException
     */
    public static DataTableSpec createOutputSpec(final Joiner3Settings settings,
        final Consumer<String> warningMessageHandler, final DataTableSpec... specs) throws InvalidSettingsException {

            settings.validateSettings();


            // concatenate all columns into one long new specification
            // e.g., (age, income, height) ⨝ (age, education, sex, income) ->
            //  (age, income, height, age, education, sex, income)

//            DataColumnSpec[] concatenated =
//                Arrays.stream(specs).flatMap(DataTableSpec::stream).toArray(DataColumnSpec[]::new);

//            return new DataTableSpec(concatenated);

            List<String> leftCols = settings.getLeftIncluded(specs[0]);
            List<String> rightCols = settings.getRightIncluded(specs[1]);

            // check if data types of joining columns do match
            // throw InvalidSettingsException otherwise
            checkJoinColumnTypeCompatibility(settings, specs);
            // remove left columns from right columns if filter duplicates is active
            duplicateHandling(settings, warningMessageHandler, leftCols, rightCols, specs);

            @SuppressWarnings("unchecked")
            UniqueNameGenerator nameGen = new UniqueNameGenerator(Collections.EMPTY_SET);
            List<String> m_leftSurvivors = new ArrayList<String>();

            List<DataColumnSpec> outColSpecs = new ArrayList<DataColumnSpec>();
            for (int i = 0; i < specs[0].getNumColumns(); i++) {
                DataColumnSpec columnSpec = specs[0].getColumnSpec(i);
                if (leftCols.contains(columnSpec.getName())) {
                    outColSpecs.add(columnSpec);
                    nameGen.newName(columnSpec.getName());
                    m_leftSurvivors.add(columnSpec.getName());
                }
            }

            List<String> m_rightSurvivors = new ArrayList<String>();
            for (int i = 0; i < specs[1].getNumColumns(); i++) {
                DataColumnSpec columnSpec = specs[1].getColumnSpec(i);
                if (rightCols.contains(columnSpec.getName())) {
                    if (settings.getDuplicateHandling().equals(DuplicateHandling.AppendSuffix)) {
                        if (m_leftSurvivors.contains(columnSpec.getName())
                            || m_rightSurvivors.contains(columnSpec.getName())) {
                            String newName = columnSpec.getName();
                            do {
                                newName += settings.getDuplicateColumnSuffix();
                            } while (m_leftSurvivors.contains(newName) || m_rightSurvivors.contains(newName));

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
                    m_rightSurvivors.add(columnSpec.getName());
                }
            }

            return new DataTableSpec(outColSpecs.toArray(new DataColumnSpec[outColSpecs.size()]));
        }

    /**
     * Disambiguate or remove columns depending on duplicate handling.
     */
    private static void duplicateHandling(final Joiner3Settings settings, final Consumer<String> warningMessageHandler,
        final List<String> leftCols, final List<String> rightCols, final DataTableSpec... specs)
        throws InvalidSettingsException {

        List<String> duplicates = new ArrayList<String>(leftCols);
        // if leftCols and rightCols have no overlap, duplicates will be empty
        duplicates.retainAll(rightCols);


        switch (settings.getDuplicateHandling()) {
            case DontExecute:
                // TODO this is validation, move somewhere else
                if(!duplicates.isEmpty()) {
                    throw new InvalidSettingsException(
                        "Found duplicate columns, won't execute. Fix it in " + "\"Column Selection\" tab");
                }
                break;
            case Filter:
                // TODO this is validation, move somewhere else
                for (String duplicate : duplicates) {
                    DataType leftType = specs[0].getColumnSpec(duplicate).getType();
                    DataType rightType = specs[1].getColumnSpec(duplicate).getType();
                    if (!leftType.equals(rightType)) {
                        warningMessageHandler.accept("The column \"" + duplicate + "\" can be found in "
                            + "both input tables but with different data type. "
                            + "Only the one in the top input table will show "
                            + "up in the output table. Please change the "
                            + "Duplicate Column Handling if both columns " + "should show up in the output table.");
                    }
                }
                // this is the only actual action taken here
                rightCols.removeAll(leftCols);
                break;
            case AppendSuffix:
                // TODO this is validation, move somewhere else
                if ((!duplicates.isEmpty()) && (settings.getDuplicateColumnSuffix() == null
                    || settings.getDuplicateColumnSuffix().equals(""))) {
                    throw new InvalidSettingsException("No suffix for duplicate columns provided.");
                }
                break;
            case AppendSuffixAutomatic:
                break;
            default:
                break;
        }

    }

    /**
     * @param settings
     * @param specs
     * @throws InvalidSettingsException
     */
    private static void checkJoinColumnTypeCompatibility(final Joiner3Settings settings, final DataTableSpec... specs)
        throws InvalidSettingsException {
        for (int i = 0; i < settings.getLeftJoinColumns().length; i++) {
            String leftJoinAttr = settings.getLeftJoinColumns()[i];
            boolean leftJoinAttrIsRowKey =
                Joiner3Settings.ROW_KEY_IDENTIFIER.equals(leftJoinAttr);
            DataType leftType = leftJoinAttrIsRowKey
            ? StringCell.TYPE
                    : specs[0].getColumnSpec(leftJoinAttr).getType();
            String rightJoinAttr = settings.getRightJoinColumns()[i];
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
    }

    /**
     * Performs an n-way join between the given tables.
     * This may perform a series of two-way joins or use a direct implementation for n-way joins.
     *
     * @param settings Defines the join columns for each input table, join predicates for each column pair (equality,
     *            smaller equal, etc.), join mode (inner, outer), hints on which implementation to use, etc.
     * @param exec Allows creation of output data containers, accepts progress and status messages.
     * @param tables The tables to be joined according to the settings.
     * @return A composite table according to the settings.
     * @throws InvalidSettingsException
     * @throws CanceledExecutionException
     */
    public BufferedDataTable computeJoinTable(final Joiner3Settings settings, final ExecutionContext exec,
        final BufferedDataTable... tables) throws CanceledExecutionException, InvalidSettingsException {

        // TODO validate settings
        // TODO handle more than two tables
        // TODO reuse old data if only minor changes in settings
        m_joinStrategy = settings.getJoinAlgorithm().getFactory().create(settings, tables);

        return m_joinStrategy.twoWayJoin(exec, tables[0], tables[1]);
    }

    /**
     * @param inputSpecs
     * @param settings
     * @return
     */
    public StreamableFunction getStreamableFunction(final Joiner3Settings settings, final DataTableSpec[] inputSpecs) {
        // the old spec vs table problem -- here we only have the specs.
        // downgrading the general interface seem weird, though -- need sizes to choose implementation?
//        m_joinStrategy = settings.getJoinAlgorithm().getFactory().create(settings, inputSpecs);
        return m_joinStrategy.getStreamableFunction();
    }


}