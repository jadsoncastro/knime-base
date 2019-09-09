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
 *   Aug 28, 2019 (Simon Schmid, KNIME GmbH, Konstanz, Germany): created
 */
package org.knime.base.node.probdistribution.probdistributioncreator;

import org.knime.base.node.probdistribution.ExceptionHandling;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.MissingCell;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.SingleCellFactory;
import org.knime.core.data.probability.ProbabilityDistributionCellFactory;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnFilter2;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.streamable.simple.SimpleStreamableFunctionNodeModel;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.util.UniqueNameGenerator;

/**
 * Node model of the node that creates probability distributions of probability values.
 *
 * @author Simon Schmid, KNIME GmbH, Konstanz, Germany
 */
final class ProbabilityDistributionCreatorNodeModel extends SimpleStreamableFunctionNodeModel {

    private final SettingsModelString m_columnNameModel = createColumnNameModel();

    private final SettingsModelBoolean m_removeIncludedColumns = createRemoveIncludedColsBooleanModel();

    private final SettingsModelColumnFilter2 m_columnFilterModel = createColumnFilterModel();

    private final SettingsModelBoolean m_allowUnpreciseProbabilities = createPrecisionBooleanModel();

    private final SettingsModelIntegerBounded m_precisionModel = createPrecisionModel(m_allowUnpreciseProbabilities);

    private final SettingsModelString m_missingValueHandling = createMissingValueHandlingModel();

    private final SettingsModelString m_invalidDistributionHandling = createInvalidDistributionHandlingModel();

    static SettingsModelString createColumnNameModel() {
        return new SettingsModelString("column_name", "Probability Distribution");
    }

    static SettingsModelBoolean createRemoveIncludedColsBooleanModel() {
        return new SettingsModelBoolean("remove_included_columns", false);
    }

    @SuppressWarnings("unchecked")
    static SettingsModelColumnFilter2 createColumnFilterModel() {
        return new SettingsModelColumnFilter2("column_filter", DoubleValue.class);
    }

    static SettingsModelBoolean createPrecisionBooleanModel() {
        return new SettingsModelBoolean("enable_precision", true);
    }

    static SettingsModelIntegerBounded createPrecisionModel(final SettingsModelBoolean precisionBoolModel) {
        final SettingsModelIntegerBounded model = new SettingsModelIntegerBounded("precision", 4, 1, Integer.MAX_VALUE);
        model.setEnabled(precisionBoolModel.getBooleanValue());
        precisionBoolModel.addChangeListener(l -> model.setEnabled(precisionBoolModel.getBooleanValue()));
        return model;
    }

    static SettingsModelString createMissingValueHandlingModel() {
        return new SettingsModelString("missing_value_handling", MissingValueHandling.FAIL.name());
    }

    static SettingsModelString createInvalidDistributionHandlingModel() {
        return new SettingsModelString("invalid_distribution_handling", ExceptionHandling.FAIL.name());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected ColumnRearranger createColumnRearranger(final DataTableSpec spec) throws InvalidSettingsException {
        ColumnRearranger columnRearranger = new ColumnRearranger(spec);
        // get included columns
        final String[] includes = m_columnFilterModel.applyTo(spec).getIncludes();
        final int[] colIndices = spec.columnsToIndices(includes);
        CheckUtils.checkSetting(colIndices.length > 0, "At least one column must be selected.");

        // check input and create variables used by the cell factory
        CheckUtils.checkSetting(!m_columnNameModel.getStringValue().trim().isEmpty(),
            "The output column name must not be empty.");
        CheckUtils.checkSetting(m_precisionModel.getIntValue() > 0, "The number of decimal digits must be > 0.");
        final double epsilon =
            m_allowUnpreciseProbabilities.getBooleanValue() ? Math.pow(10, -m_precisionModel.getIntValue()) : 0;
        CheckUtils.checkSetting(epsilon >= 0, "Epsilon must not be negative.");
        final ExceptionHandling invalidDistributionHandling;
        final MissingValueHandling missingValueHandling;
        try {
            invalidDistributionHandling = ExceptionHandling.valueOf(m_invalidDistributionHandling.getStringValue());
            missingValueHandling = MissingValueHandling.valueOf(m_missingValueHandling.getStringValue());
        } catch (IllegalArgumentException e) {
            throw new InvalidSettingsException(
                "The selected missing value or invalid distribution handling strategy does not exist.");
        }

        // create column spec
        final DataColumnSpecCreator colSpecCreator = new UniqueNameGenerator(spec)
            .newCreator(m_columnNameModel.getStringValue(), ProbabilityDistributionCellFactory.TYPE);
        colSpecCreator.setElementNames(includes);
        if (m_removeIncludedColumns.getBooleanValue()) {
            columnRearranger.remove(colIndices);
        }
        columnRearranger.append(new SingleCellFactory(colSpecCreator.createSpec()) {

            boolean m_hasMissing = false;

            boolean m_hasInvalidDistribution = false;

            @Override
            public DataCell getCell(final DataRow row) {
                double[] values = new double[colIndices.length];
                int i = 0;
                for (final int idx : colIndices) {
                    final DataCell cell = row.getCell(idx);
                    if (cell.isMissing()) {
                        switch (missingValueHandling) {
                            case FAIL:
                                throw new IllegalArgumentException(
                                    "The row '" + row.getKey().getString() + "' contains missing values.");
                            case IGNORE:
                                // set the same warning only once
                                if (!m_hasMissing) {
                                    setWarningMessage(
                                        "At least one row contains a missing value. Missing values will be in the "
                                            + "output.");
                                    m_hasMissing = true;
                                }
                                return new MissingCell("Input row contains missing values.");
                            case ZERO:
                                // set the same warning only once
                                if (!m_hasMissing) {
                                    setWarningMessage(
                                        "At least one row contains a missing value. They have been treated as zeroes.");
                                    m_hasMissing = true;
                                }
                                values[i++] = 0;
                                continue;
                        }
                    }
                    values[i++] = ((DoubleValue)cell).getDoubleValue();
                }
                try {
                    return ProbabilityDistributionCellFactory.createCell(values, epsilon);
                } catch (IllegalArgumentException e) {
                    if (invalidDistributionHandling == ExceptionHandling.FAIL) {
                        throw new IllegalArgumentException(
                            "The distribution of row '" + row.getKey().getString() + "' is invalid: " + e.getMessage());
                    } else {
                        // set the same warning only once
                        if (!m_hasInvalidDistribution) {
                            setWarningMessage(
                                "The distribution of at least one row is invalid. Missing values will be in the output."
                                    + " Hovering over the missing values display more details.");
                            m_hasInvalidDistribution = true;
                        }
                        return new MissingCell(e.getMessage());
                    }
                }
            }
        });
        return columnRearranger;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_columnFilterModel.saveSettingsTo(settings);
        m_precisionModel.saveSettingsTo(settings);
        m_columnNameModel.saveSettingsTo(settings);
        m_allowUnpreciseProbabilities.saveSettingsTo(settings);
        m_missingValueHandling.saveSettingsTo(settings);
        m_invalidDistributionHandling.saveSettingsTo(settings);
        m_removeIncludedColumns.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_columnFilterModel.validateSettings(settings);
        m_precisionModel.validateSettings(settings);
        m_columnNameModel.validateSettings(settings);
        m_allowUnpreciseProbabilities.validateSettings(settings);
        m_missingValueHandling.validateSettings(settings);
        m_invalidDistributionHandling.validateSettings(settings);
        m_removeIncludedColumns.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_columnFilterModel.loadSettingsFrom(settings);
        m_precisionModel.loadSettingsFrom(settings);
        m_columnNameModel.loadSettingsFrom(settings);
        m_allowUnpreciseProbabilities.loadSettingsFrom(settings);
        m_missingValueHandling.loadSettingsFrom(settings);
        m_invalidDistributionHandling.loadSettingsFrom(settings);
        m_removeIncludedColumns.loadSettingsFrom(settings);
    }

}
