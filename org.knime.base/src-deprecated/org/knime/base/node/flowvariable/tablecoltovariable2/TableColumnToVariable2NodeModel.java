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
 *   23.10.2013 (gabor): created
 */
package org.knime.base.node.flowvariable.tablecoltovariable2;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.stream.Collectors;

import org.knime.base.node.flowvariable.VariableAndDataCellUtil;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.MissingValue;
import org.knime.core.data.MissingValueException;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnName;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;

/**
 * This is the model implementation of TableColumnToVariable2. Converts the values from a table column to flow variables
 * with the row ids as their variable name.
 *
 * @author Gabor Bakos
 * @author Marc Bux, KNIME GmbH, Berlin, Germany
 * @deprecated replaced by {@code TableColumnToVariable3NodeModel}
 */
@Deprecated
class TableColumnToVariable2NodeModel extends NodeModel {

    private static final String CFGKEY_IGNORE_MISSING = "ignore missing";

    private static final boolean DEFAULT_IGNORE_MISSING = true;

    private static final String CFGKEY_COLUMN = "column";

    private static final String DEFAULT_COLUMN = "";

    static final SettingsModelBoolean createIgnoreMissing() {
        return new SettingsModelBoolean(CFGKEY_IGNORE_MISSING, DEFAULT_IGNORE_MISSING);
    }

    static final SettingsModelColumnName createColumnSettings() {
        return new SettingsModelColumnName(CFGKEY_COLUMN, DEFAULT_COLUMN);
    }

    private SettingsModelBoolean m_ignoreMissing = createIgnoreMissing();

    private SettingsModelColumnName m_column = createColumnSettings();

    /**
     * Constructor for the node model.
     */
    TableColumnToVariable2NodeModel() {
        super(new PortType[]{BufferedDataTable.TYPE}, new PortType[]{FlowVariablePortObject.TYPE});
    }

    @Override
    protected PortObject[] execute(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData[0] instanceof BufferedDataTable) {
            final BufferedDataTable table = (BufferedDataTable)inData[0];
            final DataTableSpec spec = table.getSpec();
            final int colIndex = spec.findColumnIndex(m_column.getStringValue());
            assert colIndex >= 0 : colIndex;
            final DataType type = spec.getColumnSpec(colIndex).getType();
            for (final DataRow row : table) {
                final DataCell cell = row.getCell(colIndex);
                final String name = row.getKey().getString();
                if (cell.isMissing()) {
                    if (m_ignoreMissing.getBooleanValue()) {
                        continue;
                    }
                    throw new MissingValueException((MissingValue)cell,
                        "Missing value in column (" + m_column.getColumnName() + ") in row: " + row.getKey());
                }

                VariableAndDataCellUtil.pushVariable(type, cell, (t, c) -> pushFlowVariable(name, t, c));
            }
        }
        return new FlowVariablePortObject[]{FlowVariablePortObject.INSTANCE};
    }

    @Override
    protected void reset() {
        // Do nothing, no internal state
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs[0] instanceof DataTableSpec) {
            final DataTableSpec spec = (DataTableSpec)inSpecs[0];
            if (m_column.getColumnName().equals(DEFAULT_COLUMN)) {
                errorOrDefault(spec, "No column selected");
            }
            final int colIndex = spec.findColumnIndex(m_column.getStringValue());
            if (colIndex < 0) {
                final String errorMessage =
                    "Wrong column name, not in input: " + m_column.getStringValue() + " in: " + spec;
                errorOrDefault(spec, errorMessage);
            }
        } else {
            throw new InvalidSettingsException("Wrong input type: " + inSpecs[0]);
        }
        return new PortObjectSpec[]{FlowVariablePortObjectSpec.INSTANCE};
    }

    private void errorOrDefault(final DataTableSpec spec, final String errorMessage) throws InvalidSettingsException {
        final Collection<DataColumnSpec> applicableColumns = applicableColumns(spec);
        if (applicableColumns.size() == 1) {
            final DataColumnSpec column = applicableColumns.iterator().next();
            m_column.setSelection(column.getName(), false);
            setWarningMessage("Selected column " + column.getName());
        } else {
            throw new InvalidSettingsException(errorMessage);
        }
    }

    private static Collection<DataColumnSpec> applicableColumns(final DataTableSpec spec) {
        return spec.stream().filter(s -> VariableAndDataCellUtil.isTypeCompatible(s.getType()))
            .collect(Collectors.toList());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_column.saveSettingsTo(settings);
        m_ignoreMissing.saveSettingsTo(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_column.loadSettingsFrom(settings);
        m_ignoreMissing.loadSettingsFrom(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_column.validateSettings(settings);
        m_ignoreMissing.validateSettings(settings);
    }

    @Override
    protected void loadInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        //No internal state
    }

    @Override
    protected void saveInternals(final File internDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        //No internal state
    }

}
