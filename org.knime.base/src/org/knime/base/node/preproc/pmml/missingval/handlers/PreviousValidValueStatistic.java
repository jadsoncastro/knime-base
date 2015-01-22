/*
 * ------------------------------------------------------------------------
 *
 *  Copyright by
 *  University of Konstanz, Germany and
 *  KNIME GmbH, Konstanz, Germany
 *  Website: http://www.knime.org; Email: contact@knime.org
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
 *  KNIME and ECLIPSE being a combined program, KNIME GMBH herewith grants
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
 *   18.12.2014 (Alexander): created
 */
package org.knime.base.node.preproc.pmml.missingval.handlers;

import org.knime.base.data.statistics.Statistic;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataValue;
import org.knime.core.data.MissingCell;
import org.knime.core.data.container.DataContainer;
import org.knime.core.data.def.DefaultRow;

/**
 * Statistic that finds for each missing value the previous valid one.
 * @author Alexander Fillbrunn
 */
public class PreviousValidValueStatistic extends Statistic {

    private DataCell m_previous;
    private DataContainer m_queued;
    private DataTable m_result;

    private String m_columnName;
    private int m_index = -1;

    /**
     * Constructor for NextValidValueStatistic.
     * @param clazz the class of the data value this statistic can be used for
     * @param column the column for which this statistic is calculated
     */
    public PreviousValidValueStatistic(final Class<? extends DataValue> clazz, final String column) {
        super(clazz, new String[] {column});
        m_columnName = column;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void init(final DataTableSpec spec, final int amountOfColumns) {
        m_index = spec.findColumnIndex(m_columnName);
        m_queued = new DataContainer(new DataTableSpec(spec.getColumnSpec(m_index)));
        m_previous = new MissingCell("No previous valid value");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void consumeRow(final DataRow dataRow) {
        DataCell cell = dataRow.getCell(m_index);
        if (cell.isMissing()) {
            m_queued.addRowToTable(new DefaultRow(dataRow.getKey(), m_previous));
        } else {
            m_previous = dataRow.getCell(m_index);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String afterEvaluation() {
        m_queued.close();
        m_result = m_queued.getTable();
        return super.afterEvaluation();
    }

    /**
     * @return the table where the next valid value for each row key is given.
     */
    public DataTable getMappingTable() {
        return m_result;
    }
}
