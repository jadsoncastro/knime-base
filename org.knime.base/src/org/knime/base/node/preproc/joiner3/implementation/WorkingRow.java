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
 *   01.12.2009 (Heiko Hofer): created
 */
package org.knime.base.node.preproc.joiner3.implementation;

import java.util.Iterator;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataRow;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.LongCell;

/**
 * This class is a container for a DataRow and an index. The index will be used
 * to define an order on the InputRow.
 *
 * @author Carl Witt, KNIME AG, Zurich, Switzerland
 */
class WorkingRow implements DataRow {

    final RowKey rowKey;

    // append a column that stores the offset in the original table
    final LongCell m_offset;
    final JoinTuple joinTuple;
    final DataCell[] joinCells;
    final DataCell[] retainedCells;

    /**
     * @param row A DataRow
     * @param index The index of row
     * @param port The DataPort of the row, either Left or Right
     * @param settings The settings common for all InputRow Objects.
     */
    WorkingRow(final DataRow original, final int[] joinColumns, final int[] retainColumns, final long offset) {
        rowKey = original.getKey();

        m_offset = new LongCell(offset);

        joinCells = joinColumns.length > 0 ? new DataCell[joinColumns.length] : null;
        for (int i = 0; i < joinColumns.length; i++) {
            joinCells[i] = original.getCell(joinColumns[i]);
        }
        joinTuple = new JoinTuple(joinCells);

        retainedCells = retainColumns.length > 0 ? new DataCell[retainColumns.length] : null;
        for (int i = 0; i < retainColumns.length; i++) {
            retainedCells[i] = original.getCell(retainColumns[i]);
        }
    }

    /**
     * Create the join tuples for this input row. Note, more than one
     * join tuple is returned in the match any case only. In this case a
     * pairwise comparison will always be false, so that an insertion in a
     * HashSet will always lead to as many new entries as there are columns
     * in the tuple.
     *
     * @return the JoinTuples of this row.
     */
//    JoinTuple[] getJoinTuples() {
//        List<Integer> indices = null;
//        indices = m_settings.getJoiningIndices(m_port);
//        if (!m_settings.getMatchAny()) {
//            int numJoinAttributes = indices.size();
//            DataCell[] cells = new DataCell[numJoinAttributes];
//            for (int i = 0; i < numJoinAttributes; i++) {
//                int index = indices.get(i);
//                if (index >= 0) {
//                    cells[i] = m_row.getCell(index);
//                } else {
//                    // create a StringCell since row IDs may match
//                    // StringCell's
//                    cells[i] = new StringCell(m_row.getKey().getString());
//                }
//            }
//            return new JoinTuple[] {new JoinTuple(cells)};
//        } else {
//            int numJoinAttributes = indices.size();
//            JoinTuple[] joinCells = new  JoinTuple[numJoinAttributes];
//
//            for (int i = 0; i < numJoinAttributes; i++) {
//                int index = indices.get(i);
//                DataCell[] cells = new DataCell[numJoinAttributes];
//                for (int k = 0; k < numJoinAttributes; k++) {
//                    cells[k] = WildCardCell.getDefault();
//                }
//                if (index >= 0) {
//                    cells[i] = m_row.getCell(index);
//                } else {
//                    // create a StringCell since row IDs may match
//                    // StringCell's
//                    cells[i] = new StringCell(m_row.getKey().getString());
//                }
//                joinCells[i] = new JoinTuple(cells);
//            }
//            return joinCells;
//        }
//    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<DataCell> iterator() {
        return new Iterator<DataCell>(){
            int cell = 0;
            @Override
            public boolean hasNext() {
                return cell < getNumCells();
            }

            @Override
            public DataCell next() {
                return getCell(cell++);
            }
        };
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumCells() {
        return 1 + joinCells.length + retainedCells.length;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RowKey getKey() {
        return rowKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataCell getCell(final int cell) {
        if (cell == 0) {
            return m_offset;
        }
        if (cell <= joinCells.length) {
            return joinCells[cell];
        }
        return retainedCells[cell];
    }

    public long getOuterRowIndex() {
        return m_offset.getLongValue();
    }
}
