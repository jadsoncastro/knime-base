/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright, 2003 - 2007
 * University of Konstanz, Germany
 * Chair for Bioinformatics and Information Mining (Prof. M. Berthold)
 * and KNIME GmbH, Konstanz, Germany
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any quesions please contact the copyright holder:
 * website: www.knime.org
 * email: contact@knime.org
 * ---------------------------------------------------------------------
 * 
 * History
 *   18.01.2007 (dill): created
 */
package org.knime.base.node.mine.subgroupminer;

import java.util.BitSet;

import org.knime.base.data.bitvector.BitVectorCell;
import org.knime.base.data.bitvector.BitVectorRowCellFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DoubleValue;

/**
 * 
 * @author Fabian Dill, University of Konstanz
 */
public class Numeric2BitVectorThresholdCellFactory extends BitVectorRowCellFactory {
    
    
    
    private int m_totalNrOf0s;
    private int m_totalNrOf1s;
    private double m_threshold;

    /**
     * 
     * @param bitColSpec {@link DataColumnSpec} of the column containing the 
     * bitvectors
     * @param threshold the threshold above which the bit is set
     */
    public Numeric2BitVectorThresholdCellFactory(final DataColumnSpec bitColSpec,
            final double threshold) {
        super(bitColSpec);
        m_threshold = threshold;
    }


    /**
     * 
     * @see org.knime.base.data.bitvector.BitVectorRowCellFactory
     * #getNumberOfNotSetBits()
     */
    @Override
    public int getNumberOfNotSetBits() {
        return m_totalNrOf0s;
    }

    /**
     * 
     * @see org.knime.base.data.bitvector.BitVectorRowCellFactory
     * #getNumberOfSetBits()
     */
    @Override
    public int getNumberOfSetBits() {
        return m_totalNrOf1s;
    }


    /**
     * 
     * @see org.knime.base.data.bitvector.BitVectorRowCellFactory
     * #wasSuccessful()
     */
    @Override
    public boolean wasSuccessful() {
        return true;
    }

    /**
     * @see org.knime.core.data.container.SingleCellFactory#getCell(
     * org.knime.core.data.DataRow)
     */
    @Override
    public DataCell getCell(DataRow row) {
        incrementNrOfRows();
        BitSet currBitSet = new BitSet(row.getNumCells());
        for (int i = 0; i < row.getNumCells(); i++) {
            if (!row.getCell(i).getType().isCompatible(
                    DoubleValue.class)) {
                continue;
            }
            if (row.getCell(i).isMissing()) {
                m_totalNrOf0s++;
                continue;
            }
            double currValue = ((DoubleValue)row.getCell(i))
                    .getDoubleValue();
                if (currValue >= m_threshold) {
                    currBitSet.set(i);
                    m_totalNrOf1s++;
                } else {
                    m_totalNrOf0s++;
                }
        }
        return new BitVectorCell(currBitSet, row.getNumCells());
    }

}
