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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.knime.base.node.preproc.joiner3.implementation.JoinImplementation;
import org.knime.base.node.preproc.joiner3.implementation.Joiner;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettings;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.property.hilite.DefaultHiLiteMapper;
import org.knime.core.node.property.hilite.HiLiteHandler;
import org.knime.core.node.property.hilite.HiLiteMapper;
import org.knime.core.node.property.hilite.HiLiteTranslator;

/**
 * This is the model of the joiner node. It delegates the dirty work to the
 * Joiner class.
 *
 * @author Heiko Hofer
 */
public class Joiner3NodeModel extends NodeModel {

    private final Joiner3Settings m_settings = new Joiner3Settings();

    private HiLiteHandler m_outHandler;
    private HiLiteTranslator m_rightTranslator;
    private HiLiteTranslator m_leftTranslator;
    private HiLiteMapper m_leftMapper;
    private HiLiteMapper m_rightMapper;

    /**
     * Holds the
     */
    private final Joiner m_joiner;

    Joiner3NodeModel(final PortsConfiguration portsConfiguration) {
        super(portsConfiguration.getInputPorts(), portsConfiguration.getOutputPorts());
        m_joiner = new Joiner();

        m_outHandler = new HiLiteHandler();
        m_leftTranslator = new HiLiteTranslator();
        m_rightTranslator = new HiLiteTranslator();

    }


    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
            throws InvalidSettingsException {

        return new DataTableSpec[]{Joiner.createOutputSpec(m_settings, this::setWarningMessage, inSpecs)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] inData, final ExecutionContext exec)
        throws Exception {

        // warning and progress messages are directly fed to the execution context
        BufferedDataTable[] joinedTable = new BufferedDataTable[]{
            m_joiner.computeJoinTable(m_settings, exec, inData)};

        // TODO hiliting
        //        m_leftMapper = new DefaultHiLiteMapper(m_leftRowKeyMap);
        //        m_rightMapper = new DefaultHiLiteMapper(m_rightRowKeyMap);
        m_leftTranslator.setMapper(m_leftMapper);
        m_rightTranslator.setMapper(m_rightMapper);

        return joinedTable;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected HiLiteHandler getOutHiLiteHandler(final int outIndex) {
        return m_outHandler;
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void setInHiLiteHandler(final int inIndex,
            final HiLiteHandler hiLiteHdl) {
        if (0 == inIndex) {
            m_leftTranslator.removeAllToHiliteHandlers();
            m_leftTranslator = new HiLiteTranslator(hiLiteHdl, m_leftMapper);
            m_leftTranslator.addToHiLiteHandler(m_outHandler);
        } else {
            m_rightTranslator.removeAllToHiliteHandlers();
            m_rightTranslator = new HiLiteTranslator(hiLiteHdl, m_rightMapper);
            m_rightTranslator.addToHiLiteHandler(m_outHandler);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        m_settings.loadSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void reset() {
        m_leftTranslator.setMapper(null);
        m_rightTranslator.setMapper(null);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        File settingsFile = new File(nodeInternDir, "joinerInternalSettings");

        try(FileInputStream in = new FileInputStream(settingsFile)) {
            NodeSettingsRO settings = NodeSettings.loadFromXML(in);
            NodeSettingsRO leftMapSet = settings.getNodeSettings(
                "leftHiliteMapping");
            m_leftTranslator.setMapper(DefaultHiLiteMapper.load(leftMapSet));
            m_leftMapper = m_leftTranslator.getMapper();

            NodeSettingsRO rightMapSet = settings.getNodeSettings(
                "rightHiliteMapping");
            m_rightTranslator.setMapper(DefaultHiLiteMapper.load(rightMapSet));
            m_rightMapper = m_rightTranslator.getMapper();
        } catch (InvalidSettingsException e) {
            throw new IOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        NodeSettings internalSettings = new NodeSettings("joiner");
        NodeSettingsWO leftMapSet =
            internalSettings.addNodeSettings("leftHiliteMapping");
        ((DefaultHiLiteMapper) m_leftTranslator.getMapper()).save(leftMapSet);

        NodeSettingsWO rightMapSet =
            internalSettings.addNodeSettings("rightHiliteMapping");
        ((DefaultHiLiteMapper) m_rightTranslator.getMapper()).save(rightMapSet);

        File f = new File(nodeInternDir, "joinerInternalSettings");
        try(FileOutputStream out = new FileOutputStream(f)){
            internalSettings.saveToXML(out);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
    	Joiner3Settings s = new Joiner3Settings();
        s.loadSettings(settings);
        JoinImplementation.validateSettings(s);
    }
}
