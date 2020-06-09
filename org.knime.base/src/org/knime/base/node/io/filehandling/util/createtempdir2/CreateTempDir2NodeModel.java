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
 *   June 08, 2020 (Temesgen H. Dadi, KNIME GmbH, Berlin, Germany): created
 */
package org.knime.base.node.io.filehandling.util.createtempdir2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.EnumSet;

import org.apache.commons.lang3.RandomStringUtils;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.flowvariable.FlowVariablePortObject;
import org.knime.core.node.port.flowvariable.FlowVariablePortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.connections.FSLocation;
import org.knime.filehandling.core.connections.FSPath;
import org.knime.filehandling.core.data.location.variable.FSLocationVariableType;
import org.knime.filehandling.core.defaultnodesettings.filechooser.WritePathAccessor;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;
import org.knime.filehandling.core.defaultnodesettings.status.PriorityStatusConsumer;

/**
 * {@link NodeModel} for the "Create Temp Dir" Node.
 *
 * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany
 */
final class CreateTempDir2NodeModel extends NodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(CreateTempDir2NodeModel.class);

    static final String CONNECTION_INPUT_PORT_GRP_NAME = "File System Connection";

    static final int FS_CONNECTION_PORT_INDEX = 0;

    static final String VARIABLE_OUTNPUT_PORT_GRP_NAME = "Variable Output Ports";

    private final CreateTempDir2Config m_config;

    static final String CFG_TEMP_DIR_PARENT = "temp_dir_parent";

    private final SettingsModelWriterFileChooser m_parentDirChooserModel;

    private String m_randomTempDirSuffix;

    private WritePathAccessor m_writePathAccessor;

    private FSPath m_tempDirFSPath;

    static SettingsModelWriterFileChooser createDirChooserModel(final PortsConfiguration portsConfig) {
        return new SettingsModelWriterFileChooser(CFG_TEMP_DIR_PARENT, portsConfig, CONNECTION_INPUT_PORT_GRP_NAME,
            FilterMode.FOLDER, FileOverwritePolicy.FAIL, EnumSet.of(FileOverwritePolicy.FAIL));
    }

    CreateTempDir2NodeModel(final PortsConfiguration portsConfig) {
        super(portsConfig.getInputPorts(), portsConfig.getOutputPorts());
        m_parentDirChooserModel = createDirChooserModel(portsConfig);
        m_writePathAccessor = m_parentDirChooserModel.createWritePathAccessor();
        m_tempDirFSPath = null;
        m_config = new CreateTempDir2Config();
    }

    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        m_randomTempDirSuffix = RandomStringUtils.randomAlphanumeric(12).toLowerCase();
        return new PortObjectSpec[]{FlowVariablePortObjectSpec.INSTANCE};
    }

    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
        try {
            m_writePathAccessor = m_parentDirChooserModel.createWritePathAccessor();
            final FSPath parentPath = m_writePathAccessor.getOutputPath(new PriorityStatusConsumer());
            if (parentPath != null && !Files.exists(parentPath)) {
                if (m_parentDirChooserModel.isCreateParentDirectories()) {
                    Files.createDirectories(parentPath);
                } else {
                    throw new IOException(String.format(
                        "The directory '%s' does not exist and must not be created due to user settings.", parentPath));
                }
            }
            m_tempDirFSPath =
                FSFiles.createTempDirectory(parentPath, m_config.getTempDirPrefix(), m_randomTempDirSuffix);
            createFlowVariables();
        } catch (Exception e) {
            // TODO: handle exception
        }
        return new PortObject[]{FlowVariablePortObject.INSTANCE};
    }

    private void createFlowVariables() {
        CheckUtils.checkArgument(m_config.getAdditionalVarNames().length == m_config.getAdditionalVarValues().length,
            "The number of names for addtional variables must be equal to that of values!");

        pushFSLocationVariable(m_config.getTempDirPathVariableName(), m_tempDirFSPath.toFSLocation());

        for (int i = 0; i < m_config.getAdditionalVarNames().length; i++) {
            final FSPath additionalPath = (FSPath)m_tempDirFSPath.resolve(m_config.getAdditionalVarValues()[i]);
            pushFSLocationVariable(m_config.getAdditionalVarNames()[i], additionalPath.toFSLocation());
        }
    }

    private void pushFSLocationVariable(final String name, final FSLocation var) {
        pushFlowVariable(name, FSLocationVariableType.INSTANCE, var);
    }

    private void deleteTmpDir() {
        try {
            FSFiles.deleteRecursively(m_tempDirFSPath);
            m_writePathAccessor.close();
        } catch (IOException e) {
            LOGGER.debug("Problem deleting temp directory " + e.getMessage());
        }
    }

    @Override
    protected void onDispose() {
        deleteTmpDir();
        super.onDispose();
    }

    @Override
    protected void reset() {
        if (m_config.deleteDirOnReset() && m_tempDirFSPath != null) {
            deleteTmpDir();
        }
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_parentDirChooserModel.validateSettings(settings);
        m_config.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_parentDirChooserModel.loadSettingsFrom(settings);
        m_config.loadSettingsForModel(settings);
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_parentDirChooserModel.saveSettingsTo(settings);
        m_config.saveSettingsTo(settings);
    }

    @Override
    protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        setWarningMessage("Temporary directory is porbably deleted. Consider re-executing.");
    }

    @Override
    protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec)
        throws IOException, CanceledExecutionException {
        // Nothing to do here
    }
}
