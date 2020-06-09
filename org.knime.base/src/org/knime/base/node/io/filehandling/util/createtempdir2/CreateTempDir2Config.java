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

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.CheckUtils;

/**
 * Settings configuration for "Create Temp Dir" node
 *
 * @author Temesgen H. Dadi, KNIME GmbH, Berlin, Germany (re-factored)
 */
final class CreateTempDir2Config extends SettingsModel {

    private static final String CFG_NAME = "create_temp_dir_config";

    private static final String DEFAULT_TEMP_DIR_VARIABLE_NAME = "temp_path";

    private static final String DEFAULT_TEMP_DIR_PREFIX = "knimetmp-";

    private static final String CFG_TEMP_DIR_PREFIX = "temp_dir_prefix";

    private static final String CFG_TEMP_DIR_PATH_VARIABLE_NAME = "temp_dir_path_variable_name";

    private static final String CFG_DELETE_ON_RESET = "delete_on_reset";

    private static final String CFG_ADDITIONAL_VARIABLE_NAMES = "additional_variable_names";

    private static final String CFG_ADDITIONAL_VARIABLE_VALUES = "additional_variable_values";

    private String m_tempDirPrefix;

    private String m_tempDirPathVariableName;

    private boolean m_deleteDirOnReset;

    private String[] m_additionalVarNames;

    private String[] m_additionalVarValues;

    CreateTempDir2Config() {
        m_tempDirPrefix = DEFAULT_TEMP_DIR_PREFIX;
        m_tempDirPathVariableName = DEFAULT_TEMP_DIR_VARIABLE_NAME;
        m_deleteDirOnReset = true;
        m_additionalVarNames = new String[0];
        m_additionalVarValues = new String[0];
    }

    private CreateTempDir2Config(final CreateTempDir2Config source) {
        m_tempDirPrefix = source.getTempDirPrefix();
        m_tempDirPathVariableName = source.getTempDirPathVariableName();
        m_deleteDirOnReset = source.deleteDirOnReset();
        m_additionalVarNames = source.getAdditionalVarNames();
        m_additionalVarValues = source.getAdditionalVarValues();
    }

    @SuppressWarnings("unchecked")
    @Override
    protected CreateTempDir2Config createClone() {
        return new CreateTempDir2Config(this);
    }

    @Override
    protected void loadSettingsForDialog(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {
        m_tempDirPrefix = settings.getString(CFG_TEMP_DIR_PREFIX, DEFAULT_TEMP_DIR_PREFIX);
        m_tempDirPathVariableName = settings.getString(CFG_TEMP_DIR_PATH_VARIABLE_NAME, DEFAULT_TEMP_DIR_VARIABLE_NAME);
        m_deleteDirOnReset = settings.getBoolean(CFG_DELETE_ON_RESET, true);
        m_additionalVarNames = settings.getStringArray(CFG_ADDITIONAL_VARIABLE_NAMES, "");
        m_additionalVarValues = settings.getStringArray(CFG_ADDITIONAL_VARIABLE_VALUES, "");
    }

    @Override
    protected void saveSettingsForDialog(final NodeSettingsWO settings) throws InvalidSettingsException {
        saveSettingsForModel(settings);
    }

    @Override
    protected void validateSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        settings.getString(CFG_TEMP_DIR_PREFIX);
        settings.getString(CFG_TEMP_DIR_PATH_VARIABLE_NAME);

        settings.getBoolean(CFG_DELETE_ON_RESET);

        settings.getStringArray(CFG_ADDITIONAL_VARIABLE_NAMES);
        settings.getStringArray(CFG_ADDITIONAL_VARIABLE_VALUES);
    }

    @Override
    protected void loadSettingsForModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_tempDirPrefix = settings.getString(CFG_TEMP_DIR_PREFIX);
        m_tempDirPathVariableName = settings.getString(CFG_TEMP_DIR_PATH_VARIABLE_NAME);

        m_deleteDirOnReset = settings.getBoolean(CFG_DELETE_ON_RESET);

        m_additionalVarNames = settings.getStringArray(CFG_ADDITIONAL_VARIABLE_NAMES);
        m_additionalVarValues = settings.getStringArray(CFG_ADDITIONAL_VARIABLE_VALUES);
        validateSettings();
    }

    @Override
    protected void saveSettingsForModel(final NodeSettingsWO settings) {
        validateSettings();
        settings.addString(CFG_TEMP_DIR_PREFIX, m_tempDirPrefix);
        settings.addString(CFG_TEMP_DIR_PATH_VARIABLE_NAME, m_tempDirPathVariableName);

        settings.addBoolean(CFG_DELETE_ON_RESET, m_deleteDirOnReset);

        settings.addStringArray(CFG_ADDITIONAL_VARIABLE_NAMES, m_additionalVarNames);
        settings.addStringArray(CFG_ADDITIONAL_VARIABLE_VALUES, m_additionalVarValues);
    }

    private void validateSettings() {
        CheckUtils.checkArgument(
            m_tempDirPathVariableName.trim() != null && m_tempDirPathVariableName.trim().length() > 0,
            "The path variable name must not be empty!");
        CheckUtils.checkArgument(m_tempDirPrefix.trim() != null && m_tempDirPrefix.trim().length() > 0,
            "The prefix for temporary directory to be created must not be empty!");
    }

    String getTempDirPrefix() {
        return m_tempDirPrefix;
    }

    void setTempDirPrefix(final String baseName) {
        m_tempDirPrefix = baseName;
    }

    /** @return the deleteOnReset */
    boolean deleteDirOnReset() {
        return m_deleteDirOnReset;
    }

    /** @param deleteOnReset the deleteOnReset to set */
    void setDeleteDirOnReset(final boolean deleteOnReset) {
        m_deleteDirOnReset = deleteOnReset;
    }

    /**
     * @return the tempDirPathVariableName
     */
    String getTempDirPathVariableName() {
        return m_tempDirPathVariableName;
    }

    /**
     * @param tempDirPathVariableName the tempDirPathVariableName to set
     */
    void setTempDirPathVariableName(final String tempDirPathVariableName) {
        m_tempDirPathVariableName = tempDirPathVariableName;
    }

    /**
     * @return the additionalVarNames
     */
    String[] getAdditionalVarNames() {
        return m_additionalVarNames;
    }

    /**
     * @param additionalVarNames the additionalVarNames to set
     */
    void setAdditionalVarNames(final String[] additionalVarNames) {
        m_additionalVarNames = additionalVarNames;
    }

    /**
     * @return the additionalVarValues
     */
    String[] getAdditionalVarValues() {
        return m_additionalVarValues;
    }

    /**
     * @param additionalVarValues the additionalVarValues to set
     */
    void setAdditionalVarValues(final String[] additionalVarValues) {
        m_additionalVarValues = additionalVarValues;
    }

    @Override
    protected String getModelTypeID() {
        return "MODEL_TYPE_ID_" + CFG_NAME;
    }

    @Override
    protected String getConfigName() {
        return CFG_NAME;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + " ('" + CFG_NAME + "')";
    }

}
