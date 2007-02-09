/*
 * ------------------------------------------------------------------
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
 * If you have any questions please contact the copyright holder:
 * website: www.knime.org
 * email: contact@knime.org
 * -------------------------------------------------------------------
 * 
 */
package org.knime.base.node.io.csvwriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.MalformedURLException;

import org.knime.core.data.DataTable;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.StringValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.StringHistory;


/**
 * NodeModel to write a DataTable to a csv (comma separated value) file.
 * 
 * @author Bernd Wiswedel, University of Konstanz
 */
public class CSVWriterNodeModel extends NodeModel {

    /** The node logger fot this class. */
    private static final NodeLogger LOGGER = NodeLogger
            .getLogger(CSVWriterNodeModel.class);

    /**
     * Identifier for StringHistory.
     * 
     * @see StringHistory
     */
    public static final String FILE_HISTORY_ID = "csvwrite";

    /** Identifier in NodeSettings object for file name. */
    static final String CFGKEY_FILE = "filename";

    /** Identifier in NodeSettings object if writing column header. */
    static final String CFGKEY_COLHEADER = "writeColHeader";

    /** Identifier in NodeSettings object if writing column header 
     * when file exists. */
    static final String CFGKEY_COLHEADER_SKIP_ON_APPEND = 
        "skipWriteColHeaderOnAppend";

    /** Identifier in NodeSettings object if writing row header. */
    static final String CFGKEY_ROWHEADER = "writeRowHeader";
    
    /** Identifier in NodeSettings object if append to output. */
    static final String CFGKEY_APPEND = "isAppendToFile";

    /** Identfier for missing pattern. */
    static final String CFGKEY_MISSING = "missing";

    /** File to write to. */
    private String m_fileName;

    /** write column header in file? */
    private boolean m_writeColHeader;

    /** write row header in file? */
    private boolean m_writeRowHeader;
    
    /** append to file, if exists. */
    private boolean m_isAppend;
    
    /** If to skip col header writing if file exists. */
    private boolean m_writeColHeaderSkipOnAppend;

    /** string to be used for missing cells. */
    private String m_missingPattern;

    /**
     * Constructor, sets port count.
     */
    public CSVWriterNodeModel() {
        super(1, 0);
        m_missingPattern = "";
    }

    /**
     * @see NodeModel#saveSettingsTo(NodeSettingsWO)
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_fileName != null) {
            settings.addString(CFGKEY_FILE, m_fileName);
        }
        settings.addBoolean(CFGKEY_COLHEADER, m_writeColHeader);
        settings.addBoolean(CFGKEY_COLHEADER_SKIP_ON_APPEND, 
                m_writeColHeaderSkipOnAppend);
        settings.addBoolean(CFGKEY_ROWHEADER, m_writeRowHeader);
        settings.addBoolean(CFGKEY_APPEND, m_isAppend);
        settings.addString(CFGKEY_MISSING, m_missingPattern);
    }

    /**
     * @see NodeModel#validateSettings(NodeSettingsRO)
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        String missing = settings.getString(CFGKEY_MISSING);
        if (missing != null && missing.indexOf(',') >= 0) {
            throw new InvalidSettingsException("Missing pattern must not "
                    + "contain comma: " + missing);
        }
        settings.getString(CFGKEY_FILE);
        settings.getBoolean(CFGKEY_COLHEADER);
        // setting was not available in KNIME 1.1.x
        settings.getBoolean(CFGKEY_COLHEADER_SKIP_ON_APPEND, false);
        settings.getBoolean(CFGKEY_ROWHEADER);
    }

    /**
     * @see NodeModel#loadValidatedSettingsFrom(NodeSettingsRO)
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        m_fileName = settings.getString(CFGKEY_FILE);
        File file = new File(m_fileName);
        String forhistory;
        try {
            forhistory = file.toURL().toString();
        } catch (MalformedURLException mue) {
            forhistory = null;
        }
        m_writeColHeader = settings.getBoolean(CFGKEY_COLHEADER);
        // setting was not available in KNIME 1.1.x
        m_writeColHeaderSkipOnAppend = 
            settings.getBoolean(CFGKEY_COLHEADER_SKIP_ON_APPEND, false);
        m_writeRowHeader = settings.getBoolean(CFGKEY_ROWHEADER);
        m_isAppend = settings.getBoolean(CFGKEY_APPEND);
        m_missingPattern = settings.getString(CFGKEY_MISSING, "");
        StringHistory history = StringHistory.getInstance(FILE_HISTORY_ID);
        if (forhistory != null) {
            history.add(forhistory);
        }
    }

    /**
     * Writes to file...
     * 
     * @see NodeModel#execute(BufferedDataTable[],ExecutionContext)
     */
    @Override
    protected BufferedDataTable[] execute(final BufferedDataTable[] data,
            final ExecutionContext exec) throws CanceledExecutionException,
            IOException {
        DataTable in = data[0];
        File file = new File(m_fileName);
        boolean writeColHeader = m_writeColHeader;
        if (writeColHeader && file.exists()) {
            writeColHeader = !m_writeColHeaderSkipOnAppend;
        }
        CSVWriter writer = new CSVWriter(new FileWriter(file, m_isAppend));
        writer.setWriteColHeader(writeColHeader);
        writer.setWriteRowHeader(m_writeRowHeader);
        writer.setMissing(m_missingPattern);
        try {
            writer.write(in, exec);
        } catch (CanceledExecutionException cee) {
            LOGGER.info("CSV Writer canceled");
            writer.close();
            if (file.delete()) {
                LOGGER.debug("File " + m_fileName + " deleted.");
            } else {
                LOGGER.warn("Unable to delete file " + m_fileName);
            }
            throw cee;
        }
        writer.close();
        // execution successful return empty array
        return new BufferedDataTable[0];
    }

    /**
     * Ignored.
     * 
     * @see org.knime.core.node.NodeModel#reset()
     */
    @Override
    protected void reset() {
    }

    /**
     * @see org.knime.core.node.NodeModel #loadInternals(java.io.File,
     *      org.knime.core.node.ExecutionMonitor)
     */
    @Override
    protected void loadInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // no internals to save
    }

    /**
     * @see org.knime.core.node.NodeModel #saveInternals(java.io.File,
     *      org.knime.core.node.ExecutionMonitor)
     */
    @Override
    protected void saveInternals(final File nodeInternDir,
            final ExecutionMonitor exec) throws IOException,
            CanceledExecutionException {
        // nothing to save.
    }

    /**
     * @see NodeModel#configure(DataTableSpec[])
     */
    @Override
    protected DataTableSpec[] configure(final DataTableSpec[] inSpecs)
            throws InvalidSettingsException {
        if (m_missingPattern != null && m_missingPattern.indexOf(',') >= 0) {
            throw new InvalidSettingsException(
                    "Missing pattern must not contain comma: "
                            + m_missingPattern);
        }
        checkFileAccess(m_fileName);
        DataTableSpec inSpec = inSpecs[0];
        for (int i = 0; i < inSpec.getNumColumns(); i++) {
            DataType c = inSpec.getColumnSpec(i).getType();
            if (!c.isCompatible(DoubleValue.class)
                    && !c.isCompatible(IntValue.class)
                    && !c.isCompatible(StringValue.class)) {
                throw new InvalidSettingsException(
                        "Input table contains not only String or Doubles");
            }
        }
        return new DataTableSpec[0];
    }

    /**
     * Helper that checks some properties for the file argument.
     * 
     * @param fileName the file to check
     * @throws InvalidSettingsException if that fails
     */
    private void checkFileAccess(final String fileName)
            throws InvalidSettingsException {
        if (fileName == null) {
            throw new InvalidSettingsException("No output file specified.");
        }
        File file = new File(fileName);

        if (file.isDirectory()) {
            throw new InvalidSettingsException("\"" + file.getAbsolutePath()
                    + "\" is a directory.");
        }
        if (!file.exists()) {
            // dunno how to check the write access to the directory. If we can't
            // create the file the execute of the node will fail. Well, too bad.
            return;
        }
        if (!file.canWrite()) {
            throw new InvalidSettingsException("Cannot write to file \""
                    + file.getAbsolutePath() + "\".");
        }
        // here it exists and we can write it: warn user if we will overwrite
        if (!m_isAppend) {
            setWarningMessage("Selected output file exists and will be "
                    + "overwritten!");
        }
    }
}
