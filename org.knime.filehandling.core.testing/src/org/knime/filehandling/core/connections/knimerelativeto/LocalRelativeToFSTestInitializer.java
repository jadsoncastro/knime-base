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
 */
package org.knime.filehandling.core.connections.knimerelativeto;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;

import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.UnsupportedWorkflowVersionException;
import org.knime.core.node.workflow.WorkflowContext;
import org.knime.core.node.workflow.WorkflowLoadHelper;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.core.node.workflow.WorkflowPersistor.WorkflowLoadResult;
import org.knime.core.util.FileUtil;
import org.knime.core.util.LockFailedException;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.testing.local.BasicLocalTestInitializer;

/**
 * Local mountpoint or workflow relative to file system initializer.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LocalRelativeToFSTestInitializer extends BasicLocalTestInitializer {
    private final URI m_fileSystemUri;

    private WorkflowManager m_workflowManager;

    private LocalRelativeToFSConnection m_fsConnection;

    /**
     * Default constructor.
     * 
     * @param fileSystemHost hostname of knime FS (knime.mountpoint or knime.workflow)
     * @throws IOException
     */
    public LocalRelativeToFSTestInitializer(final String fileSystemHost) {
        m_fileSystemUri = URI.create("knime://" + fileSystemHost);
    }

    @Override
    public FSConnection getFSConnection() {
        return m_fsConnection;
    }

    @Override
    public Path getRoot() {
        return getFileSystem().getRootDirectories().iterator().next();
    }

    protected LocalRelativeToFileSystem getFileSystem() {
        return m_fsConnection.getFileSystem();
    }

    public static WorkflowManager getWorkflowManager(final File mountpointRoot, final Path currentWorkflowDirectory,
        final boolean serverMode) throws IOException {
        try {
            final ExecutionMonitor exec = new ExecutionMonitor();
            final WorkflowContext.Factory fac = new WorkflowContext.Factory(currentWorkflowDirectory.toFile());
            fac.setMountpointRoot(mountpointRoot);
            fac.setTemporaryCopy(serverMode);
            if (serverMode) {
                fac.setRemoteAddress(URI.create("http://test-test-test:-1"), "test-test-test");
                fac.setRemoteAuthToken("test-test-test");
            }
            final WorkflowLoadHelper loadHelper = new WorkflowLoadHelper(fac.createContext());
            final WorkflowLoadResult loadResult =
                WorkflowManager.ROOT.load(currentWorkflowDirectory.toFile(), exec, loadHelper, false);
            return loadResult.getWorkflowManager();
        } catch (final InvalidSettingsException | CanceledExecutionException | UnsupportedWorkflowVersionException
                | LockFailedException e) {
            throw new IOException(e);
        }
    }

    public static Path createWorkflowDir(final Path dummyWorkflow, final Path parentDir, final String workflowName)
        throws IOException {
        final Path workflowDir = parentDir.getFileSystem().getPath(parentDir.toString(), workflowName);
        FileUtil.copyDir(dummyWorkflow.toFile(), workflowDir.toFile());
        return workflowDir;
    }

    @Override
    public void beforeTestCase(Path tmpDir, Path dummyWorkflow) throws IOException {
        super.beforeTestCase(tmpDir, dummyWorkflow);

        final Path currentWorkflow = createWorkflowDir(dummyWorkflow, tmpDir, "current-workflow");
        m_workflowManager = getWorkflowManager(tmpDir.toFile(), currentWorkflow, false);
        NodeContext.pushContext(m_workflowManager);
        m_fsConnection = new LocalRelativeToFSConnection(m_fileSystemUri);
    }

    @Override
    public void afterTestCase() throws IOException {
        try {
            WorkflowManager.ROOT.removeProject(m_workflowManager.getID());
            super.afterTestCase();
        } finally {
            NodeContext.removeLastContext();
        }
    }

    @Override
    public RelativeToPath createFile(final String... pathComponents) throws IOException {
        return createFileWithContent("", pathComponents);
    }

    @Override
    public RelativeToPath createFileWithContent(final String content, final String... pathComponents)
        throws IOException {
        super.createLocalFileWithContent(content, pathComponents);
        return getFileSystem().getPath(getFileSystem().getSeparator(), pathComponents);
    }
}
