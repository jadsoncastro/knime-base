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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.WorkflowManager;
import org.knime.filehandling.core.connections.FSFiles;
import org.knime.filehandling.core.defaultnodesettings.KNIMEConnection.Type;
import org.knime.filehandling.core.testing.local.BasicLocalTestInitializer;

/**
 * Local mountpoint or workflow relative to file system initializer.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LocalRelativeToFSTestInitializer extends BasicLocalTestInitializer<LocalRelativeToPath, LocalRelativeToFileSystem> {

    private final Path m_localMountpointRoot;

    private WorkflowManager m_workflowManager;

    /**
     * Default constructor.
     *
     * @throws IOException
     */
    public LocalRelativeToFSTestInitializer(final LocalRelativeToFSConnection fsConnection,
        final Path localMountpointRoot) throws IOException {
        super(fsConnection, getLocalWorkingDirectory(fsConnection));
        m_localMountpointRoot = localMountpointRoot;
    }

    private static Path getLocalWorkingDirectory(final LocalRelativeToFSConnection fsConn) {
        if (fsConn.getFileSystem().getPathConfig().getType() == Type.MOUNTPOINT_RELATIVE) {
            return fsConn.getFileSystem().getPathConfig().getLocalMountpointFolder();
        } else {
            return fsConn.getFileSystem().getPathConfig().getLocalWorkflowFolder();
        }
    }

    @Override
    protected void beforeTestCaseInternal() throws IOException {
        // clean out local mountpoint
        Files.list(m_localMountpointRoot).forEach((p) -> {
            try {
                FSFiles.deleteRecursively(p);
            } catch (IOException e) {
                // ignore
            }
        });

        // repopulate mountpoint with test fixture again and load workflow
        m_workflowManager = LocalRelativeToTestUtil.createAndLoadDummyWorkflow(m_localMountpointRoot);

        Files.createDirectories(getLocalTestCaseScratchDir());
    }

    @Override
    public void afterTestCase() throws IOException {
        try {
            WorkflowManager.ROOT.removeProject(m_workflowManager.getID());
        } finally {
            NodeContext.removeLastContext();
        }
    }

    @Override
    public LocalRelativeToPath createFileWithContent(final String content, final String... pathComponents)
        throws IOException {
        final Path localFile = createLocalFileWithContent(content, pathComponents);
        return getFileSystem().toAbsoluteLocalRelativeToPath(localFile);
    }
}
