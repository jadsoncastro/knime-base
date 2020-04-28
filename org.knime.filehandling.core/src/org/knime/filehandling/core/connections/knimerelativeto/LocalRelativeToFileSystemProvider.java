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
 *   Feb 11, 2020 (Sascha Wolke, KNIME GmbH): created
 */
package org.knime.filehandling.core.connections.knimerelativeto;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.util.Map;

import org.knime.core.node.workflow.WorkflowContext;
import org.knime.filehandling.core.connections.WorkflowAware;
import org.knime.filehandling.core.connections.local.LocalFileSystemProvider;
import org.knime.filehandling.core.defaultnodesettings.KNIMEConnection;
import org.knime.filehandling.core.defaultnodesettings.KNIMEConnection.Type;
import org.knime.filehandling.core.util.MountPointFileSystemAccessService;

/**
 * Local KNIME relative to File System provider.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LocalRelativeToFileSystemProvider extends BaseRelativeToFileSystemProvider<LocalRelativeToFileSystem>
    implements WorkflowAware {

    @Override
    protected LocalRelativeToFileSystem createFileSystem(final URI uri, final Map<String, ?> env)
        throws IOException {

        final Type connectionType = KNIMEConnection.connectionTypeForHost(uri.getHost());
        if (connectionType != Type.MOUNTPOINT_RELATIVE && connectionType != Type.WORKFLOW_RELATIVE) {
            throw new IllegalArgumentException("Unsupported file system type: '" + uri.getHost() + "'.");
        }

        final WorkflowContext workflowContext = getWorkflowContext();
        if (isServerContext(workflowContext)) {
            throw new UnsupportedOperationException(
                "Unsupported temporary copy of workflow detected. Relative to does not support server execution.");
        }

        final Path mountpointRoot = workflowContext.getMountpointRoot().toPath().toAbsolutePath().normalize();
        final Path workflowLocation = workflowContext.getCurrentLocation().toPath().toAbsolutePath().normalize();
        final Path relativePath = mountpointRoot.relativize(workflowLocation);

        return new LocalRelativeToFileSystem(this, uri, //
            LocalFileSystemProvider.INSTANCE.getPath(mountpointRoot.toUri()), //
            localToRelativeToPathSeperator(relativePath), //
            connectionType, false);
    }

    /**
     * Gets or creates a new {@link FileSystem} based on the input URI.
     *
     * @param uri the URI that either retrieves or creates a new file system.
     * @return a file system for the URI
     * @throws IOException if I/O error occurs
     */
    public static LocalRelativeToFileSystem getOrCreateFileSystem(final URI uri) throws IOException {
        final LocalRelativeToFileSystemProvider provider = new LocalRelativeToFileSystemProvider();
        return provider.getOrCreateFileSystem(uri, null);
    }

    @Override
    public void deployWorkflow(final File source, final Path dest, final boolean overwrite, final boolean attemptOpen)
        throws IOException {
        MountPointFileSystemAccessService.instance().deployWorkflow(source, dest.toUri(), overwrite, attemptOpen);
    }

    /**
     * Converts a given local file system path into a path string using virtual relative-to path separators.
     *
     * Note: The local (windows) file system might use other separators than the relative-to file system.
     *
     * @param localPath path in local file system
     * @return absolute path in virtual relative to file system
     */
    protected static String localToRelativeToPathSeperator(final Path localPath) {
        final StringBuilder sb = new StringBuilder();
        final String[] parts = new String[localPath.getNameCount()];
        for (int i = 0; i < parts.length; i++) {
            sb.append(BaseRelativeToFileSystem.PATH_SEPARATOR).append(localPath.getName(i).toString());
        }

        return sb.toString();
    }
}
