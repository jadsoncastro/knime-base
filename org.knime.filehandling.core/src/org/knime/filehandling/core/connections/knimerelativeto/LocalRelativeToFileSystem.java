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

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import org.knime.core.node.workflow.WorkflowContext;
import org.knime.filehandling.core.connections.local.LocalFileSystemProvider;
import org.knime.filehandling.core.defaultnodesettings.FileSystemChoice.Choice;
import org.knime.filehandling.core.defaultnodesettings.KNIMEConnection.Type;

/**
 * Local KNIME relative to File System implementation.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class LocalRelativeToFileSystem extends BaseRelativeToFileSystem {

    private final FileStore m_localFileStore;

    private final List<FileStore> m_fileStores;

    /**
     * A local path (from the default FS provider) that points to the
     * folder of the current mountpoint.
     */
    private final Path m_localMountpointDirectory;

    /**
     * A local path (from the default FS provider) that points to the folder
     * of the current workflow.
     */
    private final Path m_localWorkflowDirectory;

    /**
     * A path (from the relative-to FS provider) that specifies the current workflow
     * directory.
     */
    protected final RelativeToPath m_workflowDirectory;

    /**
     * Default constructor.
     *
     * @param fileSystemProvider Creator of this FS, holding a reference.
     * @param uri URI without a path
     * @param connectionType {@link Type#MOUNTPOINT_RELATIVE} or {@link Type#WORKFLOW_RELATIVE} connection type
     * @param isConnectedFs Whether this file system is a {@link Choice#CONNECTED_FS} or a convenience file system
     *            ({@link Choice#KNIME_FS})
     * @throws IOException
     */
    protected LocalRelativeToFileSystem(final LocalRelativeToFileSystemProvider fileSystemProvider, final URI uri,
        final Type connectionType, final boolean isConnectedFs) throws IOException {

        super(fileSystemProvider, uri, connectionType, isConnectedFs);

        final WorkflowContext workflowContext = getWorkflowContext();
        if (isServerContext(workflowContext)) {
            throw new UnsupportedOperationException(
                "Unsupported temporary copy of workflow detected. Relative to does not support server execution.");
        }

        m_localMountpointDirectory = LocalFileSystemProvider.INSTANCE.getPath(
            workflowContext.getMountpointRoot().toPath().toUri()).toAbsolutePath().normalize();
        m_localWorkflowDirectory = LocalFileSystemProvider.INSTANCE.getPath(
            workflowContext.getCurrentLocation().toPath().toUri()).toAbsolutePath().normalize();
        m_workflowDirectory = localToRelativeToPath(m_localWorkflowDirectory);
        m_localFileStore = getFileStore(m_localWorkflowDirectory, getFileStoreType(), "default_file_store");
        m_fileStores = Collections.unmodifiableList(Collections.singletonList(m_localFileStore));
    }

    @Override
    protected RelativeToPath getWorkflowDirectory() {
        return m_workflowDirectory;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return m_fileStores;
    }

    @Override
    protected FileStore getFileStore(final RelativeToPath path) throws IOException {
        return m_localFileStore;
    }

    @Override
    public boolean isWorkflowDirectory(final RelativeToPath path) throws IOException {
        return isLocalWorkflowDirectory(toAbsoluteLocalPath(path));
    }

    @Override
    protected Path toRealPathWithAccessibilityCheck(final RelativeToPath path) throws IOException {
        if (!isPathAccessible(path)) {
            throw new NoSuchFileException(path.toString());
        } else {
            return toAbsoluteLocalPath(path);
        }
    }

    /**
     * Maps a path from relative-to file system to a path in the local file system.
     *
     * @param path a relative-to path inside relative-to file system
     * @return an absolute path in the local file system (default FS provider) that corresponds to this path.
     */
    private Path toAbsoluteLocalPath(final RelativeToPath path) {
        final RelativeToPath absolutePath = (RelativeToPath) path.toAbsolutePath().normalize();
        final Path realPath =  absolutePath.appendToBaseDir(m_localMountpointDirectory);
        return LocalFileSystemProvider.INSTANCE.getPath(realPath.toUri());
    }

    /**
     * Check if given path represent a regular file. Workflow directories are files, with the exception of the current
     * workflow dir and a workflow relative path.
     *
     * @param path relative-to path to check
     * @return {@code true} if path is a normal file or a workflow directory
     * @throws IOException
     */
    @Override
    protected boolean isRegularFile(final RelativeToPath path) throws IOException {
        if (!isPathAccessible(path)) {
            throw new NoSuchFileException(path.toString()); // not allowed
        } else if (isMountpointRelativeFileSystem() && isCurrentWorkflowDirectory(path)) {
            return true;
        } else if (isMountpointRelativeFileSystem() && isOrInCurrentWorkflowDirectory(path)) {
            throw new NoSuchFileException(path.toString()); // not allowed
        } else if (!isOrInCurrentWorkflowDirectory(path) && isWorkflowDirectory(path)) {
            return true;
        } else {
            return !Files.isDirectory(toAbsoluteLocalPath(path));
        }
    }

    @Override
    protected boolean existsWithAccessibilityCheck(final RelativeToPath path) throws IOException {
        final Path localPath = toAbsoluteLocalPath(path);
        return isPathAccessible(path) && Files.exists(localPath);
    }

    @Override
    protected void prepareClose() {
        // Nothing to do
    }

    /**
     * Convert a given local file system path into a virtual absolute relative to path.
     *
     * Note: The local file system might use other separators than the relative-to file system.
     *
     * @param localPath path in local file system
     * @return absolute path in virtual relative to file system
     */
    protected RelativeToPath localToRelativeToPath(final Path localPath) {
        final Path relativePath = m_localMountpointDirectory.relativize(localPath);

        final String[] parts = new String[relativePath.getNameCount()];
        for (int i = 0; i < parts.length; i++) {
            parts[i] = relativePath.getName(i).toString();
        }

        return getPath(getSeparator(), parts);
    }
}
