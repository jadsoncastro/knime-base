package org.knime.filehandling.core.connections.knimerelativeto;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.knime.filehandling.core.connections.knimerelativeto.LocalRelativeToFSTestInitializer.createWorkflowDir;
import static org.knime.filehandling.core.connections.knimerelativeto.LocalRelativeToFSTestInitializer.getWorkflowManager;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.AccessDeniedException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.workflow.NodeContext;
import org.knime.core.node.workflow.WorkflowManager;

/**
 * Test local relative to file system specific things.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class KNIMELocalRelativeToFileSystemTest {

	@Rule
	public TemporaryFolder m_tempFolder = new TemporaryFolder();

	private File m_mountpointRoot;
	private Path m_currentWorkflow;
	private WorkflowManager m_workflowManager;

	@Before
	public void beforeTestCase() throws IOException {
		m_mountpointRoot = m_tempFolder.newFolder("mountpoint-root");
		m_currentWorkflow = createWorkflowDir(m_mountpointRoot.toPath(), "current-workflow");
		createWorkflowDir(m_mountpointRoot.toPath(), "other-workflow");
		m_workflowManager = getWorkflowManager(m_mountpointRoot, m_currentWorkflow, false);
		NodeContext.pushContext(m_workflowManager);
	}

	@After
	public void afterTestCase() {
		try {
			WorkflowManager.ROOT.removeProject(m_workflowManager.getID());
		} finally {
			NodeContext.removeLastContext();
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void unsupportedServerSideExecution() throws IOException {
		// replace the current workflow manager with a server side workflow manager
		NodeContext.removeLastContext();
		final File mountpointRoot = m_tempFolder.newFolder("other-mountpoint-root");
		final Path currentWorkflow = createWorkflowDir(mountpointRoot.toPath(), "current-workflow");
		m_workflowManager = getWorkflowManager(mountpointRoot, currentWorkflow, true);
		NodeContext.pushContext(m_workflowManager);

		// initialization should fail
		getMountpointRelativeFS();
	}

	@Test
	public void getRootDirectories() throws IOException {
		final LocalRelativeToFileSystem fs = getWorkflowRelativeFS();
		assertEquals(Collections.singletonList(fs.getPath("/")), fs.getRootDirectories());
	}

	@Test
	public void workingDirectoryWorkflowRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getWorkflowRelativeFS();
		assertEquals(fs.getPath("/current-workflow"), fs.getWorkingDirectory());
	}

	@Test
	public void workingDirectoryMountpointRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getMountpointRelativeFS();
		assertEquals(fs.getPath("/"), fs.getWorkingDirectory());
	}

	@Test(expected = NoSuchFileException.class)
	public void outsideMountpointWorkflowRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getWorkflowRelativeFS();
		final RelativeToPath path = fs.getPath("../../../somewhere-outside");
		assertFalse(fs.isPathAccessible(path));
		fs.toRealPathWithAccessibilityCheck(path); // throws exception
	}

	@Test(expected = NoSuchFileException.class)
	public void outsideMountpointMountpointRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getMountpointRelativeFS();
		final RelativeToPath path = fs.getPath("/../../../somewhere-outside");
		assertFalse(fs.isPathAccessible(path));
		fs.toRealPathWithAccessibilityCheck(path); // throws exception
	}

	@Test
	public void insideCurrentWorkflowWithWorkflowRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getWorkflowRelativeFS();
		final RelativeToPath path = fs.getPath("../current-workflow/some-file.txt");
		assertTrue(fs.isPathAccessible(path));
		fs.toRealPathWithAccessibilityCheck(path); // does not throw an exception
	}

	@Test(expected = NoSuchFileException.class)
	public void insideCurrentWorkflowWithMountpointRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getMountpointRelativeFS();
		final RelativeToPath path = fs.getPath("/current-workflow/some-file.txt");
		assertFalse(fs.isPathAccessible(path));
		fs.toRealPathWithAccessibilityCheck(path); // throws exception
	}

	@Test(expected = NoSuchFileException.class)
	public void insideOtherWorkflowWithWorkflowRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getWorkflowRelativeFS();
		final RelativeToPath path = fs.getPath("../other-workflow/some-file.txt");
		assertFalse(fs.isPathAccessible(path));
		fs.toRealPathWithAccessibilityCheck(path); // throws exception
	}

	@Test(expected = NoSuchFileException.class)
	public void insideOtherWorkflowWithMountpointRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getMountpointRelativeFS();
		final RelativeToPath path = fs.getPath("/other-workflow/some-file.txt");
		assertFalse(fs.isPathAccessible(path));
		fs.toRealPathWithAccessibilityCheck(path); // throws exception
	}

	@Test
	public void isWorkflow() throws IOException {
		final LocalRelativeToFileSystem fs = getMountpointRelativeFS();
		assertFalse(fs.isWorkflowDirectory((RelativeToPath) fs.getPath("/")));
		assertTrue(fs.isWorkflowDirectory((RelativeToPath) fs.getPath("/current-workflow")));
		assertTrue(fs.isWorkflowDirectory((RelativeToPath) fs.getPath("/other-workflow")));
	}

	@Test
	public void isWorkflowRelative() throws IOException {
		assertTrue(getWorkflowRelativeFS().isWorkflowRelativeFileSystem());
		assertFalse(getMountpointRelativeFS().isWorkflowRelativeFileSystem());
	}

	@Test
	public void isRegularFileWorkflowRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getWorkflowRelativeFS();
		assertFalse(fs.isRegularFile((RelativeToPath) fs.getPath("/")));
		assertFalse(fs.isRegularFile((RelativeToPath) fs.getPath("/current-workflow")));
		assertTrue(fs.isRegularFile((RelativeToPath) fs.getPath("/other-workflow")));

		final RelativeToPath filePath = (RelativeToPath) fs.getPath("/some-file.txt");
		Files.createFile(filePath);
		assertTrue(fs.isRegularFile(filePath));

		final RelativeToPath directoryPath = (RelativeToPath) fs.getPath("/some-directory");
		Files.createDirectory(directoryPath);
		assertFalse(fs.isRegularFile(directoryPath));
	}

	@Test
	public void isRegularFileMountpointRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getMountpointRelativeFS();
		assertFalse(fs.isRegularFile((RelativeToPath) fs.getPath("/")));
		assertTrue(fs.isRegularFile((RelativeToPath) fs.getPath("/current-workflow")));
		assertTrue(fs.isRegularFile((RelativeToPath) fs.getPath("/other-workflow")));

		final RelativeToPath filePath = (RelativeToPath) fs.getPath("/some-file.txt");
		Files.createFile(filePath);
		assertTrue(fs.isRegularFile(filePath));

		final RelativeToPath directoryPath = (RelativeToPath) fs.getPath("/some-directory");
		Files.createDirectory(directoryPath);
		assertFalse(fs.isRegularFile(directoryPath));
	}

	@Test
	public void equalsOnDifferentFS() throws IOException {
		final String filename = "/some-dir/some-file.txt";
		final LocalRelativeToFileSystem mountpointFS = getMountpointRelativeFS();
		final RelativeToPath mountpointPath = mountpointFS.getPath(filename);
		final LocalRelativeToFileSystem workflowFS = getWorkflowRelativeFS();
		final RelativeToPath workflowPath = workflowFS.getPath(filename);
		assertFalse(mountpointPath.equals(workflowPath));
		assertTrue(mountpointFS.getPath(filename).equals(mountpointPath));
		assertTrue(workflowFS.getPath(filename).equals(workflowPath));

		final Path localPath = Paths.get(filename);
		assertFalse(localPath.equals(mountpointPath));
		assertFalse(localPath.equals(workflowPath));
		assertFalse(mountpointPath.equals(localPath));
		assertFalse(workflowPath.equals(localPath));
	}

	@Test
	public void testIsSame() throws IOException {
		final String filename = "/some-dir/some-file.txt";
		final LocalRelativeToFileSystem mountpointFS = getMountpointRelativeFS();
		final RelativeToPath mountpointPath = mountpointFS.getPath(filename);
		final LocalRelativeToFileSystem workflowFS = getWorkflowRelativeFS();
		final RelativeToPath workflowPath = workflowFS.getPath(filename);

		final Path localPath = Paths.get(filename);
		assertFalse(Files.isSameFile(localPath, mountpointPath));
		assertFalse(Files.isSameFile(localPath, workflowPath));
		assertFalse(Files.isSameFile(mountpointPath, localPath));
		assertFalse(Files.isSameFile(workflowPath, localPath));
	}

	/**
	 * Ensure that {@link RelativeToPath#toAbsoluteLocalPath()} uses separator
	 * from local filesystem.
	 */
	@Test
	public void testToAbsoluteLocalPath() throws IOException {
		final String filename = "/some-dir/some-file.txt";
		final LocalRelativeToFileSystem mountpointFS = getMountpointRelativeFS();
		final RelativeToPath mountpointPath = mountpointFS.getPath(filename);
		final Path convertedLocalPath = mountpointFS.toRealPathWithAccessibilityCheck(mountpointPath);
		final Path realLocalPath = m_mountpointRoot.toPath().resolve("some-dir").resolve("some-file.txt");
		assertEquals(realLocalPath, convertedLocalPath);
	}

	/**
	 * Test conversion from virtual to local and back to virtual file system.
	 */
	@Test
	public void testLocalFSConversion() throws IOException {
		final String filename = "/some-dir/some-file.txt";
		final LocalRelativeToFileSystem mountpointFS = getMountpointRelativeFS();
		final RelativeToPath mountpointPath = mountpointFS.getPath(filename);
		final LocalRelativeToFileSystem workflowFS = getWorkflowRelativeFS();
		final RelativeToPath workflowPath = workflowFS.getPath(filename);

		final Path localMountpointPath = mountpointFS.toRealPathWithAccessibilityCheck(mountpointPath);
		final Path localWorkflowPath = workflowFS.toRealPathWithAccessibilityCheck(workflowPath);
		assertEquals(localMountpointPath, localWorkflowPath);

		final RelativeToPath convertedMountpointPath = //
		        mountpointFS.localToRelativeToPath(localMountpointPath);
		assertTrue(convertedMountpointPath.isAbsolute());
		assertEquals(mountpointPath, convertedMountpointPath);

		final RelativeToPath convertedWorkflowPath = 
				workflowFS.localToRelativeToPath(localWorkflowPath);
		assertTrue(convertedWorkflowPath.isAbsolute());
		assertEquals(workflowPath, convertedWorkflowPath);
	}

	@Test
	public void testExistsMountpointRelative() throws IOException {
		final String filename = "some-file.txt";
		final LocalRelativeToFileSystem mountpointFS = getMountpointRelativeFS();
		final RelativeToPath mountpointPath = mountpointFS.getPath(filename);
		assertFalse(Files.exists(mountpointPath));
		Files.createFile(mountpointPath);
		assertTrue(Files.exists(mountpointPath));
	}

	@Test
	public void testExistsWorkflowRelative() throws IOException {
		final String filename = "some-file.txt";
		final LocalRelativeToFileSystem workflowFS = getWorkflowRelativeFS();
		final RelativeToPath workflowPath = workflowFS.getPath(filename);
		assertFalse(Files.exists(workflowPath));
		Files.createFile(workflowPath);
		assertTrue(Files.exists(workflowPath));
	}

	@Test
	public void testCreateFileOnRelativePath() throws IOException {
		final String filename = "some-file.txt";
		final LocalRelativeToFileSystem workflowFS = getWorkflowRelativeFS();
		final RelativeToPath relativePath = workflowFS.getPath(filename);

		assertFalse(Files.exists(relativePath));
		Files.createFile(relativePath);
		assertTrue(Files.exists(relativePath));
	}

	@Test
	public void relativizeWorkflowRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getWorkflowRelativeFS();
		assertEquals(fs.getPath("../some-directory/some-workflow"),
				fs.getWorkingDirectory().relativize(fs.getPath("/some-directory/some-workflow")));
	}

	@Test
	public void absolutWorkflowRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getWorkflowRelativeFS();
		assertEquals(fs.getPath("/some-directory/some-workflow"),
				fs.getPath("../some-directory/some-workflow").toAbsolutePath().normalize());
	}

	@Test
	public void absolutMountpointRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getMountpointRelativeFS();
		assertEquals(fs.getPath("/some-directory/some-workflow"),
				fs.getPath("/some-directory/some-workflow").toAbsolutePath().normalize());
	}

	@Test
	public void toUriWorkflowRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getWorkflowRelativeFS();

		assertEquals("knime://knime.workflow/some-file", fs.getPath("some-file").toUri().toString());
		assertEquals("knime://knime.workflow/some-file", fs.getPath("/current-workflow/some-file").toUri().toString());

		assertEquals("knime://knime.workflow/some-path/some-file",
				fs.getPath("some-path/some-file").toUri().toString());
		assertEquals("knime://knime.workflow/some-path/some-file",
				fs.getPath("/current-workflow/some-path/some-file").toUri().toString());

		assertEquals("knime://knime.workflow/../some-file", fs.getPath("../some-file").toUri().toString());
		assertEquals("knime://knime.workflow/../some-path/some-file",
				fs.getPath("/some-path/some-file").toUri().toString());

		assertEquals("knime://knime.workflow/../current-path/../some-file",
				fs.getPath("/current-path/../some-file").toUri().toString());
		assertEquals("knime://knime.workflow/../current-path/../some-file",
				fs.getPath("../current-path/../some-file").toUri().toString());

		assertEquals("knime://knime.workflow/../some-path/../some-file",
				fs.getPath("/some-path/../some-file").toUri().toString());
		assertEquals("knime://knime.workflow/../some-path/../some-file",
				fs.getPath("../some-path/../some-file").toUri().toString());
	}

	@Test
	public void toUriMountpointRelative() throws IOException {
		final LocalRelativeToFileSystem fs = getMountpointRelativeFS();
		assertEquals("knime://knime.mountpoint/some-file", fs.getPath("some-file").toUri().toString());
		assertEquals("knime://knime.mountpoint/some-file", fs.getPath("/some-file").toUri().toString());

		assertEquals("knime://knime.mountpoint/some-path/some-file",
				fs.getPath("some-path/some-file").toUri().toString());
		assertEquals("knime://knime.mountpoint/some-path/some-file",
				fs.getPath("/some-path/some-file").toUri().toString());

		assertEquals("knime://knime.mountpoint/some-path/../some-file",
				fs.getPath("some-path/../some-file").toUri().toString());
		assertEquals("knime://knime.mountpoint/some-path/../some-file",
				fs.getPath("/some-path/../some-file").toUri().toString());
	}

	private static LocalRelativeToFileSystem getMountpointRelativeFS() throws IOException {
		return LocalRelativeToFileSystemProvider.getOrCreateFileSystem(URI.create("knime://knime.mountpoint"));
	}

	private static LocalRelativeToFileSystem getWorkflowRelativeFS() throws IOException {
		return LocalRelativeToFileSystemProvider.getOrCreateFileSystem(URI.create("knime://knime.workflow"));
	}
}
