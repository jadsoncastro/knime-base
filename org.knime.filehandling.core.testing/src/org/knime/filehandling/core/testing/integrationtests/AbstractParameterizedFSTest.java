package org.knime.filehandling.core.testing.integrationtests;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;

import org.eclipse.core.runtime.FileLocator;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.connections.knimerelativeto.LocalRelativeToFSTestInitializer;
import org.knime.filehandling.core.testing.FSTestInitializer;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;

/**
 * The parent class of the test suite in our file system testing framework. All
 * test classes which extend this class will automatically be parameterized by
 * all registered {@link FSTestInitializer} and thus be tested on all file
 * system implementations.
 *
 * @author Tobias Urhaug, KNIME GmbH, Berlin, Germany
 */
@RunWith(Parameterized.class)
public abstract class AbstractParameterizedFSTest {
    private static final String DUMMY_WORKFLOW = "resources/dummy-workflow";

    protected static final String LOCAL = "local";
    protected static final String S3 = "s3";
    protected static final String GS = "gs";

    @Rule
    public TemporaryFolder tmpDir = new TemporaryFolder();

    @Parameters(name = "File System: {0}")
    public static Collection<Object[]> allFileSystemTestInitializers() {
        return FSTestParameters.get();
    }

    @Before
    public void beforeTestCase() throws IOException {
        m_testInitializer.beforeTestCase();
        m_testInitializer.beforeTestCase(tmpDir.newFolder().toPath(), m_dummyWorkflow);
        m_connection = m_testInitializer.getFSConnection();
    }

    @After
    public void afterTestCase() throws IOException {
        m_testInitializer.afterTestCase();
        m_connection = null;
    }

    protected final FSTestInitializer m_testInitializer;
    protected final String m_fsType;
    protected final Path m_dummyWorkflow;

    /** {@link FSConnection} that gets initialized before each test */
    protected FSConnection m_connection;

    /**
     * Creates a new instance for a file system.
     * 
     * @param fsType
     *            the file system type
     * @param testInitializer
     *            the initializer for the file system
     */
    public AbstractParameterizedFSTest(final String fsType, final FSTestInitializer testInitializer) {
        m_testInitializer = testInitializer;
        m_fsType = fsType;
        m_dummyWorkflow = getDummyWorkflowPath();
    }

    /**
     * Ignores a test case for the specified file systems.
     * 
     * This is helpful in the case where a general test case is true for most file
     * system, but not all. Every file system given as a parameter will be ignored.
     * 
     * @param fileSystems
     *            the file systems to be ignored for this test case
     */
    public void ignore(final String... fileSystems) {
        final boolean shouldBeIgnored = Arrays.stream(fileSystems).anyMatch(fileSystem -> fileSystem.equals(m_fsType));
        final String errMsg = String.format("Test case has been ignored for the file system '%s'", m_fsType);
        Assume.assumeFalse(errMsg, shouldBeIgnored);
    }

    /**
     * Ignores a test case for the specified file system and provides a reason why.
     * 
     * This is helpful in the case where a general test case is true for most file
     * systems, but not all.
     * 
     * @param fileSystem
     *            the file system to be ignored for this test case
     */
    public void ignoreWithReason(final String reason, final String fileSystem) {
        final boolean shouldBeIgnored = fileSystem.equals(m_fsType);
        Assume.assumeFalse(reason, shouldBeIgnored);
    }

    /**
     * Ignores a test case for all FileSystems except the specified file systems.
     *
     * This is helpful in the case where a general test case is true for most file
     * system, but not all. Every file system that is not given as a parameter will
     * be ignored.
     *
     * @param fileSystems
     *            the file systems to be ignored for this test case
     */
    public void ignoreAllExcept(final String... fileSystems) {
        final boolean shouldBeIgnored = !Arrays.stream(fileSystems).anyMatch(fileSystem -> fileSystem.equals(m_fsType));
        final String errMsg = String.format("Test case has been ignored for the file system '%s'", m_fsType);
        Assume.assumeFalse(errMsg, shouldBeIgnored);
    }
    
    /**
     * @return the underlying file system instance. 
     */
    public FSFileSystem<?> getFileSystem() {
        return m_connection.getFileSystem();
    }

	/**
	 * @return path of a dummy workflow
	 */
	public static Path getDummyWorkflowPath() {
		return findInPlugin(DUMMY_WORKFLOW);
	}

	/**
	 * Find a given path in bundle or fail if missing.
	 */
	private static Path findInPlugin(final String pathInBundle) {
		try {
			Bundle thisBundle = FrameworkUtil.getBundle(LocalRelativeToFSTestInitializer.class);
			URL url = FileLocator.find(thisBundle, new org.eclipse.core.runtime.Path(pathInBundle), null);
			if (url == null) {
				throw new FileNotFoundException(thisBundle.getLocation() + pathInBundle);
			}
			return Paths.get(FileLocator.toFileURL(url).getPath());
		} catch (final IOException e) {
			throw new UncheckedIOException(e);
		}
	}
}
