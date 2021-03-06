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
package org.knime.filehandling.core.testing.integrationtests;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.knime.filehandling.core.connections.FSConnection;
import org.knime.filehandling.core.connections.FSFileSystem;
import org.knime.filehandling.core.testing.FSTestInitializer;
import org.knime.filehandling.core.util.IOESupplier;

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

    protected static final String LOCAL = "local";
    protected static final String S3 = "s3";
    protected static final String GS = "gs";

    @Parameters(name = "File System: {0}")
    public static Collection<Object[]> allFileSystemTestInitializerProviders() {
        return FSTestParameters.get();
    }

    @Before
    public void beforeTestCase() throws IOException {
        m_testInitializer.beforeTestCase();
    }

    @After
    public void afterTestCase() throws IOException {
        m_testInitializer.afterTestCase();
    }

    protected final FSTestInitializer m_testInitializer;
    protected final FSConnection m_connection;
    protected final String m_fsType;

    /**
     * Creates a new instance for a file system.
     *
     * @param fsType
     *            the file system type
     * @param testInitializer
     *            the initializer for the file system
     * @throws IOException
     */
    public AbstractParameterizedFSTest(final String fsType, final IOESupplier<FSTestInitializer> testInitializer) throws IOException {
        m_testInitializer = testInitializer.get();
        m_connection = m_testInitializer.getFSConnection();
        m_fsType = fsType;
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

}
