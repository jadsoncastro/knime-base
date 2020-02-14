package org.knime.filehandling.core.testing.integrationtests.filesystemprovider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.Test;
import org.knime.filehandling.core.testing.FSTestInitializer;
import org.knime.filehandling.core.testing.integrationtests.AbstractParameterizedFSTest;

/**
 * 
 * @author Tobias Urhaug, KNIME GmbH, Berlin, Germany
 */
public class CheckAccessTest extends AbstractParameterizedFSTest {
	
	public CheckAccessTest(String fsType, FSTestInitializer testInitializer) {
		super(fsType, testInitializer);
	}

	@Test
	public void test_file_exists() throws IOException {
		Path pathToFile = m_testInitializer.createFile("dir", "file.txt");
		assertTrue(Files.exists(pathToFile));
	}
	
	@Test
	public void test_file_does_not_exist() throws IOException {
		FileSystem fileSystem = m_connection.getFileSystem();
		String rootFolder = m_testInitializer.getRoot().toString();
		Path pathToNonExistingFile = fileSystem.getPath(rootFolder, "non-existing-file");
		
		assertFalse(Files.exists(pathToNonExistingFile));
	}
	
}
