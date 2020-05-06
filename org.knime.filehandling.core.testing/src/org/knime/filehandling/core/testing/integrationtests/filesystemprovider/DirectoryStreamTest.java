package org.knime.filehandling.core.testing.integrationtests.filesystemprovider;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.knime.filehandling.core.testing.FSTestInitializer;
import org.knime.filehandling.core.testing.integrationtests.AbstractParameterizedFSTest;

/**
 * Test class for file system directory streams.
 *
 * @author Tobias Urhaug, KNIME GmbH, Berlin, Germany
 *
 */
public class DirectoryStreamTest extends AbstractParameterizedFSTest {

	public DirectoryStreamTest(final String fsType, final FSTestInitializer testInitializer) {
		super(fsType, testInitializer);
	}

	@Test
	public void test_list_files_in_directory() throws Exception {
		final Path fileA = m_testInitializer.createFileWithContent("contentA", "dir", "fileA");
		final Path fileB = m_testInitializer.createFileWithContent("contentB", "dir", "fileB");
		final Path fileC = m_testInitializer.createFileWithContent("contentC", "dir", "fileC");
		final Path directory = fileA.getParent();

		final List<Path> paths = new ArrayList<>();
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, (path) -> true)) {
			directoryStream.forEach(paths::add);
		}

		assertTrue(paths.contains(fileA));
		assertTrue(paths.contains(fileB));
		assertTrue(paths.contains(fileC));
	}

	@Test
	public void test_list_emtpy_directory() throws Exception {
		final Path directory = m_testInitializer.getRoot();
		// root directory might contains files (e.g. workflows), use a fresh empty
		// directory
		final Path emptyDirectory = Files.createDirectories(directory.resolve("empty-directory"));

		final List<Path> paths = new ArrayList<>();
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(emptyDirectory, (path) -> true)) {
			directoryStream.forEach(paths::add);
		}

		assertTrue(paths.isEmpty());
	}

	@Test
	public void test_list_files_with_filter() throws IOException {
		final Path fileA = m_testInitializer.createFileWithContent("contentA", "dir", "fileA");
		final Path fileB = m_testInitializer.createFileWithContent("contentB", "dir", "fileB");
		final Path fileC = m_testInitializer.createFileWithContent("contentC", "dir", "fileC");
		final Path directory = fileA.getParent();

		final Filter<Path> filter = path -> path.getFileName().toString().equals("fileB");
		final List<Path> paths = new ArrayList<>();
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, filter)) {
			directoryStream.forEach(paths::add);
		}

		assertFalse(paths.contains(fileA));
		assertTrue(paths.contains(fileB));
		assertFalse(paths.contains(fileC));
	}

	@Test(expected = NoSuchFileException.class)
	public void test_non_existent_directory() throws IOException {
		final Path directory = m_testInitializer.getRoot().resolve("doesnotexist");

		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory, (path) -> true)) {
			fail("should fail with NoSuchFileException and never reach this code");
		}
	}

	@Test(expected = NotDirectoryException.class)
	public void test_list_file_instead_of_directory() throws IOException {
		final Path file = m_testInitializer.createFileWithContent("test", "some-dir", "some-file");

		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(file, path -> true)) {
			fail("should fail with before");
		}
	}
}
