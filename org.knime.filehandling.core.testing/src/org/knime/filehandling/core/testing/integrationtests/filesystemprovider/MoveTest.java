package org.knime.filehandling.core.testing.integrationtests.filesystemprovider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;
import org.knime.filehandling.core.testing.FSTestInitializer;
import org.knime.filehandling.core.testing.integrationtests.AbstractParameterizedFSTest;

/**
 * Test class for move operations on file systems.
 * 
 * @author Tobias Urhaug, KNIME GmbH, Berlin, Germany
 *
 */
public class MoveTest extends AbstractParameterizedFSTest {

	public MoveTest(String fsType, FSTestInitializer testInitializer) {
		super(fsType, testInitializer);
	}

	@Test
	public void test_move_file() throws Exception {
		String sourceContent = "Some simple test content";
		Path source = m_testInitializer.createFileWithContent(sourceContent, "dir", "file");
		Path target = source.getParent().resolve("movedFile");
		
		Files.move(source, target);
		
		assertFalse(Files.exists(source));
		assertTrue(Files.exists(target));
		List<String> movedContent = Files.readAllLines(target);
		assertEquals(sourceContent, movedContent.get(0));
	}

	@Test (expected = NoSuchFileException.class)
	public void test_move_non_existing_file() throws Exception {
		final Path source = m_testInitializer.getRoot().resolve("non-existing-file");
		final Path target = source.getParent().resolve("movedFile");

		Files.move(source, target);
	}

	@Test (expected = FileAlreadyExistsException.class)
	public void test_move_file_to_already_existing_file_without_replace_throws_exception() throws Exception {
		String sourceContent = "The source content";
		Path source = m_testInitializer.createFileWithContent(sourceContent, "dir", "file");
		String targetContent = "The target content";
		Path existingTarget = m_testInitializer.createFileWithContent(targetContent, "dir", "target");
		
		Files.move(source, existingTarget);
	}
	
	@Test
	public void test_move_file_to_already_existing_file_with_replace() throws Exception {
		String sourceContent = "The source content";
		Path source = m_testInitializer.createFileWithContent(sourceContent, "dir", "file");
		String targetContent = "The target content";
		Path existingTarget = m_testInitializer.createFileWithContent(targetContent, "dir", "target");
		
		Files.move(source, existingTarget, StandardCopyOption.REPLACE_EXISTING);
		
		assertFalse(Files.exists(source));
		assertTrue(Files.exists(existingTarget));
		List<String> movedContent = Files.readAllLines(existingTarget);
		assertEquals(sourceContent, movedContent.get(0));
	}
	
	@Test (expected = NoSuchFileException.class)
	public void test_move_file_to_non_existing_directory_throws_exception() throws Exception {
		String sourceContent = "The source content";
		Path source = m_testInitializer.createFileWithContent(sourceContent, "dir", "fileA");
		Path target = m_testInitializer.getRoot().resolve("dirB").resolve("fileB");
		
		Files.move(source, target);
	}

	@Test
	public void test_move_file_to_itself() throws Exception {
		final String testContent = "Some simple test content";
		final Path source = m_testInitializer.createFileWithContent(testContent, "dirA", "fileA");

		Files.move(source, source, StandardCopyOption.REPLACE_EXISTING);

		assertTrue(Files.exists(source));
		final List<String> copiedContent = Files.readAllLines(source);
		assertEquals(1, copiedContent.size());
		assertEquals(testContent, copiedContent.get(0));
	}

	@Test(expected = FileAlreadyExistsException.class)
	public void test_move_directory_to_other_directory() throws Exception {
		final String testContent = "Some simple test content";
		final Path dirA = m_testInitializer.createFileWithContent(testContent, "dirA", "fileA").getParent();
		final Path dirB = m_testInitializer.createFileWithContent(testContent, "dirB", "fileB").getParent();

		Files.copy(dirA, dirB);

		assertTrue(Files.exists(dirB.resolve("dirA").resolve("fileA")));
	}

	@Test(expected = DirectoryNotEmptyException.class)
	public void test_move_directory_with_replace_to_non_empty_existing_directory() throws Exception {
		final String testContent = "Some simple test content";
		final Path dirA = m_testInitializer.createFileWithContent(testContent, "dirA", "fileA").getParent();
		final Path dirB = m_testInitializer.createFileWithContent(testContent, "dirB", "fileB").getParent();

		Files.move(dirA, dirB, StandardCopyOption.REPLACE_EXISTING);
	}

	@Test
	public void test_deep_cache_invalidation() throws Exception {
		final Path file = m_testInitializer.createFile("dir-A1", "dir-A2", "dir-A3", "file-A4");

		assertTrue(Files.exists(file));
		Files.move(m_testInitializer.makePath("dir-A1"), m_testInitializer.makePath("dir-B1"));
		assertFalse(Files.exists(file)); // old file does not exists anymore
	}

	@Test
	public void test_deep_parent_dir_cache_invalidation() throws Exception {
		final Path fileA3 = m_testInitializer.createFile("dir-A1", "dir-A2", "file-A3");
		final Path dirA2 = fileA3.getParent();
		final Path dirA1 = dirA2.getParent();
		final Path fileB2 = m_testInitializer.createFile("dir-B1", "file-B2");
		final Path dirB1 = fileB2.getParent();
		
		// load file attributes
		assertTrue(Files.isRegularFile(fileA3));
		assertTrue(Files.exists(fileA3));

		// load dir-A1 and childs into cache
		final List<Path> beforeA1 = listDir(dirA1);
		assertTrue(beforeA1.contains(dirA2));
		assertEquals(1, beforeA1.size());

		// load dir-B1 and childs into cache
		final List<Path> beforeB1 = listDir(dirB1);
		assertTrue(beforeB1.contains(fileB2));
		assertEquals(1, beforeB1.size());

		// move dir-A1/dir-A2 to dir-B1/dir-B3
		final Path dirB3 = m_testInitializer.makePath("dir-B1", "dir-B3");
		Files.move(dirA2, dirB3);

		// check file attributes
		assertFalse(Files.isRegularFile(fileA3));
		assertFalse(Files.exists(fileA3));

		// check dir-A1 is now empty
		final List<Path> afterA1 = listDir(dirA1);
		assertEquals(0, afterA1.size());

		// check dir-B1 contains new childs
		final List<Path> afterB1 = listDir(dirB1);
		assertTrue(afterB1.contains(fileB2));
		assertTrue(afterB1.contains(dirB3));
		assertEquals(2, afterB1.size());
	}
	
	private static List<Path> listDir(final Path path) throws IOException {
		final List<Path> list = new ArrayList<>();
		try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path, p -> true)) {
			directoryStream.forEach(list::add);
		}
		return list;
	}

}