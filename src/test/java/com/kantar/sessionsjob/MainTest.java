package com.kantar.sessionsjob;


import com.kantar.sessionsjob.exception.FileGenerationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Main Test Class
 */
public class MainTest {

    private String srcFilePath = "src/test/resources/input-statements.psv";
    private String targetFilePath = "target/actual-sessions.psv";
    private String expectedFilePath = "src/test/resources/expected-sessions.psv";
    private String invalidSourceFile = "src/test/resources/invalid-input-statements.psv";
    private String invalidTargetFileName = "invalidTargetFileName";


    @BeforeEach
    void setUp() {
        try {
            //Delete file if created in target
            Files.deleteIfExists(Paths.get(targetFilePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * test OutputFile Generated in target folder
     */
    @Test
    void testOutputFileGenerated() throws FileGenerationException {
        String args[] = {srcFilePath, targetFilePath};
        Main.processSession(args);
        //Check if output File is created
        assertTrue(Files.exists(Paths.get(targetFilePath)));
    }

    /**
     * test OutputFile Generated in target folder & compare content with expected file
     */
    @Test
    void testOutputFileComparison() throws FileGenerationException, IOException {
        String args[] = {srcFilePath, targetFilePath};
        Main.processSession(args);
        Path expectedPath = Paths.get(expectedFilePath);
        Path actualPath = Paths.get(targetFilePath);
        byte[] expectedBytes = Files.readAllBytes(expectedPath);
        byte[] actualBytes = Files.readAllBytes(actualPath);
        //Check if content of expected & generated output File matching
        assertArrayEquals(expectedBytes, actualBytes, "File contents matched");
    }

    /**
     * test file generation with invalid input file data
     */
    @Test
    void testOutputFileComparisonWithInvalidSrcFileData() {
        String args[] = {invalidSourceFile, targetFilePath};
        assertThrows(
                FileGenerationException.class,
                () -> Main.processSession(args) // This will throw a FileGenerationException
        );
    }


    /**
     * test OutputFile generation with invalid input source file path
     */
    @Test
    void testOutputFileWithInvalidSourcePath() {
        String args[] = {"invalidSourcePath", targetFilePath};
        assertThrows(
                FileGenerationException.class,
                () -> Main.processSession(args) // This will throw a FileGenerationException
        );
    }

    /**
     * Compare File Generation Error Message
     */
    @Test
    void testOutputFileWithInvalidSourcePath11() {
        String args[] = {"invalidSourcePath", targetFilePath};
        try {
            Main.processSession(args); // This will throw a FileGenerationException
        } catch (FileGenerationException e) {
            assertEquals(e.getMessage(), "Error while processing Session. Output file generation failed.");
        }
    }

    /**
     * test OutputFile generation with invalid target source file path
     */
    @Test
    void testOutputFileWithInvalidTargetPath() {
        String args[] = {srcFilePath, "invalidTargetFolder/" + invalidTargetFileName};
        assertThrows(
                FileGenerationException.class,
                () -> Main.processSession(args) // This will throw a FileNotFoundException
        );
    }

    /**
     * test if target file created at unexpected path
     */
    @Test
    void testOutputFileNotAtExpectedPath() throws FileGenerationException {
        String args[] = {srcFilePath, invalidTargetFileName};
        Main.processSession(args);
        Path targetPath = Paths.get(targetFilePath);
        //Check if output File is created
        assertFalse(Files.exists(targetPath));
        //Generated target File at unexpected location deleted in 'cleanup' method
    }

    @AfterEach
    void cleanup() {
        try {
            //Delete file if created in target
            Files.deleteIfExists(Paths.get(invalidTargetFileName));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
