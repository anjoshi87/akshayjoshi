package com.kantar.sessionsjob;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Main Test Class
 */
public class MainTest {

    private String srcFilePath = "src/test/resources/input-statements.psv";
    private String targetFilePath = "target/actual-sessions.psv";
    private String expectedFilePath = "src/test/resources/expected-sessions.psv";


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
    void testOutputFileGenerated() throws IOException {
        String args[] = {srcFilePath, targetFilePath};
        Main.processSession(args);
        //Check if output File is created
        assertTrue(Files.exists(Paths.get(targetFilePath)));
    }

    /**
     * test OutputFile Generated in target folder
     */
    @Test
    void testOutputFileComparison() throws IOException {
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
     * test OutputFile generation with invalid input source file path
     */
    @Test
    void testOutputFileNegative() {
        String args[] = {"fakeSourcePath", targetFilePath};
        assertThrows(
                FileNotFoundException.class,
                () -> Main.processSession(args) // This will throw a FileNotFoundException
        );
    }

    /**
     * test if content length of expected & generated output File matching
     */
    @Test
    void testOutputFileComparisonWithInvalidData() throws IOException {
        String args[] = {srcFilePath, targetFilePath};
        Main.processSession(args);
        Path expectedPath = Paths.get(expectedFilePath);
        Path actualPath = Paths.get(targetFilePath);
        int unExpectedFileLength = Files.lines(expectedPath).map(s -> s.concat("tt")).collect(Collectors.joining()).length();
        int actualFileLength = Files.lines(expectedPath).collect(Collectors.joining()).length();
        //Check if content length of expected & generated output File matching
        assertNotEquals(unExpectedFileLength,actualFileLength);
    }

}
