package com.kantar.sessionsjob.exception;

//Custom Exception class
public class FileGenerationException extends Exception {
    public FileGenerationException(String message, Throwable cause) {
        super(message, cause); // Call the constructor of the parent Exception class
    }
}
