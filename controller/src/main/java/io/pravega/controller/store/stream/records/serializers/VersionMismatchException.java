package io.pravega.controller.store.stream.records.serializers;

public class VersionMismatchException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Version mismatch during deserialization %s.%s.";

    /**
     * Creates a new instance of VersionMismatchException class.
     *
     * @param className resource on which lock failed
     */
    public VersionMismatchException(final String className) {
        super(String.format(FORMAT_STRING, className));
    }
}
