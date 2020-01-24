package com.rnd.elastic.dal;

class VersionConflictException extends Exception {
    public VersionConflictException(Throwable cause) {
        super(cause);
    }
}
