package org.makeyourcase.admin;

public class AdminServiceRuntimeException extends RuntimeException {
    public AdminServiceRuntimeException(Exception e) {
        super(e);
    }
}
