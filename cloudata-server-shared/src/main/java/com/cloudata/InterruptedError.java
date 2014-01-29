package com.cloudata;

public class InterruptedError extends Error {

    private static final long serialVersionUID = 1L;

    public InterruptedError(InterruptedException e) {
        super(e);
    }

}
