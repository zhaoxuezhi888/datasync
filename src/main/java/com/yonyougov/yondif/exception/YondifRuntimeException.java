package com.yonyougov.yondif.exception;

/**
* 自定义异常类
* @Author zxz
* */
public class YondifRuntimeException  extends RuntimeException {

    private static final long serialVersionUID = 888856912557685442L;

    /**
     * Creates a new Exception with the given message and null as the cause.
     *
     * @param message The exception message
     */
    public YondifRuntimeException(String message) {
        super(message);
    }

    /**
     * Creates a new exception with a null message and the given cause.
     *
     * @param cause The exception that caused this exception
     */
    public YondifRuntimeException(Throwable cause) {
        super(cause);
    }

    /**
     * Creates a new exception with the given message and cause.
     *
     * @param message The exception message
     * @param cause The exception that caused this exception
     */
    public YondifRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
