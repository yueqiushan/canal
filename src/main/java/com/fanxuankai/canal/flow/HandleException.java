package com.fanxuankai.canal.flow;

/**
 * 处理异常
 *
 * @author fanxuankai
 */
public class HandleException extends RuntimeException {

    public HandleException(String message) {
        super(message);
    }

    public HandleException(Throwable cause) {
        super(cause);
    }
}
