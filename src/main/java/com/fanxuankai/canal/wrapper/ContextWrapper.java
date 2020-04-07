package com.fanxuankai.canal.wrapper;

import com.fanxuankai.canal.flow.Context;
import lombok.Getter;
import lombok.Setter;

/**
 * @author fanxuankai
 */
public class ContextWrapper {

    private Context raw;

    @Getter
    private MessageWrapper messageWrapper;

    @Setter
    private boolean handleError;

    public ContextWrapper(Context raw) {
        this.raw = raw;
        this.messageWrapper = new MessageWrapper(raw.getMessage());
    }

    public void confirm() {
        if (handleError) {
            raw.rollback();
        } else {
            raw.ack();
        }
    }

}
