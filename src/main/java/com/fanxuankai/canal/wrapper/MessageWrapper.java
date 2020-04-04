package com.fanxuankai.canal.wrapper;

import com.alibaba.otter.canal.protocol.Message;
import lombok.Getter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
public class MessageWrapper {
    private Message raw;
    @Getter
    private List<EntryWrapper> entryWrapperList;
    @Getter
    private int allRawRowDataCount;

    public MessageWrapper(Message raw) {
        this.raw = raw;
        this.entryWrapperList = raw.getEntries().stream().map(EntryWrapper::new).collect(Collectors.toList());

        this.allRawRowDataCount = this.entryWrapperList
                .stream()
                .map(EntryWrapper::getRawRowDataCount)
                .reduce(Integer::sum)
                .orElse(0);
    }

    public long getBatchId() {
        return raw.getId();
    }
}
