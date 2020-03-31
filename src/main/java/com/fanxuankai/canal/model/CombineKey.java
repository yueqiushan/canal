package com.fanxuankai.canal.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.List;

/**
 * @author fanxuankai
 */
@AllArgsConstructor
@Getter
public class CombineKey {
    private List<Entry> entries;
}
