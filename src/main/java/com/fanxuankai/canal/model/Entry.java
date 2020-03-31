package com.fanxuankai.canal.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author fanxuankai
 */
@AllArgsConstructor
@Getter
public class Entry {
    private String name;
    private Object value;

    public static List<Entry> fromMap(LinkedHashMap<String, Object> map) {
        return map.entrySet()
                .stream()
                .map(entry -> new Entry(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Entry entry = (Entry) o;
        return name.equals(entry.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
