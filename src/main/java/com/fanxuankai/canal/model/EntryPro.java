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
public class EntryPro {
    private String name;
    private List<Object> values;

    public static List<EntryPro> fromMap(LinkedHashMap<String, List<Object>> map) {
        return map.entrySet()
                .stream()
                .map(entry -> new EntryPro(entry.getKey(), entry.getValue()))
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
        EntryPro entryPro = (EntryPro) o;
        return name.equals(entryPro.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }
}
