package io.pravega.controller.store.stream;

import lombok.Data;

@Data
public class VersionedMetadata<Obj> {
    private final Obj object;
    private final Integer version;
}
