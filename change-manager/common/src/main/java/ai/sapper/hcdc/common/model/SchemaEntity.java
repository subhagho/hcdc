package ai.sapper.hcdc.common.model;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class SchemaEntity {
    private String domain;
    private String entity;

    public SchemaEntity() {
    }

    public SchemaEntity(@NonNull String domain, @NonNull String entity) {
        this.domain = domain;
        this.entity = entity;
    }
}
