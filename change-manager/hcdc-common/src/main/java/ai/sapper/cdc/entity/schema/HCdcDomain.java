package ai.sapper.cdc.entity.schema;

import ai.sapper.cdc.core.filters.DomainFilters;
import ai.sapper.cdc.entity.schema.Domain;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class HCdcDomain extends Domain {
    private DomainFilters filters;
}
