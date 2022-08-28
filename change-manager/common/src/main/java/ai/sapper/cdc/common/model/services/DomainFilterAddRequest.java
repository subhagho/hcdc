package ai.sapper.cdc.common.model.services;

import ai.sapper.cdc.common.filters.Filter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
public class DomainFilterAddRequest {
    private String entity;
    private String group;
    private List<Filter> filters;

    public static class DomainFilterAddRequestBuilder {
        private DomainFilterAddRequest request = new DomainFilterAddRequest();

        public DomainFilterAddRequestBuilder withEntity(@NonNull String entity) {
            request.setEntity(entity);
            return this;
        }

        public DomainFilterAddRequestBuilder withGroup(@NonNull String group) {
            request.setGroup(group);
            return this;
        }

        public DomainFilterAddRequestBuilder withFilter(@NonNull Filter filter) {
            if (request.filters == null) {
                request.filters = new ArrayList<>();
            }
            request.filters.add(filter);
            return this;
        }

        public DomainFilterAddRequest build() {
            Preconditions.checkState(!Strings.isNullOrEmpty(request.entity));
            Preconditions.checkNotNull(request.filters);
            Preconditions.checkState(!request.filters.isEmpty());
            return request;
        }
    }
}
