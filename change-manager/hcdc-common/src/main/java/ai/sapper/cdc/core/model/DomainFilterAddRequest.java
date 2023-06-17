/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.cdc.core.model;

import ai.sapper.cdc.core.filters.Filter;
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
