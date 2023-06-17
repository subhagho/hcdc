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

package ai.sapper.cdc.core.filters;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.*;

@Getter
@Setter
public class DomainFilters {
    private String domain;
    private final Map<String, DomainFilter> filters = new HashMap<>();

    public synchronized Filter add(@NonNull String entity,
                                   @NonNull String path,
                                   @NonNull String regex,
                                   String group) {
        DomainFilter filter = filters.get(entity);
        if (filter == null) {
            filter = new DomainFilter(domain, entity).withGroup(group);
            filters.put(entity, filter);
        }
        return filter.add(path, regex);
    }

    public DomainFilter updateGroup(@NonNull String entity, @NonNull String group) {
        DomainFilter filter = filters.get(entity);
        if (filter != null) {
            return filter.withGroup(group);
        }
        return null;
    }

    public DomainFilter getDomainFilter(@NonNull String entity) {
        if (filters.containsKey(entity)) {
            return filters.get(entity);
        }
        return null;
    }

    public List<Filter> get(@NonNull String path) {
        if (!filters.isEmpty()) {
            List<Filter> fs = new ArrayList<>();
            for (String key : filters.keySet()) {
                DomainFilter df = filters.get(key);
                List<Filter> dfs = df.find(path);
                if (dfs != null && !dfs.isEmpty()) {
                    fs.addAll(dfs);
                }
            }
            if (!fs.isEmpty()) return fs;
        }
        return null;
    }

    public List<Filter> get() {
        if (!filters.isEmpty()) {
            List<Filter> fs = new ArrayList<>();
            for (String key : filters.keySet()) {
                DomainFilter df = filters.get(key);
                List<Filter> dfs = df.getFilters();
                if (dfs != null && !dfs.isEmpty()) {
                    fs.addAll(dfs);
                }
            }
            if (!fs.isEmpty()) return fs;
        }
        return null;
    }

    public DomainFilter remove(@NonNull String entity) {
        if (filters.containsKey(entity)) {
            DomainFilter filter = filters.get(entity);
            filters.remove(entity);
            return filter;
        }
        return null;
    }

    public List<Filter> remove(@NonNull String entity,
                               @NonNull String path) {
        if (filters.containsKey(entity)) {
            DomainFilter df = filters.get(entity);
            return df.remove(path);
        }
        return null;
    }

    public Filter remove(@NonNull String entity,
                         @NonNull String path,
                         @NonNull String regex) {
        if (filters.containsKey(entity)) {
            DomainFilter df = filters.get(entity);
            return df.remove(path, regex);
        }
        return null;
    }

    public Set<String> keySet() {
        return filters.keySet();
    }

}
