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

import java.util.Objects;

@Getter
@Setter
public class Filter {
    private String entity;
    private String path;
    private String regex;

    public Filter() {
    }

    public Filter(@NonNull String entity,
                  @NonNull String path,
                  @NonNull String regex) {
        this.entity = entity;
        this.path = path;
        this.regex = regex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Filter)) return false;
        Filter filter = (Filter) o;
        return entity.equals(filter.entity) && path.equals(filter.path) && regex.equals(filter.regex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(entity, path, regex);
    }
}
