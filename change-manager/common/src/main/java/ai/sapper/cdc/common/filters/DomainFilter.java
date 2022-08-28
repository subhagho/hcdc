package ai.sapper.cdc.common.filters;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
@Setter
@ToString(exclude = {"createdTime", "updatedTime"})
public class DomainFilter {
    private String domain;
    private String entity;
    private String group;
    private List<Filter> filters;
    private long createdTime;
    private long updatedTime;

    public DomainFilter() {
    }

    public DomainFilter(@NonNull String domain, @NonNull String entity) {
        this.domain = domain;
        this.entity = entity;
        this.createdTime = System.currentTimeMillis();
        this.updatedTime = this.createdTime;
    }

    public DomainFilter withGroup(String group) {
        this.group = group;
        return this;
    }

    public synchronized Filter add(@NonNull String path, @NonNull String regex) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(regex));
        if (filters == null) {
            filters = new ArrayList<>();
        }
        Filter filter = get(path, regex);
        if (filter == null) {
            filter = new Filter(entity, path, regex);
            filters.add(filter);
            updatedTime = System.currentTimeMillis();
        }
        return filter;
    }

    public synchronized List<Filter> remove(@NonNull String path) {
        boolean ret = false;
        if (filters != null && !filters.isEmpty()) {
            List<Integer> indexes = new ArrayList<>();
            List<Filter> fs = new ArrayList<>();
            for (int ii = 0; ii < filters.size(); ii++) {
                Filter filter = filters.get(ii);
                if (filter.getPath().compareTo(path) == 0) {
                    indexes.add(ii);
                    fs.add(filter);
                }
            }
            if (!indexes.isEmpty()) {
                for (int index : indexes) {
                    filters.remove(index);
                }
                return fs;
            }
        }
        return null;
    }

    public synchronized Filter remove(@NonNull String path, @NonNull String regex) {
        if (filters != null && !filters.isEmpty()) {
            int index = -1;
            Filter filter = null;
            for (int ii = 0; ii < filters.size(); ii++) {
                filter = filters.get(ii);
                if (filter.getPath().compareTo(path) == 0
                        && filter.getRegex().compareTo(regex) == 0) {
                    index = ii;
                    break;
                }
            }
            if (index >= 0) {
                filters.remove(index);
                updatedTime = System.currentTimeMillis();
                return filter;
            }
        }
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DomainFilter)) return false;
        DomainFilter that = (DomainFilter) o;
        if (domain.compareTo(that.getDomain()) == 0
                && entity.compareTo(that.getEntity()) == 0) {
            if (filters != null && filters.size() == that.filters.size()) {
                for (Filter filter : filters) {
                    if (!that.hasFilter(filter)) return false;
                }
                return true;
            } else return filters == null && that.filters == null;
        }
        return false;
    }

    public List<Filter> find(@NonNull String path) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(path));
        if (filters != null && !filters.isEmpty()) {
            List<Filter> fs = new ArrayList<>();
            for (Filter filter : filters) {
                if (filter.getPath().compareTo(path) == 0) {
                    fs.add(filter);
                }
            }
            if (!fs.isEmpty()) return fs;
        }
        return null;
    }

    public boolean hasFilter(@NonNull Filter filter) {
        return hasFilter(filter.getPath(), filter.getRegex());
    }

    public boolean hasFilter(@NonNull String path, @NonNull String regex) {
        if (filters != null && !filters.isEmpty()) {
            for (Filter f : filters) {
                if (f.getPath().compareTo(path) == 0
                        && f.getRegex().compareTo(regex) == 0) {
                    return true;
                }
            }
        }
        return false;
    }

    public Filter get(@NonNull String path, @NonNull String regex) {
        if (filters != null && !filters.isEmpty()) {
            for (Filter f : filters) {
                if (f.getPath().compareTo(path) == 0
                        && f.getRegex().compareTo(regex) == 0) {
                    return f;
                }
            }
        }
        return null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(domain, entity, filters);
    }
}
