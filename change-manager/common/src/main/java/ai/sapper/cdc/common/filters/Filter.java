package ai.sapper.cdc.common.filters;

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
