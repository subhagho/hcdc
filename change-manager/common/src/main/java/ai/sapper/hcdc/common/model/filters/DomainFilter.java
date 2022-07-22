package ai.sapper.hcdc.common.model.filters;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

import java.util.Objects;

@Getter
@Setter
@ToString(exclude = {"createdTime", "updatedTime"})
public class DomainFilter {
    private String path;
    private String regex;
    private long createdTime;
    private long updatedTime;

    public DomainFilter() {
    }

    public DomainFilter(@NonNull String path, @NonNull String regex) {
        this.path = path;
        this.regex = regex;
        this.createdTime = System.currentTimeMillis();
        this.updatedTime = this.createdTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DomainFilter)) return false;
        DomainFilter that = (DomainFilter) o;
        return path.equals(that.path) && regex.equals(that.regex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, regex);
    }
}
