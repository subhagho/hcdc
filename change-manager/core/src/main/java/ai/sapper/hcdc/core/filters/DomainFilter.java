package ai.sapper.hcdc.core.filters;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
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
}
