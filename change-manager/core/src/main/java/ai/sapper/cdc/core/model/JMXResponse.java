package ai.sapper.cdc.core.model;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Getter
@Setter
public class JMXResponse {
    public static final String KEY_BEAN_NAME = "name";

    private List<Map<String, String>> beans;

    public Map<String, String> findBeanByName(@NonNull String regex) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(regex));
        if (beans != null && !beans.isEmpty()) {
            Pattern pattern = Pattern.compile(regex);
            for (Map<String, String> bean : beans) {
                if (bean.containsKey(KEY_BEAN_NAME)) {
                    String name = bean.get(KEY_BEAN_NAME);
                    Matcher m = pattern.matcher(name);
                    if (m.matches()) return bean;
                }
            }
        }
        return null;
    }
}
