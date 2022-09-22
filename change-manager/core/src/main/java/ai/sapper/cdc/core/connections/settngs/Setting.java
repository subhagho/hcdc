package ai.sapper.cdc.core.connections.settngs;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Setting {
    String name();

    boolean required() default true;

    Class<? extends SettingParser<?>> parser() default SettingParser.NullSettingParser.class;
}
