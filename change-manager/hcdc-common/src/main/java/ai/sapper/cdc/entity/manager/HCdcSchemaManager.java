package ai.sapper.cdc.entity.manager;

import ai.sapper.cdc.core.connections.hadoop.HdfsConnection;
import ai.sapper.cdc.core.filters.*;
import ai.sapper.cdc.core.processing.ProcessorState;
import ai.sapper.cdc.entity.avro.AvroEntitySchema;
import ai.sapper.cdc.entity.schema.Domain;
import ai.sapper.cdc.entity.schema.EntitySchema;
import ai.sapper.cdc.entity.schema.HCdcDomain;
import ai.sapper.cdc.entity.schema.SchemaEntity;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.ex.ConfigurationException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Getter
@Accessors(fluent = true)
public class HCdcSchemaManager extends SchemaManager {
    public static final String DEFAULT_DOMAIN = "HadoopCDC";

    private static final String IGNORE_REGEX = "(.*)\\.(_*)COPYING(_*)|/tmp/(.*)|(.*)\\.hive-staging(.*)";
    private HdfsConnection hdfsConnection;
    private Map<String, DomainFilterMatcher> matchers = new HashMap<>();
    private final List<FilterAddCallback> callbacks = new ArrayList<>();
    private Pattern ignorePattern = Pattern.compile(IGNORE_REGEX);


    public HCdcSchemaManager() {
        super(HCdcSchemaManagerSettings.class);
    }

    @Override
    public Domain createDomain(@NonNull String name) throws Exception {
        HCdcDomain domain = new HCdcDomain();
        domain.setName(name);
        return domain;
    }

    @Override
    public SchemaEntity createEntity(@NonNull String domain,
                                     @NonNull String name) throws Exception {
        SchemaEntity se = getEntity(domain, name);
        if (se != null) {
            return se;
        }
        return new SchemaEntity(domain, name);
    }

    @Override
    public EntitySchema createSchema(@NonNull SchemaEntity schemaEntity) throws Exception {
        AvroEntitySchema schema = getSchema(schemaEntity, AvroEntitySchema.class);
        if (schema != null) {
            return schema;
        }
        return createSchema(schemaEntity, AvroEntitySchema.class);
    }

    public void addFilterCallback(@NonNull FilterAddCallback callback) {
        callbacks.add(callback);
    }

    @Override
    protected void init(@NonNull SchemaManagerSettings settings) throws ConfigurationException {
        Preconditions.checkArgument(settings instanceof HCdcSchemaManagerSettings);
        try {
            hdfsConnection = env().connectionManager()
                    .getConnection(((HCdcSchemaManagerSettings) settings).getHdfsConnection(), HdfsConnection.class);
            if (hdfsConnection == null) {
                throw new ConfigurationException(
                        String.format("HDFS Connection not found. [name=%s]",
                                ((HCdcSchemaManagerSettings) settings).getHdfsConnection()));
            }
            if (!Strings.isNullOrEmpty(((HCdcSchemaManagerSettings) settings).getIgnoreRegEx())) {
                ignorePattern = Pattern.compile(((HCdcSchemaManagerSettings) settings).getIgnoreRegEx());
            }
            initFilters(true);
            state().setState(ProcessorState.EProcessorState.Running);
        } catch (Exception ex) {
            state().error(ex);
            throw new ConfigurationException(ex);
        }
    }

    private void initFilters(boolean useCallbacks) throws Exception {
        List<Domain> domains = listDomains();
        if (domains != null && !domains.isEmpty()) {
            for (Domain domain : domains) {
                Preconditions.checkArgument(domain instanceof HCdcDomain);
                DomainFilters df = ((HCdcDomain) domain).getFilters();
                if (df != null) {
                    DomainFilterMatcher m = new DomainFilterMatcher(df.getDomain(), df)
                            .withIgnoreRegex(ignorePattern);
                    matchers.put(df.getDomain(), m);
                    if (useCallbacks && !callbacks.isEmpty()) {
                        for (FilterAddCallback callback : callbacks) {
                            callback.onStart(m);
                        }
                    }
                }
            }
        }
    }

    public void refresh() throws Exception {
        schemaLock().lock();
        try {
            initFilters(false);
        } finally {
            schemaLock().unlock();
        }
    }

    public SchemaEntity matches(@NonNull String path) {
        Preconditions.checkState(state().isRunning());

        if (matchers != null && !matchers.isEmpty()) {
            Map<String, DomainFilterMatcher> ms = matchers;
            for (String d : matchers.keySet()) {
                DomainFilterMatcher m = ms.get(d);
                DomainFilterMatcher.PathFilter pf = m.matches(path);
                if (pf != null) {
                    SchemaEntity dd = new SchemaEntity();
                    dd.setDomain(m.filters().getDomain());
                    dd.setEntity(pf.filter().getEntity());
                    return dd;
                }
            }
        }
        return null;
    }

    public DomainFilter updateGroup(@NonNull String domain,
                                    @NonNull String entity,
                                    @NonNull String group) {
        if (matchers.containsKey(domain)) {
            DomainFilterMatcher matcher = matchers.get(domain);
            return matcher.updateGroup(entity, group);
        }
        return null;
    }

    public DomainFilters add(@NonNull String domain,
                             @NonNull String entity,
                             @NonNull String path,
                             @NonNull String regex,
                             String group) throws Exception {
        Preconditions.checkState(state().isRunning());

        DomainFilterMatcher matcher = null;
        DomainFilterMatcher.PathFilter filter = null;
        if (!matchers.containsKey(domain)) {
            DomainFilters df = new DomainFilters();
            df.setDomain(domain);
            Filter f = df.add(entity, path, regex, group);

            matcher = new DomainFilterMatcher(domain, df)
                    .withIgnoreRegex(ignorePattern);
            matchers.put(domain, matcher);
            filter = matcher.find(f);
        } else {
            matcher = matchers.get(domain);
            filter = matcher.add(entity, path, regex, group);
        }

        updateDomainFilters(domain, matcher.filters());
        if (!callbacks.isEmpty()) {
            for (FilterAddCallback callback : callbacks) {
                callback.process(matcher, filter, path);
            }
        }
        return matcher.filters();
    }

    public DomainFilter remove(@NonNull String domain,
                               @NonNull String entity) throws Exception {
        Preconditions.checkState(state().isRunning());
        if (matchers.containsKey(domain)) {
            DomainFilterMatcher matcher = matchers.get(domain);
            DomainFilter df = matcher.remove(entity);
            if (df != null) {
                updateDomainFilters(domain, matcher.filters());
            }
            return df;
        }
        return null;
    }

    public List<Filter> remove(@NonNull String domain,
                               @NonNull String entity,
                               @NonNull String path) throws Exception {
        Preconditions.checkState(state().isRunning());
        if (matchers.containsKey(domain)) {
            DomainFilterMatcher matcher = matchers.get(domain);
            List<Filter> fs = matcher.remove(entity, path);
            if (fs != null && !fs.isEmpty()) {
                updateDomainFilters(domain, matcher.filters());
            }
            return fs;
        }
        return null;
    }

    public Filter remove(@NonNull String domain,
                         @NonNull String entity,
                         @NonNull String path,
                         @NonNull String regex) throws Exception {
        Preconditions.checkState(state().isRunning());
        if (matchers.containsKey(domain)) {
            DomainFilterMatcher matcher = matchers.get(domain);
            Filter fs = matcher.remove(entity, path, regex);
            if (fs != null) {
                updateDomainFilters(domain, matcher.filters());
            }
            return fs;
        }
        return null;
    }

    private void updateDomainFilters(String domain, DomainFilters filters) throws Exception {
        synchronized (this) {
            Domain d = getDomain(domain);
            if (!(d instanceof HCdcDomain)) {
                throw new Exception(String.format("Domain not found. [domain=%s]", domain));
            }
            ((HCdcDomain) d).setFilters(filters);
            updateDomain(d);
        }
    }
}
