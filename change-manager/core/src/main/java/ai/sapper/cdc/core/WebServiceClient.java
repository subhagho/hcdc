package ai.sapper.cdc.core;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.ConnectionError;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.WebServiceConnection;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.http.HttpStatus;
import org.glassfish.jersey.client.JerseyWebTarget;

import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

@Getter
@Accessors(fluent = true)
public class WebServiceClient {
    private WebServiceConnection connection;
    private WebServiceClientConfig config;

    public WebServiceClient() {
    }

    public WebServiceClient(@NonNull WebServiceConnection connection) {
        this.connection = connection;
    }

    public WebServiceClient init(@NonNull HierarchicalConfiguration<ImmutableNode> xmlConfig,
                                 @NonNull String configPath,
                                 @NonNull ConnectionManager manager) throws ConfigurationException {
        try {
            config = new WebServiceClientConfig(xmlConfig, configPath);
            config.read();

            connection = manager.getConnection(config.connection, WebServiceConnection.class);
            if (connection == null) {
                throw new ConfigurationException(
                        String.format("Connection not found. [name=%s][type=%s]",
                                config.connection, WebServiceConnection.class.getCanonicalName()));
            }

            return this;
        } catch (Exception ex) {
            throw new ConfigurationException(ex);
        }
    }

    public <T> T get(@NonNull String service,
                     @NonNull Class<T> type,
                     List<String> params,
                     String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        String path = config.pathMap.get(service);
        if (Strings.isNullOrEmpty(path)) {
            throw new ConnectionError(String.format("No service registered with name. [name=%s]", service));
        }
        return getUrl(path, type, params, mediaType);
    }

    public <T> T getUrl(@NonNull String path,
                        @NonNull Class<T> type,
                        Map<String, String> query,
                        String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        JerseyWebTarget target = connection.connect(path);
        if (query != null && !query.isEmpty()) {
            for (String q : query.keySet()) {
                target = target.queryParam(q, query.get(q));
            }
        }

        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        return getUrl(target, type, mediaType);
    }

    public <T> T getUrl(@NonNull String path,
                        @NonNull Class<T> type,
                        List<String> params,
                        String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        JerseyWebTarget target = connection.connect(path);
        if (params != null && !params.isEmpty()) {
            for (String param : params) {
                target = target.path(param);
            }
        }

        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        return getUrl(target, type, mediaType);
    }

    private <T> T getUrl(JerseyWebTarget target, Class<T> type, String mediaType) throws ConnectionError {
        Invocation.Builder builder = target.request(mediaType);
        int count = 0;
        boolean handle = true;
        while (true) {
            try {
                Response response = builder.get();
                if (response != null) {
                    if (response.getStatus() != HttpStatus.SC_OK) {
                        handle = false;
                        throw new ConnectionError(String.format("Service returned status = [%d]", response.getStatus()));
                    }
                    return response.readEntity(type);
                }
                throw new ConnectionError(
                        String.format("Service response was null. [service=%s]", target.getUri().toString()));
            } catch (Throwable t) {
                if (handle && count < config().retryCount) {
                    count++;
                    DefaultLogger.LOGGER.error(
                            String.format("Error calling web service. [tries=%d][service=%s]",
                                    count, target.getUri().toString()), t);
                } else {
                    throw t;
                }
            }
        }
    }

    public <R, T> T post(@NonNull String service,
                         @NonNull Class<T> type,
                         @NonNull R request,
                         List<String> params,
                         String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        String path = config.pathMap.get(service);
        if (Strings.isNullOrEmpty(path)) {
            throw new ConnectionError(String.format("No service registered with name. [name=%s]", service));
        }
        JerseyWebTarget target = connection.connect(path);
        if (params != null && !params.isEmpty()) {
            for (String param : params) {
                target = target.path(param);
            }
        }
        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        Invocation.Builder builder = target.request(mediaType);
        int count = 0;
        boolean handle = true;
        while (true) {
            try {
                try (Response response = builder.post(Entity.entity(request, mediaType))) {
                    if (response != null) {
                        if (response.getStatus() != HttpStatus.SC_OK) {
                            handle = false;
                            throw new ConnectionError(String.format("Service returned status = [%d]", response.getStatus()));
                        }
                        return response.readEntity(type);
                    }
                    throw new ConnectionError(
                            String.format("Service response was null. [service=%s]", target.getUri().toString()));
                }
            } catch (Throwable t) {
                if (handle && count < config().retryCount) {
                    count++;
                    DefaultLogger.LOGGER.error(
                            String.format("Error calling web service. [tries=%d][service=%s]",
                                    count, target.getUri().toString()), t);
                } else {
                    throw t;
                }
            }
        }
    }

    public <R, T> T put(@NonNull String service,
                        @NonNull Class<T> type,
                        @NonNull R request,
                        List<String> params,
                        String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        String path = config.pathMap.get(service);
        if (Strings.isNullOrEmpty(path)) {
            throw new ConnectionError(String.format("No service registered with name. [name=%s]", service));
        }
        JerseyWebTarget target = connection.connect(path);
        if (params != null && !params.isEmpty()) {
            for (String param : params) {
                target = target.path(param);
            }
        }
        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        Invocation.Builder builder = target.request(mediaType);
        int count = 0;
        boolean handle = true;
        while (true) {
            try {
                try (Response response = builder.put(Entity.entity(request, mediaType))) {
                    if (response != null) {
                        if (response.getStatus() != HttpStatus.SC_OK) {
                            handle = false;
                            throw new ConnectionError(String.format("Service returned status = [%d]", response.getStatus()));
                        }
                        return response.readEntity(type);
                    }
                    throw new ConnectionError(
                            String.format("Service response was null. [service=%s]", target.getUri().toString()));
                }
            } catch (Throwable t) {
                if (handle && count < config().retryCount) {
                    count++;
                    DefaultLogger.LOGGER.error(
                            String.format("Error calling web service. [tries=%d][service=%s]",
                                    count, target.getUri().toString()), t);
                } else {
                    throw t;
                }
            }
        }
    }

    public <T> T delete(@NonNull String service,
                        @NonNull Class<T> type,
                        List<String> params,
                        String mediaType) throws ConnectionError {
        Preconditions.checkNotNull(connection);
        String path = config.pathMap.get(service);
        if (Strings.isNullOrEmpty(path)) {
            throw new ConnectionError(String.format("No service registered with name. [name=%s]", service));
        }
        JerseyWebTarget target = connection.connect(path);
        if (params != null && !params.isEmpty()) {
            for (String param : params) {
                target = target.path(param);
            }
        }
        if (Strings.isNullOrEmpty(mediaType)) {
            mediaType = MediaType.APPLICATION_JSON;
        }
        Invocation.Builder builder = target.request(mediaType);
        int count = 0;
        boolean handle = true;
        while (true) {
            try {
                try (Response response = builder.delete()) {
                    if (response != null) {
                        if (response.getStatus() != HttpStatus.SC_OK) {
                            handle = false;
                            throw new ConnectionError(String.format("Service returned status = [%d]", response.getStatus()));
                        }
                        return response.readEntity(type);
                    }
                    throw new ConnectionError(
                            String.format("Service response was null. [service=%s]", target.getUri().toString()));
                }
            } catch (Throwable t) {
                if (handle && count < config().retryCount) {
                    count++;
                    DefaultLogger.LOGGER.error(
                            String.format("Error calling web service. [tries=%d][service=%s]",
                                    count, target.getUri().toString()), t);
                } else {
                    throw t;
                }
            }
        }
    }

    public static class WebServiceClientConfig extends ConfigReader {
        private static final String __CONFIG_PATH = "client";

        public static class Constants {
            public static final String CONFIG_NAME = "name";
            public static final String CONFIG_CONNECTION = "connection";
            public static final String CONFIG_PATH_MAP = "paths";
            public static final String CONFIG_RETRIES = "retryCount";
        }

        private String name;
        private String connection;
        private Map<String, String> pathMap;
        private int retryCount = 0;

        public WebServiceClientConfig(@NonNull HierarchicalConfiguration<ImmutableNode> config, @NonNull String path) {
            super(config, String.format("%s.%s", path, __CONFIG_PATH));
        }

        public void read() throws ConfigurationException {
            if (get() == null) {
                throw new ConfigurationException("WebService client Configuration not set or is NULL");
            }
            name = get().getString(Constants.CONFIG_NAME);
            if (Strings.isNullOrEmpty(name)) {
                throw new ConfigurationException(String.format("WebService connection Configuration Error: missing [%s]",
                        Constants.CONFIG_NAME));
            }
            connection = get().getString(Constants.CONFIG_CONNECTION);
            if (Strings.isNullOrEmpty(connection)) {
                throw new ConfigurationException(String.format("WebService connection Configuration Error: missing [%s]",
                        Constants.CONFIG_CONNECTION));
            }
            if (ConfigReader.checkIfNodeExists(get(), Constants.CONFIG_PATH_MAP)) {
                pathMap = ConfigReader.readAsMap(get(), Constants.CONFIG_PATH_MAP);
            }
            if (pathMap == null || pathMap.isEmpty()) {
                throw new ConfigurationException(String.format("WebService connection Configuration Error: missing [%s]",
                        Constants.CONFIG_PATH_MAP));
            }
            String s = get().getString(Constants.CONFIG_RETRIES);
            if (!Strings.isNullOrEmpty(s)) {
                retryCount = Integer.parseInt(s);
            }
        }
    }
}
