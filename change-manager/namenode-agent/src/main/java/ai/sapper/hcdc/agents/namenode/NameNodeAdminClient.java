package ai.sapper.hcdc.agents.namenode;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.WebServiceClient;
import ai.sapper.cdc.core.connections.WebServiceConnection;
import ai.sapper.cdc.core.model.JMXResponse;
import ai.sapper.hcdc.agents.common.NameNodeError;
import ai.sapper.hcdc.agents.model.NameNodeStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import javax.ws.rs.core.MediaType;
import java.util.HashMap;
import java.util.Map;


@Getter
@Accessors(fluent = true)
public class NameNodeAdminClient {
    public static class Constants {
        public static String PATH_NN_PATH = "jmx";
        public static String PATH_NN_QUERY_KEY = "qry";
        public static String PATH_NN_QUERY = "Hadoop:service=NameNode,name=NameNodeStatus";
        public static String REGEX_NAME = ".*(service=NameNode,name=NameNodeStatus)";
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final String url;
    private WebServiceConnection connection;
    private WebServiceClient client;

    public NameNodeAdminClient(@NonNull String url, boolean useSSL) {
        if (!url.startsWith("http")) {
            if (useSSL) {
                url = String.format("https://%s", url);
            } else {
                url = String.format("http://%s", url);
            }
        }
        this.url = url;
    }

    public NameNodeStatus status() throws NameNodeError {
        try {
            String up = String.format("%s/%s", url, Constants.PATH_NN_QUERY);
            if (connection == null) {
                connection = new WebServiceConnection(getClass().getSimpleName(), url);
                client = new WebServiceClient(connection);
            }
            DefaultLogger.LOGGER.debug(String.format("NameNode Status URL: [%s]", up));
            Map<String, String> query = new HashMap<>(1);
            query.put(Constants.PATH_NN_QUERY_KEY, Constants.PATH_NN_QUERY);

            String json = client.getUrl(Constants.PATH_NN_PATH, String.class, query, MediaType.APPLICATION_JSON);
            JMXResponse response = mapper.readValue(json, JMXResponse.class);
            Map<String, String> bean = response.findBeanByName(Constants.REGEX_NAME);
            if (bean != null) {
                NameNodeStatus status = new NameNodeStatus().parse(bean);
                DefaultLogger.LOGGER.info(
                        String.format("[%s] Received NN state [State=%s][Transition Time=%d]",
                                status.getHost(), status.getState(), status.getHaLastTransitionTime()));
                return status;
            }
            return null;
        } catch (Exception ex) {
            throw new NameNodeError(ex);
        }
    }
}
