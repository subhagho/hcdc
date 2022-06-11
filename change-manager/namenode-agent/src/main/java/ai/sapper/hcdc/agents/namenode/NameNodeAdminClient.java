package ai.sapper.hcdc.agents.namenode;

import ai.sapper.hcdc.agents.namenode.model.JMXResponse;
import ai.sapper.hcdc.agents.namenode.model.NameNodeStatus;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;
import lombok.Getter;
import lombok.NonNull;
import lombok.experimental.Accessors;

import javax.ws.rs.core.MediaType;


@Getter
@Accessors(fluent = true)
public class NameNodeAdminClient {
    public static class Constants {
        public static String PATH_NN_STATUS = "jmx?qry=Hadoop:service=NameNode,name=NameNodeStatus";
    }

    private final ObjectMapper mapper = new ObjectMapper();
    private final String url;

    public NameNodeAdminClient(@NonNull String url) {
        if (!url.startsWith("http")) {
            url = String.format("http://%s", url);
        }
        this.url = url;
    }

    public NameNodeStatus status() throws NameNodeError {
        try {
            ClientConfig config = new DefaultClientConfig();
            Client client = Client.create(config);
            String up = String.format("%s/%s", url, Constants.PATH_NN_STATUS);

            WebResource wr = client.resource(up);
            String json = wr.accept(MediaType.APPLICATION_JSON).get(String.class);
            JMXResponse response = mapper.readValue(json, JMXResponse.class);

            if (response.getBeans() != null && !response.getBeans().isEmpty()) {
                NameNodeStatus status = new NameNodeStatus().parse(response.getBeans().get(0));
                DefaultLogger.__LOG.info(
                        String.format("[%s] Received NN state [State=%s][Transition Time=%d]",
                                status.getHost(), status.getState(), status.getLastHATransitionTime()));
                return status;
            }
            return null;
        } catch (Exception ex) {
            throw new NameNodeError(ex);
        }
    }
}
