/*
 * Copyright(C) (2023) Sapper Inc. (open.source at zyient dot io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ai.sapper.cdc.core;

import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.WebServiceConnection;
import ai.sapper.cdc.core.model.JMXResponse;
import ai.sapper.cdc.core.model.NameNodeStatus;
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
            DefaultLogger.debug(String.format("NameNode Status URL: [%s]", up));
            Map<String, String> query = new HashMap<>(1);
            query.put(Constants.PATH_NN_QUERY_KEY, Constants.PATH_NN_QUERY);

            String json = client.getUrl(Constants.PATH_NN_PATH, String.class, query, MediaType.APPLICATION_JSON);
            JMXResponse response = mapper.readValue(json, JMXResponse.class);
            Map<String, String> bean = response.findBeanByName(Constants.REGEX_NAME);
            if (bean != null) {
                NameNodeStatus status = new NameNodeStatus().parse(bean);
                DefaultLogger.info(
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
