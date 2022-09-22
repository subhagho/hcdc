package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.core.connections.ConnectionConfig;
import ai.sapper.cdc.core.connections.WebServiceConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class WebServiceConnectionSettings extends ConnectionSettings {
    @Setting(name = WebServiceConnection.WebServiceConnectionConfig.Constants.CONFIG_URL)
    private String endpoint;

    public WebServiceConnectionSettings() {
        setConnectionClass(WebServiceConnection.class);
        setType(EConnectionType.rest);
    }

    @Override
    public void validate() throws Exception {
        ConfigReader.checkStringValue(getName(), getClass(), ConnectionConfig.CONFIG_NAME);
        ConfigReader.checkStringValue(getEndpoint(), getClass(),
                WebServiceConnection.WebServiceConnectionConfig.Constants.CONFIG_URL);
    }
}
