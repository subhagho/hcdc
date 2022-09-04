package ai.sapper.cdc.core.connections.settngs;

import ai.sapper.cdc.core.connections.WebServiceConnection;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY,
        property = "@class")
public class WebServiceConnectionSettings extends ConnectionSettings{
    private String endpoint;

    public WebServiceConnectionSettings() {
        setConnectionClass(WebServiceConnection.class);
        setType(EConnectionType.rest);
    }
}
