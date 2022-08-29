package ai.sapper.cdc.core.model;

import lombok.Getter;
import lombok.Setter;

import java.net.InetAddress;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

@Getter
@Setter
public class ModuleInstance {
    private String source;
    private String module;
    private String name;
    private String ip;
    private String startTime;
    private String instanceId;

    public ModuleInstance withStartTime(long startTime) {
        Date date = new Date(startTime);
        DateFormat df = new SimpleDateFormat("yyyyMMdd:HH:mm");
        this.startTime = df.format(date);
        return this;
    }

    public ModuleInstance withIp(InetAddress address) {
        if (address != null) {
            ip = address.toString();
        }
        return this;
    }

    public String id() {
        return String.format("%s/%s/%s", source, module, name);
    }
}
