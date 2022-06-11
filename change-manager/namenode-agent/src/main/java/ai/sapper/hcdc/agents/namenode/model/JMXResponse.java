package ai.sapper.hcdc.agents.namenode.model;

import lombok.Getter;
import lombok.Setter;

import java.util.List;
import java.util.Map;

@Getter
@Setter
public class JMXResponse {
    private List<Map<String, String>> beans;
}
