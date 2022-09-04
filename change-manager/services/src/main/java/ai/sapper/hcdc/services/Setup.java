package ai.sapper.hcdc.services;

import ai.sapper.cdc.common.ConfigReader;
import ai.sapper.cdc.common.model.services.BasicResponse;
import ai.sapper.cdc.common.model.services.ConfigSource;
import ai.sapper.cdc.common.model.services.EResponseState;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.connections.Connection;
import ai.sapper.cdc.core.connections.ConnectionManager;
import ai.sapper.cdc.core.connections.ConnectionOpRequest;
import ai.sapper.cdc.core.connections.settngs.ConnectionSettings;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
public class Setup {
    private static final String NAME = "SETUP";


    @RequestMapping(value = "/admin/setup/start", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<NameNodeEnv.NameNEnvState>> start(@RequestBody ConfigSource config) {
        try {
            try {
                HierarchicalConfiguration<ImmutableNode> xmlConfig = ConfigReader.read(config.getPath(), config.getType());
                NameNodeEnv env = NameNodeEnv.setup(NAME, getClass(), xmlConfig);
                return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                        env.state()),
                        HttpStatus.OK);
            } catch (Throwable t) {
                DefaultLogger.LOGGER.error("Error starting service.", t);
                DefaultLogger.stacktrace(t);
                return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                        NameNodeEnv.get(NAME).state()).withError(t),
                        HttpStatus.INTERNAL_SERVER_ERROR);
            }
        } catch (Exception ex) {
            DefaultLogger.LOGGER.error("Error starting service.", ex);
            DefaultLogger.stacktrace(ex);
            throw new RuntimeException(ex);
        }
    }

    @RequestMapping(value = "/admin/connections/add", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<ConnectionSettings>> addConnection(@RequestBody ConnectionSettings settings) {
        try {
            ConnectionManager connectionManager = NameNodeEnv.get(NAME).connectionManager();
            connectionManager.create(settings.getConnectionClass(), settings);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    settings),
                    HttpStatus.OK);
        } catch (Exception ex) {
            DefaultLogger.LOGGER.error("Error starting service.", ex);
            DefaultLogger.stacktrace(ex);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    (ConnectionSettings) null).withError(ex),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/connections/update", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<ConnectionSettings>> updateConnection(@RequestBody ConnectionSettings settings) {
        try {
            ConnectionManager connectionManager = NameNodeEnv.get(NAME).connectionManager();
            connectionManager.create(settings.getConnectionClass(), settings);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    settings),
                    HttpStatus.OK);
        } catch (Exception ex) {
            DefaultLogger.LOGGER.error("Error starting service.", ex);
            DefaultLogger.stacktrace(ex);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    (ConnectionSettings) null).withError(ex),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/connections/remove/{name}", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<Boolean>> removeConnection(@PathVariable("name") String name,
                                                                   @RequestBody ConnectionOpRequest request) {
        try {
            ConnectionManager connectionManager = NameNodeEnv.get(NAME).connectionManager();
            boolean ret = connectionManager.remove(request.getConnectionClass(), name);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    ret),
                    HttpStatus.OK);
        } catch (Exception ex) {
            DefaultLogger.LOGGER.error("Error starting service.", ex);
            DefaultLogger.stacktrace(ex);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    false).withError(ex),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/connections/get/{name}", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<ConnectionSettings>> getConnection(@PathVariable("name") String name,
                                                                           @RequestBody ConnectionOpRequest request) {
        try {
            ConnectionManager connectionManager = NameNodeEnv.get(NAME).connectionManager();
            Connection connection = null;
            ConnectionSettings settings = null;
            if (request != null && request.getConnectionClass() != null) {
                connection = connectionManager.getConnection(name, request.getConnectionClass());
            } else {
                connection = connectionManager.getConnection(name);
            }
            if (connection != null) {
                settings = connection.settings();
            }
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    settings),
                    HttpStatus.OK);
        } catch (Exception ex) {
            DefaultLogger.LOGGER.error("Error starting service.", ex);
            DefaultLogger.stacktrace(ex);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    (ConnectionSettings) null).withError(ex),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/connections/list/{type}", method = RequestMethod.GET)
    public ResponseEntity<BasicResponse<Map<String, ConnectionSettings>>> listConnections(
            @PathVariable("type") String type) {
        try {
            ConnectionManager connectionManager = NameNodeEnv.get(NAME).connectionManager();
            Map<String, ConnectionSettings> settings = connectionManager.list(type);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    settings),
                    HttpStatus.OK);
        } catch (Exception ex) {
            DefaultLogger.LOGGER.error("Error starting service.", ex);
            DefaultLogger.stacktrace(ex);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    (Map<String, ConnectionSettings>) null).withError(ex),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
