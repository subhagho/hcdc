package ai.sapper.hcdc.services.namenode;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.namenode.main.EditLogRunner;
import ai.sapper.cdc.common.model.services.BasicResponse;
import ai.sapper.cdc.common.model.services.ConfigSource;
import ai.sapper.cdc.common.model.services.EResponseState;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.services.ServiceHelper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EditsLogReaderService {
    private static EditLogRunner processor;

    @RequestMapping(value = "/editslog/start", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<String>> start(@RequestBody ConfigSource config) {
        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    NameNodeEnv.ENameNEnvState state = NameNodeEnv.dispose();
                    DefaultLogger.LOG.warn(String.format("Edit Log Processor Shutdown...[state=%s]", state.name()));
                }
            });
            processor = new EditLogRunner();
            processor.setConfigfile(config.getPath());
            processor.setFileSource(config.getType());
            processor.init();
            processor.run();
            DefaultLogger.LOG.info(String.format("EditsLog processor started. [config=%s]", config.toString()));
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    NameNodeEnv.get().state().state().name()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error, t.getMessage()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/editslog/status", method = RequestMethod.GET)
    public ResponseEntity<BasicResponse<NameNodeEnv.ENameNEnvState>> state() {
        try {
            ServiceHelper.checkService(processor);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    NameNodeEnv.get().state().state()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    NameNodeEnv.ENameNEnvState.Error).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/editslog/stop", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<NameNodeEnv.ENameNEnvState>> stop() {
        try {
            ServiceHelper.checkService(processor);
            NameNodeEnv.dispose();
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    NameNodeEnv.get().state().state()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    NameNodeEnv.ENameNEnvState.Error).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
