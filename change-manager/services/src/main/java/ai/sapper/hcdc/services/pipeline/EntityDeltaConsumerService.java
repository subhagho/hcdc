package ai.sapper.hcdc.services.pipeline;

import ai.sapper.cdc.common.model.services.BasicResponse;
import ai.sapper.cdc.common.model.services.ConfigSource;
import ai.sapper.cdc.common.model.services.EResponseState;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.main.EntityChangeDeltaConsumer;
import ai.sapper.hcdc.services.ServiceHelper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class EntityDeltaConsumerService {
    private EntityChangeDeltaConsumer processor;

    @RequestMapping(value = "/entity/consumer/start", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<NameNodeEnv.NameNodeEnvState>> start(@RequestBody ConfigSource config) {
        try {
            processor = new EntityChangeDeltaConsumer();
            processor.setConfigFile(config.getPath())
                    .setConfigSource(config.getType().name());

            processor.init();
            processor.start();
            DefaultLogger.info(processor.getEnv().LOG,
                    String.format("Edits Delta processor started. [config=%s]", config.toString()));
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    processor.status()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error, processor.status()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/entity/consumer/status", method = RequestMethod.GET)
    public ResponseEntity<BasicResponse<NameNodeEnv.NameNodeEnvState>> state() {
        try {
            ServiceHelper.checkService(processor.name(), processor);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    processor.status()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    processor.status()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/entity/consumer/stop", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<NameNodeEnv.NameNodeEnvState>> stop() {
        try {
            ServiceHelper.checkService(processor.name(), processor);
            processor.stop();
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    processor.status()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    processor.status()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
