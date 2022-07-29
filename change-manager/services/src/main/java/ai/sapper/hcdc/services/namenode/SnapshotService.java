package ai.sapper.hcdc.services.namenode;

import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.namenode.main.SnapshotRunner;
import ai.sapper.hcdc.agents.namenode.model.DFSFileReplicaState;
import ai.sapper.hcdc.common.filters.DomainFilter;
import ai.sapper.hcdc.common.filters.DomainFilters;
import ai.sapper.hcdc.common.filters.Filter;
import ai.sapper.hcdc.common.model.services.BasicResponse;
import ai.sapper.hcdc.common.model.services.ConfigSource;
import ai.sapper.hcdc.common.model.services.EResponseState;
import ai.sapper.hcdc.common.model.services.SnapshotDoneRequest;
import ai.sapper.hcdc.common.utils.DefaultLogger;
import ai.sapper.hcdc.services.ServiceHelper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
public class SnapshotService {
    private static SnapshotRunner processor;

    @RequestMapping(value = "/snapshot/start", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<String>> start(@RequestBody ConfigSource config) {
        try {
            processor = new SnapshotRunner();
            processor.setConfigfile(config.getPath());
            processor.setFileSource(config.getType());
            processor.init();
            DefaultLogger.LOG.info(String.format("EditsLog processor started. [config=%s]", config.toString()));
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    NameNodeEnv.get().state().state().name()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error, t.getMessage()).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/add/{domain}", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<DomainFilters>> addFilter(@PathVariable("domain") String domain,
                                                                  @RequestBody Filter filter) {
        try {
            ServiceHelper.checkService(processor);
            DomainFilters filters = processor.getProcessor().addFilter(domain, filter);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    filters),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error, (DomainFilters) null).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/add/{domain}/batch", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<DomainFilters>> addFilter(@PathVariable("domain") String domain,
                                                                  @RequestBody Map<String, List<Filter>> filters) {
        try {
            ServiceHelper.checkService(processor);
            DomainFilters dfs = null;
            for (String entity : filters.keySet()) {
                List<Filter> fs = filters.get(entity);
                for (Filter filter : fs) {
                    dfs = processor.getProcessor().addFilter(domain, filter);
                }
            }
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    dfs),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error, (DomainFilters) null).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/remove/{domain}", method = RequestMethod.DELETE)
    public ResponseEntity<BasicResponse<Filter>> removeFilter(@PathVariable("domain") String domain,
                                                              @RequestBody Filter filter) {
        try {
            ServiceHelper.checkService(processor);
            Filter f = processor.getProcessor().removeFilter(domain, filter);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    f),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error, (Filter) null).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/remove/{domain}/{entity}", method = RequestMethod.DELETE)
    public ResponseEntity<BasicResponse<DomainFilter>> removeFilter(@PathVariable("domain") String domain,
                                                                    @PathVariable("entity") String entity) {
        try {
            ServiceHelper.checkService(processor);
            DomainFilter f = processor.getProcessor().removeFilter(domain, entity);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    f),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error, (DomainFilter) null).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/remove/{domain}/{entity}/{path}", method = RequestMethod.DELETE)
    public ResponseEntity<BasicResponse<List<Filter>>> removeFilter(@PathVariable("domain") String domain,
                                                                    @PathVariable("entity") String entity,
                                                                    @PathVariable("path") String path) {
        try {
            ServiceHelper.checkService(processor);
            List<Filter> f = processor.getProcessor().removeFilter(domain, entity, path);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    f),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error, (List<Filter>) null).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/run")
    public ResponseEntity<BasicResponse<Integer>> run() {
        try {
            ServiceHelper.checkService(processor);
            int count = processor.getProcessor().run();
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    count),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    -1).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/done", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<DFSFileReplicaState>> snapshotDone(@RequestBody SnapshotDoneRequest request) {
        try {
            ServiceHelper.checkService(processor);
            DFSFileReplicaState rState = processor.getProcessor()
                    .snapshotDone(request.getHdfsPath(),
                            request.getEntity(),
                            request.getTransactionId());
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    rState),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    (DFSFileReplicaState) null).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/status")
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

    @RequestMapping(value = "/snapshot/stop")
    public ResponseEntity<BasicResponse<NameNodeEnv.ENameNEnvState>> stop() {
        try {
            ServiceHelper.checkService(processor);
            NameNodeEnv.dispose();
            processor = null;
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
