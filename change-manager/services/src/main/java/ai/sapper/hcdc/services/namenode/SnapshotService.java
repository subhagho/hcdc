package ai.sapper.hcdc.services.namenode;

import ai.sapper.cdc.common.model.services.*;
import ai.sapper.hcdc.agents.common.NameNodeEnv;
import ai.sapper.hcdc.agents.main.SnapshotRunner;
import ai.sapper.hcdc.agents.model.DFSFileReplicaState;
import ai.sapper.cdc.common.filters.DomainFilter;
import ai.sapper.cdc.common.filters.DomainFilters;
import ai.sapper.cdc.common.filters.Filter;
import ai.sapper.cdc.common.utils.DefaultLogger;
import ai.sapper.cdc.core.schema.SchemaManager;
import ai.sapper.hcdc.services.ServiceHelper;
import com.google.common.base.Strings;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
public class SnapshotService {
    private static SnapshotRunner processor;

    @RequestMapping(value = "/snapshot/filters/add/{domain}", method = RequestMethod.PUT)
    public ResponseEntity<DomainFilters> addFilter(@PathVariable("domain") String domain,
                                                   @RequestBody Filter filter) {
        try {
            ServiceHelper.checkService(processor);
            DomainFilters filters = processor.getProcessor().addFilter(domain, filter);
            return new ResponseEntity<>(filters,
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((DomainFilters) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/add/{domain}/batch", method = RequestMethod.PUT)
    public ResponseEntity<DomainFilters> addFilter(@PathVariable("domain") String domain,
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
            return new ResponseEntity<>(dfs, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((DomainFilters) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/remove/{domain}", method = RequestMethod.DELETE)
    public ResponseEntity<Filter> removeFilter(@PathVariable("domain") String domain,
                                               @RequestBody Filter filter) {
        try {
            ServiceHelper.checkService(processor);
            Filter f = processor.getProcessor().removeFilter(domain, filter);
            return new ResponseEntity<>(f, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((Filter) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/remove/{domain}/{entity}", method = RequestMethod.DELETE)
    public ResponseEntity<DomainFilter> removeFilter(@PathVariable("domain") String domain,
                                                     @PathVariable("entity") String entity) {
        try {
            ServiceHelper.checkService(processor);
            DomainFilter f = processor.getProcessor().removeFilter(domain, entity);
            return new ResponseEntity<>(f, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((DomainFilter) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/filters/remove/{domain}/{entity}/{path}", method = RequestMethod.DELETE)
    public ResponseEntity<List<Filter>> removeFilter(@PathVariable("domain") String domain,
                                                     @PathVariable("entity") String entity,
                                                     @PathVariable("path") String path) {
        try {
            ServiceHelper.checkService(processor);
            List<Filter> f = processor.getProcessor().removeFilter(domain, entity, path);
            return new ResponseEntity<>(f, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((List<Filter>) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/run", method = RequestMethod.POST)
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
    public ResponseEntity<DFSFileReplicaState> snapshotDone(@RequestBody SnapshotDoneRequest request) {
        try {
            ServiceHelper.checkService(processor);
            DFSFileReplicaState rState = processor.getProcessor()
                    .snapshotDone(request.getHdfsPath(),
                            request.getEntity(),
                            request.getTransactionId());
            return new ResponseEntity<>(rState, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((DFSFileReplicaState) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/snapshot/status", method = RequestMethod.GET)
    public ResponseEntity<BasicResponse<NameNodeEnv.NameNEnvState>> state() {
        try {
            ServiceHelper.checkService(processor);
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    NameNodeEnv.get().state()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    (NameNodeEnv.NameNEnvState) null).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/snapshot/start", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<NameNodeEnv.NameNEnvState>> start(@RequestBody ConfigSource config) {
        try {
            processor = new SnapshotRunner();
            processor.setConfigfile(config.getPath());
            processor.setFileSource(config.getType());
            processor.init();
            DefaultLogger.LOG.info(String.format("EditsLog processor started. [config=%s]", config.toString()));
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    NameNodeEnv.get().state()),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(
                    new BasicResponse<>(EResponseState.Error,
                            (NameNodeEnv.NameNEnvState) null).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/admin/snapshot/stop", method = RequestMethod.POST)
    public ResponseEntity<BasicResponse<NameNodeEnv.ENameNEnvState>> stop() {
        try {
            ServiceHelper.checkService(processor);
            NameNodeEnv.dispose();
            processor = null;
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Success,
                    NameNodeEnv.ENameNEnvState.Disposed),
                    HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>(new BasicResponse<>(EResponseState.Error,
                    NameNodeEnv.ENameNEnvState.Error).withError(t),
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/schema/expand/domain/{domain}", method = RequestMethod.GET)
    public ResponseEntity<List<PathOrSchema>> expandDomain(@PathVariable("domain") String domain) {
        try {
            SchemaManager schemaManager = NameNodeEnv.get().schemaManager();
            if (schemaManager == null) {
                throw new Exception("SchemaManager not initialized...");
            }
            if (Strings.isNullOrEmpty(domain)
                    || domain.compareToIgnoreCase("null") == 0) {
                domain = null;
            }
            List<PathOrSchema> paths = schemaManager.domainNodes(domain);
            return new ResponseEntity<>(paths, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((List<PathOrSchema>) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @RequestMapping(value = "/schema/expand/path/{domain}", method = RequestMethod.GET)
    public ResponseEntity<List<PathOrSchema>> expandPath(@PathVariable("domain") String domain,
                                                         @RequestBody String path) {
        try {
            SchemaManager schemaManager = NameNodeEnv.get().schemaManager();
            if (schemaManager == null) {
                throw new Exception("SchemaManager not initialized...");
            }
            List<PathOrSchema> paths = schemaManager.pathNodes(domain, path);
            return new ResponseEntity<>(paths, HttpStatus.OK);
        } catch (Throwable t) {
            return new ResponseEntity<>((List<PathOrSchema>) null,
                    HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
}
