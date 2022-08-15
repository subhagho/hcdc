package ai.sapper.hcdc.agents.common;

import ai.sapper.cdc.common.utils.DefaultLogger;
import com.google.common.base.Preconditions;
import lombok.NonNull;

public class HeartbeatThread implements Runnable {
    private long sleepInterval = 60 * 1000; // 60 secs.
    private ZkStateManager stateManager;

    public HeartbeatThread withStateManager(@NonNull ZkStateManager stateManager) {
        this.stateManager = stateManager;
        return this;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        Preconditions.checkNotNull(stateManager);
        try {
            while (NameNodeEnv.get().state().isAvailable()) {
                stateManager.heartbeat(stateManager.moduleInstance().getInstanceId(),
                        NameNodeEnv.get().agentState());
                Thread.sleep(sleepInterval);
            }
        } catch (Exception ex) {
            DefaultLogger.LOG.error(
                    String.format("Heartbeat thread terminated. [module=%s]", stateManager.module()), ex);
        }
    }
}
