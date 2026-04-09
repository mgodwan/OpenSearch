package org.opensearch.index.engine;

import org.apache.lucene.search.ReferenceManager;
import org.opensearch.index.seqno.LocalCheckpointTracker;

import java.util.concurrent.atomic.AtomicLong;

public class LastRefreshedCheckpointListener implements ReferenceManager.RefreshListener {
    private final AtomicLong refreshedCheckpoint;
    private final  AtomicLong pendingCheckpoint;
    private final LocalCheckpointTracker localCheckpointTracker;

    LastRefreshedCheckpointListener(LocalCheckpointTracker localCheckpointTracker) {
        this.refreshedCheckpoint = new AtomicLong(localCheckpointTracker.getProcessedCheckpoint());
        this.pendingCheckpoint = new AtomicLong(localCheckpointTracker.getProcessedCheckpoint());
        this.localCheckpointTracker = localCheckpointTracker;
    }

    @Override
    public void beforeRefresh() {
        // all changes until this point should be visible after refresh
        pendingCheckpoint.updateAndGet(curr -> Math.max(curr, localCheckpointTracker.getProcessedCheckpoint()));
    }

    @Override
    public void afterRefresh(boolean didRefresh) {
        if (didRefresh) {
            updateRefreshedCheckpoint(pendingCheckpoint.get());
        }
    }

    void updateRefreshedCheckpoint(long checkpoint) {
        refreshedCheckpoint.updateAndGet(curr -> Math.max(curr, checkpoint));
        assert refreshedCheckpoint.get() >= checkpoint : refreshedCheckpoint.get() + " < " + checkpoint;
        // This shouldn't be required ideally, but we're also invoking this method from refresh as of now.
        // This change is added as safety check to ensure that our checkpoint values are consistent at all times.
        pendingCheckpoint.updateAndGet(curr -> Math.max(curr, checkpoint));

    }

    long lastRefreshedCheckpoint() {
        return refreshedCheckpoint.get();
    }

    long pendingCheckpoint() {
        return pendingCheckpoint.get();
    }
}
