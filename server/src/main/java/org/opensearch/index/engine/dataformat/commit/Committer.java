package org.opensearch.index.engine.dataformat.commit;

import java.util.Map;

public interface Committer {

    Map<String, String> readLastCommittedUserData();

    long getLastCommittedGeneration();
}
