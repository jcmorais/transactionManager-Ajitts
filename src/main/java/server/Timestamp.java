package server;

/**
 * Created by carlosmorais on 12/04/2017.
 */
public interface Timestamp {
    long nextCommitTS();
    long getStartTS();
    void updateStartTS(long commitTS);
}
