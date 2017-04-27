package server;

/**
 * Created by carlosmorais on 12/04/2017.
 */
public interface Timestamp {
    long next();
    long nextCommitTS();
    long nextBeginTS();

    long getBeginTimestamp();
    long getCommitTimestamp();
}
