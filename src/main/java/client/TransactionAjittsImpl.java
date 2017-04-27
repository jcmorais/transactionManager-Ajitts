package client;

/**
 * Created by carlosmorais on 24/04/2017.
 */
public class TransactionAjittsImpl implements TransactionAjitts {

    private long beginTS;
    private long commitTS;

    public TransactionAjittsImpl(long beginTS, long commitTS) {
        this.beginTS = beginTS;
        this.commitTS = commitTS;
    }

    public long getBeginTS() {
        return beginTS;
    }

    public void setBeginTS(long beginTS) {
        this.beginTS = beginTS;
    }

    public long getCommitTS() {
        return commitTS;
    }

    public void setCommitTS(long commitTS) {
        this.commitTS = commitTS;
    }

    @Override
    public String toString() {
        return "TransactionAjittsImpl{" +
                "beginTS=" + beginTS +
                ", commitTS=" + commitTS +
                '}';
    }
}
