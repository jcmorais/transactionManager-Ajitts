package client;

import messages.BeginReply;

/**
 * Created by carlosmorais on 12/04/2017.
 */
public class TransactionManagerAjittsImpl implements TransactionManagerAjitts {

    private TmClient tmClient;


    public TransactionManagerAjittsImpl() {
        this.tmClient = new TmClient();
    }

    @Override
    public TransactionAjitts begin() {
        BeginReply reply = tmClient.begin();
        TransactionAjitts t = new TransactionAjittsImpl(reply.getStartTimestamp(), reply.getCommitTimestamp());
        return t;
    }



    @Override
    public boolean commit(TransactionAjitts t) {
        tmClient.commit(t.getBeginTS());
        return true;
    }
}
