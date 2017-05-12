package test;

/**
 * Created by carlosmorais on 20/04/2017.
 */
public class Test {
    public static void main(String[] args) throws InterruptedException {
        /*


        HBaseTransactionManager tm = new HBaseTransactionManager();

        TransactionAjitts t = tm.begin();

        System.out.println("get tx context: "+t);

        tm.commit(t);


        System.exit(0);

        Queue<TransactionAjitts> queue = new LinkedList();

        BlockingDeque<TransactionAjitts> quee = new LinkedBlockingDeque<>();

        Runnable task =  () -> {
            System.out.println("Run start");
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            TransactionAjitts t = new TransactionAjitts(null, null);
            t.setCommitTS(1001);
            quee.add(t);
            System.out.println("Run end");
        };

        Thread thread = new Thread(task);
        thread.start();

        System.out.println("wait for element");
        TransactionAjitts ele =  quee.takeFirst();
        System.out.println("GET: "+ele.toString());


        TransactionAjitts t1 = new TransactionAjitts();
        t1.setCommitTS(1);

        TransactionAjitts t2 = new TransactionAjitts();
        t2.setCommitTS(2);

        TransactionAjitts t3 = new TransactionAjitts();
        t3.setCommitTS(3);

        TransactionAjitts t4 = new TransactionAjitts();
        t4.setCommitTS(4);


        queue.add(t1);
        queue.add(t2);
        queue.add(t3);
        queue.add(t4);


        System.out.println("Head: "+queue.element().toString());

        System.out.println("Remove: "+queue.remove().toString());

        System.out.println("Head: "+queue.element().toString());

        System.out.println("Remove: "+queue.remove().toString());


        System.out.println("Head: "+queue.element().toString());
        System.out.println("Remove: "+queue.remove().toString());
*/


    }
}
