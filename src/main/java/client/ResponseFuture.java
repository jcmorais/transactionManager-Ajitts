package client;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import messages.MessageEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Created by carlosmorais on 27/04/2017.
 */
public class ResponseFuture implements Future<MessageEvent> {

    private volatile State state = State.WAITING;
    ArrayBlockingQueue<MessageEvent> blockingResponse = new ArrayBlockingQueue<MessageEvent>(1);

    private enum State {
        WAITING,
        DONE
    }


    @Override
    public boolean isSuccess() {
        return false;
    }

    @Override
    public boolean isCancellable() {
        return false;
    }

    @Override
    public Throwable cause() {
        return null;
    }

    @Override
    public Future<MessageEvent> addListener(GenericFutureListener<? extends Future<? super MessageEvent>> genericFutureListener) {
        return null;
    }

    @Override
    public Future<MessageEvent> addListeners(GenericFutureListener<? extends Future<? super MessageEvent>>[] genericFutureListeners) {
        return null;
    }

    @Override
    public Future<MessageEvent> removeListener(GenericFutureListener<? extends Future<? super MessageEvent>> genericFutureListener) {
        return null;
    }

    @Override
    public Future<MessageEvent> removeListeners(GenericFutureListener<? extends Future<? super MessageEvent>>[] genericFutureListeners) {
        return null;
    }

    @Override
    public Future<MessageEvent> sync() throws InterruptedException {
        return null;
    }

    @Override
    public Future<MessageEvent> syncUninterruptibly() {
        return null;
    }

    @Override
    public Future<MessageEvent> await() throws InterruptedException {
        return null;
    }

    @Override
    public Future<MessageEvent> awaitUninterruptibly() {
        return null;
    }

    @Override
    public boolean await(long l, TimeUnit timeUnit) throws InterruptedException {
        return false;
    }

    @Override
    public boolean await(long l) throws InterruptedException {
        return false;
    }

    @Override
    public boolean awaitUninterruptibly(long l, TimeUnit timeUnit) {
        return false;
    }

    @Override
    public boolean awaitUninterruptibly(long l) {
        return false;
    }

    @Override
    public MessageEvent getNow() {
        return null;
    }

    @Override
    public boolean cancel(boolean b) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return state == State.DONE;
    }

    @Override
    public MessageEvent get() throws InterruptedException, ExecutionException {
        return blockingResponse.take();
    }

    @Override
    public MessageEvent get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        final MessageEvent responseAfterWait = blockingResponse.poll(timeout, unit);
        if (responseAfterWait == null) {
            throw new TimeoutException();
        }
        return responseAfterWait;
    }

    public void set(MessageEvent msg) {
        if (state == State.DONE) {
            return;
        }

        try {
            blockingResponse.put(msg);
            state = State.DONE;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
