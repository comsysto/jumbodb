package org.jumbodb.database.service.query;

import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public class FutureCancelableTask implements CancelableTask {
    private Future<?> future;

    public FutureCancelableTask(Future<?> future) {
        this.future = future;
    }

    @Override
    public void cancel() {
        future.cancel(true);
    }
}
