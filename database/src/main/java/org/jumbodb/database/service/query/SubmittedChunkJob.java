package org.jumbodb.database.service.query;

import org.jumbodb.database.service.query.definition.DeliveryChunkDefinition;

import java.util.concurrent.Future;

/**
 * @author Carsten Hufe
 */
public class SubmittedChunkJob {
    private DeliveryChunkDefinition deliveryChunkDefinition;
    private Future<Integer> future;

    public SubmittedChunkJob(DeliveryChunkDefinition deliveryChunkDefinition, Future<Integer> future) {
        this.deliveryChunkDefinition = deliveryChunkDefinition;
        this.future = future;
    }

    public DeliveryChunkDefinition getDeliveryChunkDefinition() {
        return deliveryChunkDefinition;
    }

    public Future<Integer> getFuture() {
        return future;
    }
}
