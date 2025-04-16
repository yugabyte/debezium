package io.debezium.connector.postgresql.metrics;

import io.debezium.connector.base.ChangeEventQueueMetrics;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.PostgresPartition;
import io.debezium.data.Envelope;
import io.debezium.metrics.Metrics;
import io.debezium.pipeline.ConnectorEvent;
import io.debezium.pipeline.metrics.ChangeEventSourceMetrics;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.spi.schema.DataCollectionId;
import io.debezium.util.Collect;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

abstract class AbstractYugabyteDBTaskMetrics<B extends AbstractYugabyteDBPartitionMetrics> extends YugabyteDBMetrics
    implements ChangeEventSourceMetrics<PostgresPartition>, YugabyteDBTaskMetricsMXBean {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractYugabyteDBTaskMetrics.class);

    private final ChangeEventQueueMetrics changeEventQueueMetrics;
    private final Map<PostgresPartition, B> beans = new HashMap<>();
    private final Function<PostgresPartition, B> beanFactory;

    AbstractYugabyteDBTaskMetrics(CdcSourceTaskContext taskContext,
                                  String contextName,
                                  ChangeEventQueueMetrics changeEventQueueMetrics,
                                  Collection<PostgresPartition> partitions,
                                  Function<PostgresPartition, B> beanFactory) {
        super(taskContext, Collect.linkMapOf(
                "server", taskContext.getConnectorName(),
                "task", taskContext.getTaskId(),
                "context", contextName));
        this.changeEventQueueMetrics = changeEventQueueMetrics;

        this.beanFactory = beanFactory;

        for (PostgresPartition partition : partitions) {
            beans.put(partition, beanFactory.apply(partition));
        }
    }

    @Override
    public synchronized void register() {
        super.register();
        beans.values().forEach(YugabyteDBMetrics::register);
    }

    @Override
    public synchronized void unregister() {
        beans.values().forEach(YugabyteDBMetrics::unregister);
        super.unregister();
    }

    @Override
    public void reset() {
        beans.values().forEach(B::reset);
    }

    @Override
    public void onEvent(PostgresPartition partition, DataCollectionId source, OffsetContext offset, Object key,
                        Struct value, Envelope.Operation operation) {
        onPartitionEvent(partition, bean -> bean.onEvent(source, offset, key, value, operation));
    }

    @Override
    public void onFilteredEvent(PostgresPartition partition, String event) {
        onPartitionEvent(partition, bean -> bean.onFilteredEvent(event));
    }

    @Override
    public void onFilteredEvent(PostgresPartition partition, String event, Envelope.Operation operation) {
        onPartitionEvent(partition, bean -> bean.onFilteredEvent(event, operation));
    }

    @Override
    public void onErroneousEvent(PostgresPartition partition, String event) {
        onPartitionEvent(partition, bean -> bean.onErroneousEvent(event));
    }

    @Override
    public void onErroneousEvent(PostgresPartition partition, String event, Envelope.Operation operation) {
        onPartitionEvent(partition, bean -> bean.onErroneousEvent(event, operation));
    }

    @Override
    public void onConnectorEvent(PostgresPartition partition, ConnectorEvent event) {
        onPartitionEvent(partition, bean -> bean.onConnectorEvent(event));
    }

    @Override
    public int getQueueTotalCapacity() {
        return changeEventQueueMetrics.totalCapacity();
    }

    @Override
    public int getQueueRemainingCapacity() {
        return changeEventQueueMetrics.remainingCapacity();
    }

    @Override
    public long getMaxQueueSizeInBytes() {
        return changeEventQueueMetrics.maxQueueSizeInBytes();
    }

    @Override
    public long getCurrentQueueSizeInBytes() {
        return changeEventQueueMetrics.currentQueueSizeInBytes();
    }

    protected void onPartitionEvent(PostgresPartition partition, Consumer<B> handler) {
        B bean = beans.get(partition);
        if (bean == null) {
            LOGGER.info("MBean for partition {} are not registered, registering them now", partition);
            beans.put(partition, beanFactory.apply(partition));
            bean = beans.get(partition);
            bean.register();
        }
        handler.accept(bean);
    }
}
