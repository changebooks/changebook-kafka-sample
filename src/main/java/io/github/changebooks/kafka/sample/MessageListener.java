package io.github.changebooks.kafka.sample;

import io.github.changebooks.kafka.KafkaBatchConsumer;
import io.github.changebooks.kafka.KafkaBatchContext;
import io.github.changebooks.kafka.KafkaBatchContextImpl;
import io.github.changebooks.kafka.KafkaBatchListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.lang.Nullable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author changebooks@qq.com
 */
@Service
public class MessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageListener.class);

    /**
     * 执行线程数
     */
    private static final int THREAD_NUM = 3;

    /**
     * 消费接口
     */
    private final KafkaBatchConsumer batchConsumer = this::onConsume;

    /**
     * 标准线程池
     */
    private final KafkaBatchExecutor batchExecutor = new KafkaBatchExecutor(THREAD_NUM, batchConsumer);

    @KafkaListener(topics = Application.TOPIC)
    public void onListen(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        List<String> values = records.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        LOGGER.info("onListen, thread.id: {}, records.size: {}, values: {}", Thread.currentThread().getId(), records.size(), values);

        KafkaBatchListener batchListener = batchExecutor.getBatchListener();
        KafkaBatchContext batchContext = new KafkaBatchContextImpl();

        setThreadNum(4);

        if (batchListener.listen(records, batchContext)) {
            ack.acknowledge();
        } else {
            // retry use context
        }
    }

    /**
     * 执行消费
     *
     * @param records 消息列表
     * @param context 消费上下文
     * @return 消费成功提交消息？否则，消费失败等待重试，或抛出异常等待重试
     */
    public boolean onConsume(List<ConsumerRecord<String, String>> records, @Nullable final KafkaBatchContext context) {
        List<String> values = records.stream().map(ConsumerRecord::value).collect(Collectors.toList());
        LOGGER.info("onConsume, thread.id: {}, records.size: {}, values: {}", Thread.currentThread().getId(), records.size(), values);

        return true;
    }

    /**
     * 设置执行线程数
     * 通过执行线程数，重算线程池线程数
     *
     * @param threadNum 执行线程数
     */
    public void setThreadNum(int threadNum) {
        LOGGER.info("setThreadNum before, threadNum: {}, corePoolSize: {}, maximumPoolSize: {}",
                batchExecutor.getBatchListener().getThreadNum(),
                batchExecutor.getThreadPoolExecutor().getCorePoolSize(),
                batchExecutor.getThreadPoolExecutor().getMaximumPoolSize());

        this.batchExecutor.setThreadNum(threadNum);

        LOGGER.info("setThreadNum after, threadNum: {}, corePoolSize: {}, maximumPoolSize: {}",
                batchExecutor.getBatchListener().getThreadNum(),
                batchExecutor.getThreadPoolExecutor().getCorePoolSize(),
                batchExecutor.getThreadPoolExecutor().getMaximumPoolSize());
    }

}
