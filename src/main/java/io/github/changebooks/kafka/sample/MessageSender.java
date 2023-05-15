package io.github.changebooks.kafka.sample;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author changebooks@qq.com
 */
@Service
public class MessageSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(MessageSender.class);

    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * 批量发送消息
     *
     * @param data 消息内容
     */
    public void send(List<String> data) throws ExecutionException, InterruptedException {
        List<ListenableFuture<SendResult<String, String>>> futures = new ArrayList<>();
        for (String value : data) {
            ListenableFuture<SendResult<String, String>> f = kafkaTemplate.send(Application.TOPIC, value);
            futures.add(f);
        }

        List<String> result = new ArrayList<>();
        for (ListenableFuture<SendResult<String, String>> f : futures) {
            result.add(f.get().getProducerRecord().value());
        }

        LOGGER.info("send trace, thread: {}, result: {}", Thread.currentThread().getId(), result);
    }

}
