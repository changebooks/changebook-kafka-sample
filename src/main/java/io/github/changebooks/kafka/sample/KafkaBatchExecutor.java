package io.github.changebooks.kafka.sample;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.github.changebooks.kafka.KafkaBatchConsumer;
import io.github.changebooks.kafka.KafkaBatchListener;
import org.springframework.util.Assert;

import java.util.concurrent.*;

/**
 * 标准线程池
 *
 * @author changebooks@qq.com
 */
public class KafkaBatchExecutor {
    /**
     * 名称
     */
    private static final String NAME_FORMAT = "KafkaBatchExecutor-%d";

    /**
     * 多余的空闲线程生存时间
     */
    private static final long KEEP_ALIVE_TIME = 1L;

    /**
     * 多余空闲线程生存时间的单位
     */
    private static final TimeUnit KEEP_ALIVE_TIME_UNIT = TimeUnit.MILLISECONDS;

    /**
     * 阻塞队列
     */
    private static final BlockingQueue<Runnable> WORK_QUEUE = new SynchronousQueue<>();

    /**
     * 线程工厂
     */
    private static final ThreadFactory THREAD_FACTORY = new ThreadFactoryBuilder().setNameFormat(NAME_FORMAT).build();

    /**
     * 崩溃处理
     */
    private static final ThreadPoolExecutor.AbortPolicy ABORT_POLICY = new ThreadPoolExecutor.AbortPolicy();

    /**
     * 线程池
     */
    private final ThreadPoolExecutor threadPoolExecutor;

    /**
     * 消息分页，并行消费
     */
    private final KafkaBatchListener batchListener;

    public KafkaBatchExecutor(int threadNum, KafkaBatchConsumer consumer) {
        this.threadPoolExecutor = newThreadPoolExecutor(threadNum);
        this.batchListener = new KafkaBatchListener(threadPoolExecutor, consumer).setThreadNum(threadNum);
    }

    public ThreadPoolExecutor getThreadPoolExecutor() {
        return threadPoolExecutor;
    }

    public KafkaBatchListener getBatchListener() {
        return batchListener;
    }

    /**
     * 新建线程池
     *
     * @param threadNum 执行线程数
     * @return {@link ThreadPoolExecutor} 实例
     */
    public ThreadPoolExecutor newThreadPoolExecutor(int threadNum) {
        checkThreadNum(threadNum);

        int corePoolSize = getCorePoolSize(threadNum);
        int maximumPoolSize = getMaximumPoolSize(threadNum);

        checkPoolSize(corePoolSize, maximumPoolSize);

        return new ThreadPoolExecutor(
                corePoolSize,
                maximumPoolSize,
                KEEP_ALIVE_TIME,
                KEEP_ALIVE_TIME_UNIT,
                WORK_QUEUE,
                THREAD_FACTORY,
                ABORT_POLICY
        );
    }

    /**
     * 设置执行线程数
     * 通过执行线程数，重算线程池线程数
     *
     * @param threadNum 执行线程数
     */
    public void setThreadNum(int threadNum) {
        checkThreadNum(threadNum);

        int oldThreadNum = batchListener.getThreadNum();
        if (oldThreadNum != threadNum) {
            batchListener.setThreadNum(threadNum);

            int corePoolSize = getCorePoolSize(threadNum);
            int maximumPoolSize = getMaximumPoolSize(threadNum);
            setPoolSize(corePoolSize, maximumPoolSize);
        }
    }

    /**
     * 设置线程池线程数
     *
     * @param corePoolSize    核心线程数
     * @param maximumPoolSize 最大线程数
     */
    public void setPoolSize(int corePoolSize, int maximumPoolSize) {
        checkPoolSize(corePoolSize, maximumPoolSize);

        if (threadPoolExecutor.getCorePoolSize() != corePoolSize) {
            threadPoolExecutor.setCorePoolSize(corePoolSize);
        }

        if (threadPoolExecutor.getMaximumPoolSize() != maximumPoolSize) {
            threadPoolExecutor.setMaximumPoolSize(maximumPoolSize);
        }
    }

    /**
     * 通过执行线程数，获取线程池的核心线程数
     *
     * @param threadNum 执行线程数
     * @return 核心线程数
     */
    public int getCorePoolSize(int threadNum) {
        return threadNum + 1;
    }

    /**
     * 通过执行线程数，获取线程池的最大线程数
     *
     * @param threadNum 执行线程数
     * @return 最大线程数
     */
    public int getMaximumPoolSize(int threadNum) {
        return threadNum * 2 + 1;
    }

    /**
     * 校验执行线程数
     *
     * @param threadNum 执行线程数
     */
    public void checkThreadNum(int threadNum) {
        Assert.isTrue(threadNum > 0, "threadNum must be greater than 0");
    }

    /**
     * 校验线程池线程数
     *
     * @param corePoolSize    核心线程数，核心线程数 < 最大线程数
     * @param maximumPoolSize 最大线程数，最大线程数 > 核心线程数
     */
    public void checkPoolSize(int corePoolSize, int maximumPoolSize) {
        Assert.isTrue(corePoolSize > 0, "corePoolSize must be greater than 0");
        Assert.isTrue(maximumPoolSize > 0, "maximumPoolSize must be greater than 0");
        Assert.isTrue(corePoolSize < maximumPoolSize, "corePoolSize must be less than maximumPoolSize");
    }

}
