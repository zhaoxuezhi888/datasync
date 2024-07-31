package com.yonyougov.yondif.env;

import com.yonyougov.yondif.config.setting.ParameterToolService;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.PredefinedOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.TimeUnit;

public class EsEnv extends AbsEnv {
    private static final Logger logger = LogManager.getLogger(ParameterToolService.class);

    public EsEnv(ParameterTool parameterTool) {
        super(parameterTool);
    }

    @Override
    public StreamExecutionEnvironment createEnvironment() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置 Savepoint 路径
        EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend(false);
        //SPINNING_DISK_OPTIMIZED_HIGH_MEM 设置为机械硬盘+内存模式,FLASH_SSD_OPTIMIZED  (有条件使用ssd的可以使用这个选项)
        stateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
//        env.setStateBackend(new HashMapStateBackend());
        environment.setStateBackend(stateBackend);
        //在执行环境（或单个运算符）上使用来设置缓冲区填满的最大等待时间。在此时间之后，即使缓冲区未满，也会自动发送缓冲区。此超时的默认值为 100 毫秒,为了最大限度地提高吞吐量，设置setBufferTimeout(-1)将删除超时和缓冲区仅在它们已满时才被刷新。要最大限度地减少延迟，请将超时设置为接近 0 的值（例如 5 或 10 毫秒）。应避免缓冲区超时为 0，因为它会导致严重的性能下降
        environment.setBufferTimeout(100);

        //开启检查点，每3分钟启动一次检查点保存
        environment.enableCheckpointing(180 * 1000L);

        CheckpointConfig checkpointConfig = environment.getCheckpointConfig();
        // 设置至少一次模式
        checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);

        // 确保checkpoint之间至少有1s的时间间隔，即checkpoint的最小间隔，该属性定义在 checkpoint 之间需要多久的时间，以确保流应用在 checkpoint 之间有足够的进展。
        // 如果值设置为了 5000， 无论 checkpoint 持续时间与间隔是多久，在前一个 checkpoint 完成时的至少五秒后会才开始下一个 checkpoint。
        //往往使用“checkpoints 之间的最小时间”来配置应用会比 checkpoint 间隔容易很多，
        // 因为“checkpoints 之间的最小时间”在 checkpoint 的执行时间超过平均值时不会受到影响（例如如果目标的存储系统忽然变得很慢）。
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        // 超时时间10分钟，如果 checkpoint 执行的时间超过了该配置的阈值，还在进行中的 checkpoint 操作就会被抛弃
        checkpointConfig.setCheckpointTimeout(10 * 60 * 1000L);
        //同时只能有一个检查点,并发 checkpoint 的数目: 默认情况下，在上一个 checkpoint 未完成（失败或者成功）的情况下，系统不会触发另一个 checkpoint。
        // 这确保了拓扑不会在 checkpoint 上花费太多时间，从而影响正常的处理流程。
        // 不过允许多个 checkpoint 并行进行是可行的，对于有确定的处理延迟（例如某方法所调用比较耗时的外部服务），
        // 但是仍然想进行频繁的 checkpoint 去最小化故障后重跑的 pipelines 来说，是有意义的。
        //该选项不能和 “checkpoints 间的最小时间”同时使用。
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 启用不对齐的检查点保存方式
        checkpointConfig.enableUnalignedCheckpoints(false);
        // 设置为0表示不容忍checkpoint失败,2为允许两个连续的 checkpoint 错误，checkpoint 可容忍连续失败次数：该属性定义可容忍多少次连续的 checkpoint 失败。超过这个阈值之后会触发作业错误 fail over。 默认次数为“0”，这意味着不容忍 checkpoint 失败，作业将在第一次 checkpoint 失败时fail over。
        // 可容忍的checkpoint失败仅适用于下列情形：Job Manager的IOException，TaskManager做checkpoint时异步部分的失败， checkpoint超时等。
        // TaskManager做checkpoint时同步部分的失败会直接触发作业fail over。其它的checkpoint失败（如一个checkpoint被另一个checkpoint包含）会被忽略掉。
        checkpointConfig.setTolerableCheckpointFailureNumber(10);
        // 重启策略：重启延迟为10s，固定无限期重试
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Time.of(10, TimeUnit.SECONDS)));
        // 一旦Flink程序被cancel后，会保留checkpoint数据，
        // ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        // ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        logger.info("成功加载完毕Environment");
        return environment;
    }
}
