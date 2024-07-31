package com.yonyougov.yondif.job;

import org.apache.flink.api.common.state.CheckpointListener;

public class ProcessCheckpointListener implements CheckpointListener {
    @Override
    public void notifyCheckpointComplete(long l) throws Exception {
        // Checkpoint 完成时的处理逻辑
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        CheckpointListener.super.notifyCheckpointAborted(checkpointId);
        // Checkpoint 失败时的处理逻辑
    }
}
