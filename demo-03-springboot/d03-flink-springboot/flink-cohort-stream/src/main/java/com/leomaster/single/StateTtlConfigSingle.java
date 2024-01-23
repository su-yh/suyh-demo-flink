package com.leomaster.single;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

/**
 * @author suyh
 * @since 2024-01-10
 */
public class StateTtlConfigSingle {
    private static volatile StateTtlConfig instance;
    private static final Object lock = new Object();

    public static StateTtlConfig getInstance() {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = StateTtlConfig.newBuilder(Time.hours(25))
                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                            .cleanupFullSnapshot()
                            .cleanupInRocksdbCompactFilter(1000L)
                            .build();
//                    instance = StateTtlConfig.newBuilder(Time.minutes(10)).build();
//                    instance = StateTtlConfig.newBuilder(Time.minutes(10)).cleanupFullSnapshot().build();
//                    instance = StateTtlConfig.newBuilder(Time.minutes(10))
//                            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
//                            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
//                            .cleanupFullSnapshot()
//                            .build();
                }
            }
        }

        return instance;
    }
}
