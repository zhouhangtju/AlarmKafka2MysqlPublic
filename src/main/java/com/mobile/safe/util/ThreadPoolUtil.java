package com.mobile.safe.util;

import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {


    private static ThreadPoolExecutor consumerExecutor = new ThreadPoolExecutor(32,
            32,0, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(50), Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy());

    private static ThreadPoolExecutor apiExecutor = new ThreadPoolExecutor(16,
            32,0, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(50), Executors.defaultThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy());

    public static ThreadPoolExecutor getConsumerExecutor() {
        return consumerExecutor;
    }

    public static ThreadPoolExecutor getApiExecutor() {
        return apiExecutor;
    }
}
