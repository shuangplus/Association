package funv;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


import association.JavaSimpleFPGrowth;

/**
 * 启动一个任务，然后等待任务的计算结果，如果等待时间超出预设定的超时时间，则中止任务。
 *

 */

/**
 * Created by kingdee on 2017/1/12.
 */
public class TaskTimeoutDemo {


    public static void testTask(ExecutorService exec, int timeout, String[] ar) {
        // 启动任务
        MyTask task = new MyTask(ar);
        Future<Boolean> future = exec.submit(task);
        Boolean taskResult = null;
        String failReason = null;
        try {
            // 等待计算结果，最长等待timeout秒，timeout秒后中止任务
            taskResult = future.get(timeout, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            failReason = "主线程在等待计算结果时被中断！";
        } catch (ExecutionException e) {
            failReason = "主线程等待计算结果，但计算抛出异常！";
        } catch (TimeoutException e) {
            failReason = "主线程等待计算结果超时，因此中断任务线程！";
            exec.shutdownNow();
        }

        System.out.println("taskResult : " + taskResult);
        System.out.println("failReason : " + failReason);

    }

    public static void main(String[] args) {
        System.out.println("**********************\nStart ...\n*************************");
        String[] str = {args[0],args[1],"0"};

        ExecutorService exec = Executors.newCachedThreadPool();

        testTask(exec, 1200,str);   //运行20分钟还未出结果则kill掉，并调整阈值
        exec.shutdown();

        System.out.println("End!");
    }

}

class MyTask implements Callable<Boolean> {

    private String[] ar;
    public MyTask(String[] ar) {
        this.ar = ar;
    }

    @Override

    public Boolean call() throws Exception {
        // 总计耗时约10秒

//            String[] ar={"","",""};
        JavaSimpleFPGrowth.FPGrowth(ar);

        return Boolean.TRUE;
    }
}
