package funv;

import association.JavaSimpleFPGrowth;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.*;

import static funv.TaskTimeoutDemo.testTask;


/**
 * Created by kingdee on 2017/1/12.
 */
public class FirstTry {


    public static void main(String[] args) {
        //获取当前时间
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置日期格式
        String current = df.format(new Date()).substring(0,10); // new Date()为获取当前系统时间
        System.out.println(current);

        Configuration conf1 = new Configuration();

        boolean status = hadoopbasic.checkAndDel("/source/test/freqItemsets/" + args[0] + "/cdate="+current+"_" + args[1], conf1);

        if (status == false) {
            System.out.println("**********************\nStart ...\n*************************");
            String[] str = {args[0], args[1], args[2]};

            ExecutorService exec = Executors.newCachedThreadPool();

            testTask(exec, 1200, str);
            exec.shutdown();

            System.out.println("End!");
        }else {
            System.out.println("Results already exist!");
        }
    }
}




