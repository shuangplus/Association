package association;
/**
 * Created by tp on 2016/10/27.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.ml.param.FloatParam;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static java.util.Calendar.DAY_OF_YEAR;
import static java.util.Calendar.getInstance;

// $example off$
// $example on$
// $example off$

public class JavaSimpleFPGrowth {

    public static void FPGrowth(String[] args) {
        SparkConf conf = new SparkConf().setAppName("FP-growth Example Pre_"+args[0]+"_"+args[1]);
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        //获取当前时间
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //设置日期格式
        String current = df.format(new Date()).substring(0,10); // new Date()为获取当前系统时间
        System.out.println(current);
        Calendar calendar = getInstance();
        calendar.set(DAY_OF_YEAR, calendar.get(DAY_OF_YEAR) - 3);
        Date today = calendar.getTime();
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
        String past = format.format(today);

        // $example on$  hdfs://master1:8020/ws/project/guanyi/source/tradeitem/*
        JavaRDD<String> data = sc.textFile("/source/test/tradeitem/"+args[0]+"/*"+args[1]+"/*");

        JavaRDD<List<String>> transactions = data.map(
                new Function<String, List<String>>() {
                    public List<String> call(String line) {
                        String[] parts = line.split(" ");
                        return Arrays.asList(parts);
                    }
                }
        );
        transactions.cache();
        double n = transactions.count();
        System.out.println(n);
        System.out.println("***************************************");
        System.out.println("***************************************");
        System.out.println("***************************************");
        //double s = Double.valueOf(args[1]);

        double s = 0.0001;
        if (args[0].equals("edgj")) {
            switch (args[1]) {
                case "7":
                    s = 0.00016;                  //0.00025    0.00027;   //0.000353
                    break;
                case "30":
                    s = 0.00015;    //0.00022  0.0005   0.0005177
                    break;
                case "90":
                    s = 0.000206;    //0.0001975
                    break;
                case "180":
                    s = 0.0001327;     //82w
                    break;
                default:
                    s = 0.0002;
            }
        } else if (args[0].equals("v1")) {
            switch (args[1]) {
                case "7":
                    s = 0.00002;       //0.00004   14w
                    break;
                case "30":
                    s = 0.000012;     //0.000055   80w
                    break;
                case "90":
                    s = 0.000018;    //没调    90w
                    break;
                case "180":
                    s = 0.00001;     // 130w
                    break;
                default:
                    s = 0.00002;
            }
        } else if (args[0].equals("v2")) {
            switch (args[1]) {
                case "7":
                    s = 0.00005;     //0.0000295  0.00005  0.000045   24607    0.0000198
                    break;
                case "30":
                    s = 0.000028;     //0.00003 0.000029  0.000028   0.00003888
                    break;
                case "90":
                    s = 0.000027;     //0.000028  0.000027  0.00002344  0.00002263   0.00003   1450/n
                    break;
                case "180":
                    s = 0.00002;      //0.000017  0.00001458 1600/n
                    break;
                default:
                    s = (double) 1600 / n;
            }
        } else if (args[0].equals("demo")) {
            switch (args[1]) {
                case "7":
                    s = 0.00002;     //0.00002 0.00005  0.000045   24607    0.0000198
                    break;
                case "30":
                    s = 0.00002;     //0.00003 0.000029  0.000028   0.00003888
                    break;
                case "90":
                    s = (double) 6 / n;     //0.000027  0.00002344  0.00002263   0.00003   1450/n
                    break;
                case "180":
                    s = (double) 9.5 / n;      //0.000017  0.00001458 1600/n
                    break;
                default:
                    s = (double) 1600 / n;
            }
        }

        if (args[2].equals("1")){
            s = s+0.000002;
        }else if (args[2].equals("2")){
            s = s+0.000005;
        }else if (args[2].equals("3")){
            s = s+0.000007;
        }

        //System.out.println(s);
        //s = (double) 2/n;
        FPGrowth fpg = new FPGrowth()
                .setMinSupport(s)
                .setNumPartitions(10);
        System.out.println("FPGrowth fpg***************************************");
        System.out.println("FPGrowth fpg***************************************");
        System.out.println("FPGrowth fpg***************************************");
        FPGrowthModel<String> model = fpg.run(transactions);


        System.out.println("model***************************************");
        System.out.println("model***************************************");
        System.out.println("model***************************************");
        long c = model.freqItemsets().count();
        System.out.println(c);

        //FPGrowth.FreqItemset<String> itemset = model.freqItemsets().toJavaRDD().collect();
        model.freqItemsets().saveAsTextFile("/source/test/freqItemsets/"+args[0]+"/cdate="+current+"_"+args[1]);

        System.out.println("save freqItemsets***************************************");
        System.out.println("save freqItemsets***************************************");
        System.out.println("save freqItemsets***************************************");

/*
        double minConfidence = 0.1;
        if (args[1].equals("180")) {

            for (AssociationRules.Rule<String> rule
                    : model.generateAssociationRules(minConfidence).toJavaRDD().collect()) {
                System.out.println(
                        rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
            }

            // $example off$
            model.generateAssociationRules(minConfidence).saveAsTextFile("/source/test/associationRules/" + args[0] +
                    "/associationRules_" + args[1]);

        }else {
            System.out.println("Did not save!");
        }
*/

        sc.stop();


    }
}
