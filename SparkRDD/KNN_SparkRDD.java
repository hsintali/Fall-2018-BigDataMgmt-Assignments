import java.io.*;
import java.io.IOException;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


public class KNN_SparkRDD
{
    public static void main(String[] args) throws IOException
    {
    	if(args.length != 3) System.exit(-1);
    	
    	String inputFilePath = args[0];    	
    	File inputFile = new File(inputFilePath);
        if(!inputFile.exists()) System.exit(-1);
        
        String[] query = args[1].split(",");
        final Double query_x = Double.parseDouble(query[0]);
        final Double query_y = Double.parseDouble(query[1]);
        
        Integer k = Integer.parseInt(args[2]);
        
        JavaSparkContext spark = new JavaSparkContext("local", "KNN");
        
        JavaRDD<String> logFile = spark.textFile(inputFilePath);
        
        JavaPairRDD<Double, String> distanceMap = logFile.mapToPair(new PairFunction<String, Double, String>()
        {
            public Tuple2<Double, String> call(String line) throws Exception
            {
                String[] text = line.split(",");
                Double point_x = Double.valueOf(text[1]);
                Double point_y = Double.valueOf(text[2]);

                double sum = (point_x - query_x) * (point_x - query_x) + (point_y - query_y) * (point_y - query_y);
                double dist = Math.sqrt(sum);

                String output = "ID:" + text[0] + "; (x:" + text[1] + ", y:" + text[2] + ")\t distance:" + dist;
                return new Tuple2<Double,String>(dist, output);
            }
        });
    	
        JavaPairRDD<Double, String> sorted = distanceMap.sortByKey(true);
      
        FileWriter fileWriter = new FileWriter("SparkRDD_Result.txt");
        PrintWriter printWriter = new PrintWriter(fileWriter);
        
        printWriter.println("--------------- SparkRDD Result: ---------------\n");       
        for(Tuple2<Double, String> text : sorted.collect())
        {
            if(k > 0)
            {
            	printWriter.println(text._2);
                --k;
            }
            else
            {
                break;
            }
        }        
        printWriter.println( "\n--------------- SparkRDD End ---------------" );
        
        printWriter.close();
        fileWriter.close();
    }
}