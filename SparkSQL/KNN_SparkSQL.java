import java.io.*;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


public class KNN_SparkSQL
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
        
        // Starting Point: SparkSession
        SparkSession spark = SparkSession.builder().appName("SparkSQL").getOrCreate();
        
        // Create an RDD
        JavaRDD<String> rdd = spark.sparkContext().textFile(inputFilePath, 1).toJavaRDD();
  
        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();    
        StructField field1 = DataTypes.createStructField("distance", DataTypes.DoubleType, true);
        StructField field2 = DataTypes.createStructField("text", DataTypes.StringType, true);
        fields.add(field1);
        fields.add(field2);
        StructType schema = DataTypes.createStructType(fields);
              
        // Convert records of the RDD to Rows
        JavaRDD<Row> rowRDD = rdd.map((Function<String, Row>) record ->
        {   
        	String[] attributes = record.split(",");
            double point_x = Double.valueOf(attributes[1]);
            double point_y = Double.valueOf(attributes[2]);
            
            double sum = (point_x - query_x) * (point_x - query_x) + (point_y - query_y) * (point_y - query_y);
            double dist = Math.sqrt(sum);
            
            String output = "ID:" + attributes[0] + "; (x:" + point_x + ", y:" + point_y + ")\t distance:" + dist;

            return RowFactory.create(dist, output);     	
        });
    
        // Apply the schema to the RDD
        Dataset<Row> dataFrame = spark.createDataFrame(rowRDD, schema);       
    
        // Creates a temporary view using the DataFrame
        dataFrame.createOrReplaceTempView("KNN");
             
        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT text AS output FROM KNN ORDER BY distance LIMIT " + k);
         
        List<String> sorted = results.toDF().select("output").as(Encoders.STRING()).collectAsList();
     
        FileWriter fileWriter = new FileWriter("SparkSQL_Result.txt");
        PrintWriter printWriter = new PrintWriter(fileWriter);
        
        printWriter.println("--------------- SparkSQL Result: ---------------\n");       
        for(int i = 0; i < sorted.size(); ++i)
        {    	
        	printWriter.println(sorted.get(i));
        }       
        printWriter.println( "\n--------------- SparkSQL End ---------------" );
        
        printWriter.close();
        fileWriter.close();
    }
}