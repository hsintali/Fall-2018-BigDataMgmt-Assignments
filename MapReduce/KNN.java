import java.io.FileReader;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Comparator;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Collections;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

class Point
{
	public String ID;
	public double[] coordinate;
	
	public Point(String line)
    {
		String[] splited = line.split(",");
		
		ID = splited[0];  

		coordinate = new double[2];
		coordinate[0] = Double.parseDouble(splited[1]);
		coordinate[1] = Double.parseDouble(splited[2]);
    }
	
	public String GetPoint()
	{
		String line = ID + "," + Double.toString(coordinate[0]) + "," + Double.toString(coordinate[1]);
		return line;
	}
}

public class KNN 
{
	public static double EuclideanDistance(double[] a, double[] b)	     
	{
		double sum = 0.0;

		for(int i = 0; i < a.length; ++i)	
		{
			sum += Math.pow(a[i] - b[i], 2);   
		}    
	      
		return Math.sqrt(sum);
	}
	
	public static class KnnMapper extends Mapper<LongWritable, Text, Text, Text>
    {
		public Point testPoint;
		
		protected void setup(Context context) throws IOException, InterruptedException          
		{
			Configuration conf = context.getConfiguration();
			testPoint = new Point("-1," + conf.get("queryPoint"));
		}
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException 
		{		
            Point trainPoint = new Point(value.toString());

            double dis = EuclideanDistance(trainPoint.coordinate, testPoint.coordinate);
			           
			context.write(new Text(testPoint.GetPoint()), new Text(value.toString() + "," + Double.toString(dis)));
		}
    }
	
	public static class MapValueStringComparator implements Comparator<Map.Entry<String, Double>>
	{
        public int compare(Entry<String, Double> entry1, Entry<String, Double> entry2)
        {
            return entry1.getValue().compareTo(entry2.getValue());
        }
    }
    
    public static class KnnReducer  extends Reducer<Text, Text, Text, NullWritable>
    {
    	public int k;

		protected void setup(Context context) throws IOException, InterruptedException          
		{
			Configuration conf = context.getConfiguration();
			k = Integer.parseInt(conf.get("k"));
		}	
		
        public void reduce(Text key, Iterable<Text> values, Context context)
        		throws IOException, InterruptedException
        {      
        	Map<String, Double> hashMap = new HashMap<String, Double>(); 
        	
            for (Text value : values)
            {
            	String[] splited = value.toString().split(",");
            	double dis = Double.parseDouble(splited[3]);  
            	
            	hashMap.put("ID:" + splited[0] + "; (x:" + splited[1] + ", y:" + splited[2] + ")\t distance:" + splited[3], dis);
            }
          
            Map<String, Double> sortedMap = new LinkedHashMap<String, Double>();
            
            List<Map.Entry<String, Double>> entryList = new ArrayList<Map.Entry<String, Double>>(hashMap.entrySet());
            
            Collections.sort(entryList, new MapValueStringComparator());

            Iterator<Map.Entry<String, Double>> iter = entryList.iterator();

            for(int i =0; i < k; ++i)
            {
            	if(iter.hasNext())
            	{
                	context.write(new Text(iter.next().getKey()), NullWritable.get());      
            	}
            }
        }
    }
		
	public static void main(String[] args) throws Exception
	{		
		FileSystem fileSystem = FileSystem.get(new Configuration());
		
        if(fileSystem.exists(new Path("hdfs://localhost:9000/result")))
        {
            fileSystem.delete(new Path("hdfs://localhost:9000/result"), true);
        }
		
        Configuration conf = new Configuration();
        conf.set("queryPoint", args[1]);
        conf.set("k", args[2]);
		
        Job job = new Job(conf, "KNN");
        job.setJarByClass(KNN.class);   
             
        // mapper
        job.setMapperClass(KnnMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
        // reducer
        job.setReducerClass(KnnReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        job.setInputFormatClass(TextInputFormat.class);
        
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/result"));
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
        
        FSDataInputStream fr = fileSystem.open(new Path("hdfs://localhost:9000/result/part-r-00000"));
        IOUtils.copyBytes(fr, System.out, 1024, true);
	}
}