package br.usp.sd.reduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class MonthReduce extends Reducer<Text, Text, Text, Text> {
    
    public static Logger logger = Logger.getLogger(MonthReduce.class); 
    
    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        StringBuilder result = new StringBuilder();
        double[] average = new double[4];
        int count = 0;
        
        for (Text data : value) {
            StringTokenizer st = new StringTokenizer(data.toString());
            for (int i = 0; i < average.length; i++) {
                average[i] += Double.parseDouble(st.nextToken().split(",")[0]);
            }
            count++;
        }
        
        for (double total : average) {
            result.append(total/count+" ");
        }
        
        context.write(key, new Text(result.toString()));
    }
}
