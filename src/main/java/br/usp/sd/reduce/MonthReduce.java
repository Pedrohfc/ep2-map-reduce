package br.usp.sd.reduce;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import br.usp.sd.DayData;

public class MonthReduce extends Reducer<Text, Text, Text, Text> {
    
    public static Logger logger = Logger.getLogger(MonthReduce.class); 
    
    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        
        StringBuilder result = new StringBuilder();
        double[] average = new double[3];
        int count = 0;
        
        for (Text data : value) {
            DayData dayData = new DayData(data.toString());
            average[0] += dayData.getTemp();
            average[1] += dayData.getDewp();
            average[2] += dayData.getWdsp();
            count++;
        }
        
        for (int i = 0; i < average.length; i++) {
            result.append(average[i]/count+" ");
        }
        
        context.write(key, new Text(result.toString()));
    }
}
