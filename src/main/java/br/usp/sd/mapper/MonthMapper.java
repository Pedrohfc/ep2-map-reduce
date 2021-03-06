package br.usp.sd.mapper;
//package ep2mapreduce.src.main.java.br.usp.sd.mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

public class MonthMapper extends Mapper<Object, Text, Text, Text> {
    
    public static Logger logger = Logger.getLogger(MonthMapper.class);
    
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        
        StringTokenizer st = new StringTokenizer(value.toString());
        logger.info("value map: "+value.toString());
        
        if (!st.nextToken().equals("STN---")) { // cabecalho do arquivo
            st.nextToken();
            
            String ymd = st.nextToken();
            
            String month = ymd.substring(0, 6);
            StringBuilder data = new StringBuilder();
            data.append(ymd.substring(6, 8)+" ");
            while (st.hasMoreTokens()) {
                data.append(st.nextToken()+" ");
            }
            
            Text monthKey = new Text(month);
            Text dataValue = new Text(data.toString());
            
            
            context.write(monthKey, dataValue);
        }
    }
}
