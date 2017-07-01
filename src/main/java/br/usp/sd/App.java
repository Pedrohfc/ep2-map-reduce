package br.usp.sd;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

import br.usp.sd.mapper.MonthMapper;
import br.usp.sd.reduce.MonthReduce;

/**
 * Hello world!
 *
 */
public class App 
{
    public static Logger logger = Logger.getLogger(App.class);
    
    public static void main( String[] args )
    {
        
        logger.info("iniciando job");
        try {
            Configuration config = new Configuration();
            Job job = Job.getInstance(config, "word count");
            job.setJarByClass(App.class);
            job.setMapperClass(MonthMapper.class);
            //job.setCombinerClass(IntSumReducer.class);
            job.setReducerClass(MonthReduce.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            logger.info("job configurado");
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}
