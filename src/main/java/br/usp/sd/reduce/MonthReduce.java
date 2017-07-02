package br.usp.sd.reduce;
//package ep2mapreduce.src.main.java.br.usp.sd.reduce;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import br.usp.sd.DayData;
//import ep2mapreduce.src.main.java.br.usp.sd.DayData;

public class MonthReduce extends Reducer<Text, Text, Text, Text> {
    
    public static Logger logger = Logger.getLogger(MonthReduce.class); 
    
    @Override
    protected void reduce(Text key, Iterable<Text> value, Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        
        StringBuilder result = new StringBuilder();
        List<String> data = new ArrayList<>();
        
        logger.info("key: "+key+" data: ");
        for (Text t : value) {
            data.add(t.toString());
            logger.info("t: "+t.toString());
        }
        
        double[] average = calcAverage(data);
        double[] desvio = calcDesvio(data, average);
        double[][] mmq = calcMMQ(data, average);
        double[] variancia = calcVariancia(desvio);
        double[] cvp = calcCVP(average, desvio);

        
        DecimalFormat df = new DecimalFormat("#00.00");
        for (int i = 0; i < average.length; i++) {
            if (i > 0) {
                result.append(" ");
            }
            result.append(df.format(average[i])+" "+df.format(desvio[i])+
            		" "+df.format(mmq[i][0]) + " " + df.format(mmq[i][1]));
        }
        
        context.write(key, new Text(result.toString()));
    }
    
    
    public static double[] calcAverage(List<String> value) {
        double[] average = new double[3];
        int[] count = new int[3];
        for (String data : value) {
            DayData dayData = new DayData(data);
            if (dayData.getTempCount() > 0) {
                average[0] += dayData.getTemp();
                count[0]++;
            }
            
            if (dayData.getDewpCount() > 0) {
                average[1] += dayData.getDewp();
                count[1]++;
            }
            
            if (dayData.getWdspCount() > 0) {
                average[2] += dayData.getWdsp();
                count[2]++;
            }
            
        }
        
        for (int i = 0; i < average.length; i++) {
            average[i] /= count[i];
        }
        
        return average;
    }
    
    public static double[] calcDesvio(List<String> value, double[] media) {
        double[] desvio = new double[3];
        int[] count = new int[3];
        for (String data : value) {
            DayData dayData = new DayData(data);
            if (dayData.getTempCount() > 0) {
                desvio[0] += Math.pow(dayData.getTemp()-media[0], 2);
                count[0]++;
            }
            if (dayData.getDewpCount() > 0) {
                desvio[1] += Math.pow(dayData.getDewp()-media[1], 2);
                count[1]++;
            }
            
            if (dayData.getWdspCount() > 0) {
                desvio[2] += Math.pow(dayData.getWdsp()-media[2], 2);
                count[2]++;
            }
            
        }
        
        for (int i = 0; i < desvio.length; i++) {
            if (count[i] > 1) {
                desvio[i] = Math.sqrt(desvio[i]/(count[i]-1));
            }
        }
        
        return desvio;
    }
    
    public static double[][] calcMMQ(List<String> value, double[] media) {
        double[][] mmq = new double[media.length][2];
        double media_dia = (value.size() / 2) + 0.5;
        double[] top = new double[media.length];
        double[] bot = new double[media.length];
        int i;
        for (String data : value) {
            DayData dayData = new DayData(data);
            
            if (dayData.getTempCount() > 0) {
            	i = 0;
            	top[i] += dayData.getDay() * (dayData.getTemp() - media[i]);
                bot[i] += dayData.getDay() * (dayData.getDay() - media_dia);
                
            }
            if (dayData.getDewpCount() > 0) {
            	i = 1;
            	top[i] += dayData.getDay() * (dayData.getDewp() - media[i]);
                bot[i] += dayData.getDay() * (dayData.getDay() - media_dia);
            }
            
            if (dayData.getWdspCount() > 0) {
            	i = 2;
            	top[i] += dayData.getDay() * (dayData.getWdsp() - media[i]);
                bot[i] += dayData.getDay() * (dayData.getDay() - media_dia);
            }
            
        }
        
        for (int ii = 0; ii < top.length; ii++) {
        	
        	mmq[ii][1] = top[ii] / bot[ii];
            mmq[ii][0] = media[ii] - mmq[ii][1] * media_dia;
        }
        
        return mmq;
    }

    public static double[] calcVariancia(double[] desvio) {
		double[] variancia = new double[desvio.length]; 
        for (int i = 0; i < variancia.length; i++) {
			variancia[i] = Math.pow(desvio[i],2)
        }
		return variancia;
    }
	
    public static double[] calcCVP(double[] media, double[] desvio) {
		double[] CVP = new double[desvio.length]; ;
        for (int i = 0; i < CVP.length; i++) {
			CVP[i] = (desvio[i]/media[i])*100;
        }
		return CVP;
    }

    
}
