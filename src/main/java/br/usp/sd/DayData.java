package br.usp.sd;
//package ep2mapreduce.src.main.java.br.usp.sd;

import java.util.StringTokenizer;

import org.apache.log4j.Logger;

public class DayData {
    
    public static Logger logger = Logger.getLogger(DayData.class);

    private double temp, dewp, slp, stp, visib, wdsp, mxspd, gust, max, min, prcp, sndp;
    private int day, tempCount, dewpCount, slpCount, stpCount, visibCount, wdspCount, frshtt;
    private char maxFlag, minFlag, prcpFlag;
    
    public DayData(String data)
    {
        StringTokenizer st = new StringTokenizer(data);
        //logger.info("data: "+data);
        day = Integer.parseInt(st.nextToken());
        temp = Double.parseDouble(st.nextToken());
        tempCount = Integer.parseInt(st.nextToken());
        dewp = Double.parseDouble(st.nextToken());
        dewpCount = Integer.parseInt(st.nextToken());
        slp = Double.parseDouble(st.nextToken());
        slpCount = Integer.parseInt(st.nextToken());
        stp = Double.parseDouble(st.nextToken());
        stpCount = Integer.parseInt(st.nextToken());
        visib = Double.parseDouble(st.nextToken());
        visibCount = Integer.parseInt(st.nextToken());
        wdsp = Double.parseDouble(st.nextToken());
        wdspCount = Integer.parseInt(st.nextToken());
        mxspd = Double.parseDouble(st.nextToken());
        gust = Double.parseDouble(st.nextToken());
        String max = st.nextToken();
        this.max = Double.parseDouble(max.replace("*", ""));
        maxFlag = max.contains("*") ? '*' : ' ';
        String min = st.nextToken();
        this.min = Double.parseDouble(min.replace("*",""));
        minFlag = min.contains("*") ? '*' : ' ';
        String prcp = st.nextToken();
        try {
            this.prcp = Double.parseDouble(prcp);
            this.prcpFlag = ' ';
        } catch (Exception e) {
            //logger.info("substring: "+prcp.substring(0, prcp.length()-1));
            this.prcp = Double.parseDouble(prcp.substring(0, prcp.length()-1));
            this.prcpFlag = prcp.charAt(prcp.length()-1);
        }
        sndp = Double.parseDouble(st.nextToken());
        frshtt = Integer.parseInt(st.nextToken());
        
    }
    
    public int getDay(){
    	return day;
    }
    
    public double getTemp() {
        return temp;
    }

    public double getDewp() {
        return dewp;
    }

    public double getSlp() {
        return slp;
    }

    public double getStp() {
        return stp;
    }

    public double getVisib() {
        return visib;
    }

    public double getWdsp() {
        return wdsp;
    }

    public double getMxspd() {
        return mxspd;
    }

    public double getGust() {
        return gust;
    }

    public double getMax() {
        return max;
    }

    public double getMin() {
        return min;
    }

    public double getPrcp() {
        return prcp;
    }

    public double getSndp() {
        return sndp;
    }

    public int getTempCount() {
        return tempCount;
    }

    public int getDewpCount() {
        return dewpCount;
    }

    public int getSlpCount() {
        return slpCount;
    }

    public int getStpCount() {
        return stpCount;
    }

    public int getVisibCount() {
        return visibCount;
    }

    public int getWdspCount() {
        return wdspCount;
    }

    public int getFrshtt() {
        return frshtt;
    }

    public char getMaxFlag() {
        return maxFlag;
    }

    public char getMinFlag() {
        return minFlag;
    }

    public char getPrcpFlag() {
        return prcpFlag;
    }
      
}
