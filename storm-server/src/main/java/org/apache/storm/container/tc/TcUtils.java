package org.apache.storm.container.tc;

public class TcUtils {

    private final static String BIT_UNIT = "bit";
    private final static String K_BIT_UNIT = "Kbit";
    private final static String M_BIT_UNIT = "Mbit";
    private final static String G_BIT_UNIT = "Gbit";

    /**
     * parse rate string to integer with mbit
     *
     * @param rateStr rate string with particular unit bit , Kbit , Mbit
     * @return integer with unit Mbit, if less than 1 return 0.
     */
    public final static Integer parseRateStr(String rateStr){

        if(rateStr.endsWith(K_BIT_UNIT)){
            int value = Integer.parseInt(rateStr.replace(K_BIT_UNIT, ""));
            return value/1000;
        }

        if(rateStr.endsWith(M_BIT_UNIT)){
            int value = Integer.parseInt(rateStr.replace(M_BIT_UNIT, ""));
            return value;
        }

        if(rateStr.endsWith(G_BIT_UNIT)){
            int value = Integer.parseInt(rateStr.replace(G_BIT_UNIT, ""));
            return value*1000;
        }

        if(rateStr.endsWith(BIT_UNIT)){
            int value = Integer.parseInt(rateStr.replace(BIT_UNIT, ""));
            return value/1000000;
        }

        throw new RuntimeException("unknown tc class rate unit");
    }

    public final static void main(String[] args){
        System.out.println(TcUtils.parseRateStr("5000Mbit"));
        System.out.println(TcUtils.parseRateStr("5000Kbit"));
        System.out.println(TcUtils.parseRateStr("5000bit"));
        System.out.println(TcUtils.parseRateStr("5000Gbit"));
        System.out.println(TcUtils.parseRateStr("123bit"));
    }
}
