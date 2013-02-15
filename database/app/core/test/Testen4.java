package core.test;

import java.io.RandomAccessFile;

/**
 * Created with IntelliJ IDEA.
 * User: carsten
 * Date: 1/10/13
 * Time: 9:37 AM
 * To change this template use File | Settings | File Templates.
 */
public class Testen4 {
    public static void main(String[] args) throws Exception {
        RandomAccessFile raf = new RandomAccessFile("/Users/carsten/smhadoop/input/catchbig/daily/part-r-00000", "r");


        for(int i = 0; i < 20000; i++) {
            raf.readLine();
        }
        System.out.println(raf.getFilePointer());
        System.out.println(raf.readLine());


        raf.close();
    }
}

/*
  nr: 10000
  4487808
{"_id":{"date":20121002,"fromCell":{"cellId":"12141124114","lonlat":[0.684,51.782]},"toCell":{"cellId":"12141113443","lonlat":[0.516,51.8685]}},"value":{"date":20121002,"residentsFromCell":192.2470871368752,"age20":0.0,"age30":0.0,"age40":0.0,"age50":0.0,"age60":0.0,"age70":0.0,"stuff":0.0,"basic":1.847102778422016,"female":0.0,"male":0.0,"fourWeekIntervals":[201240,201241,201242,201243],"yearAndWeek":201240,"days":1,"yearAndMonth":201240}} 

8990472
{"_id":{"date":20121002,"fromCell":{"cellId":"121144131114","lonlat":[0.486,51.31]},"toCell":{"cellId":"121144143213","lonlat":[0.45,51.3676]}},"value":{"date":20121002,"residentsFromCell":266.73220374157796,"age20":0.0,"age30":0.0,"age40":0.0,"age50":0.0,"age60":0.0,"age70":0.0,"stuff":0.0,"basic":2.7380519342785736,"female":0.0,"male":0.0,"fourWeekIntervals":[201240,201241,201242,201243],"yearAndWeek":201240,"days":1,"yearAndMonth":201240}} 

 */