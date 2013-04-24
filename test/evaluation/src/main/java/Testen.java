import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

public class Testen {
    public static void main(String[] args) {
//        Double pi = Math.PI;
//        Double pi2 = Math.PI + 1d;
//        Double pi3 = Math.PI + 100d;
//        Double pi4 = 0.000008246924;
//        System.out.println(Double.doubleToLongBits(pi));
//        System.out.println(Double.doubleToLongBits(pi2));
//        System.out.println(Double.doubleToLongBits(pi3));
//        System.out.println(Double.doubleToLongBits(pi4));
        System.out.println(new Long(1).hashCode() == new Long(-2).hashCode());
        System.out.println(new Long(0).hashCode() == new Long(-1).hashCode());
    }
}
