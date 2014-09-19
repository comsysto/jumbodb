import org.apache.commons.collections.FastHashMap;
import org.apache.commons.lang.time.FastDateFormat;

import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Locale;
import java.util.TimeZone;

public class Testen {
    public static void main(String[] args) throws Exception {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.US);
        System.out.println(simpleDateFormat.parse("Thu Apr 18 01:12:17 +0000 2013"));

//        int v1 = Integer.MAX_VALUE - 8437589;
//        int v2 = Integer.MAX_VALUE - 7894375;
//        int v3 = 252;
//
//        }
    }


//    public class Bits {

        public static int getBoundaryBoxComparistionValue(int point1, int point2) {
            int bitsToShift = getBitsToShift(point1, point2);
            return point1 >>> bitsToShift;
        }

        public static int getBitsToShift(int b1, int b2) {
            int max = Math.min(Integer.highestOneBit(b1), Integer.highestOneBit(b2));
            for (int i = 0; i < max; i++) {
                if(b1 == b2) {
                    return i;
                }
                System.out.println("B1 " + i + " -> " + Integer.toBinaryString(b1));
                System.out.println("B2 " + i + " -> " + Integer.toBinaryString(b2));
                b1 = b1 >>> 1;
                b2 = b2 >>> 1;
            }
            return 0;
        }

        public static BitSet convert(int value) {
            BitSet bits = new BitSet();
            for(int i = 31; i >= 0; i--) {
                if(value % 2 != 0) {
                    bits.set(i);
                }
                value = value >>> 1;
            }
            return bits;
        }

        public static int convert(BitSet bits) {
            int value = 0;
            for (int i = 0; i < bits.length(); ++i) {
                value += bits.get(i) ? (1 << i) : 0;
            }
            return value;
        }
//    }
}
