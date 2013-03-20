import org.apache.commons.io.FileUtils;

import java.io.File;

public class Testen {
    public static void main(String[] args) {
        File f = new File("/Users/carsten/workspaces/jumbodb/database/~/jumbodb/data/de.catchment.aggregated.daily.sum.by_cell");
        System.out.println(f.length());
        long start = System.currentTimeMillis();
        long l = FileUtils.sizeOfDirectory(f);
        System.out.println(FileUtils.byteCountToDisplaySize(l));
        System.out.println((System.currentTimeMillis() - start));
    }
}
