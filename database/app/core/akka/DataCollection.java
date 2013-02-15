package core.akka;

import com.google.common.collect.HashMultimap;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class DataCollection {
    HashMultimap<String, IndexFile> indexFiles = HashMultimap.create();
    Map<Integer, File> dataFiles = new HashMap<Integer, File>();
}