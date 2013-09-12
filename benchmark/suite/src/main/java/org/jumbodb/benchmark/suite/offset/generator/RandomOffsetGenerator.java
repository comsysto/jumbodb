package org.jumbodb.benchmark.suite.offset.generator;

import org.apache.commons.lang.math.RandomUtils;
import org.jumbodb.benchmark.suite.offset.FileOffset;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO ULF CARSTEN offset sorting?
 * @author Ulf Gitschthaler
 */
public class RandomOffsetGenerator extends BaseOffsetGenerator {

    @Override
    List<FileOffset> generateFileOffsets(File[] dataFiles, int defaultSampleCount, int lastSampleCount) {
        List<FileOffset> fileOffsets = new ArrayList<FileOffset>(dataFiles.length * defaultSampleCount);
        for (int i=0; i<dataFiles.length; i++) {
            int sampleCount = i < (dataFiles.length - 1) ? defaultSampleCount : lastSampleCount;
            fileOffsets.addAll(generateFileOffsets(dataFiles[i], sampleCount));
        }
        return fileOffsets;
    }

    List<FileOffset> generateFileOffsets(File dataFile, int samplesPerFile) {
        List<FileOffset> fileOffsets = new ArrayList<FileOffset>(samplesPerFile);

        long [] gapSizes = generateRandomGaps(dataFile, samplesPerFile);
        assert (samplesPerFile + 1) == gapSizes.length;
        long newRangeStartPos = 0l;

        for (int i=0; i<samplesPerFile; i++) {
            long finalOffset = gapSizes[i] + newRangeStartPos;
            newRangeStartPos = finalOffset + getAverageDataSetSize() + 1;
            fileOffsets.add(new FileOffset(dataFile, finalOffset));
        }
        return fileOffsets;
    }

    private long[] generateRandomGaps(File dataFile, int samplesPerFile) {
        long overallGapSize = calculateRemainingBytes(dataFile, samplesPerFile);
        int averageGapSize = calculateAverageGapSize(overallGapSize, samplesPerFile);
        int maxGapSize = calculateRandomNumberRange(averageGapSize);
        int nrOfGaps = calculateNumberOfGaps(samplesPerFile);

        long [] gapSizes = new long[nrOfGaps];
        fillWithGaps(gapSizes, maxGapSize);
        distributeDeviation(gapSizes, overallGapSize);
        return gapSizes;
    }

    private void fillWithGaps(long[] gapSizes, int maxGapSize) {
        for (int i=0; i<gapSizes.length; i++) {
            long randomGapSize = (long)(RandomUtils.nextDouble() * maxGapSize);
            gapSizes[i] = randomGapSize;
        }
    }

    private void distributeDeviation(long[] gapSizes, long overallGapSize) {
        long deviation = calculateDeviation(gapSizes, overallGapSize);
        int nrOfGaps = gapSizes.length;

        while(deviation != 0l) {
            int indexOfAdaptedGap = RandomUtils.nextInt(nrOfGaps);
            long currentSize = gapSizes[indexOfAdaptedGap];

            if (deviation > 0) {
                gapSizes[indexOfAdaptedGap] = ++currentSize;
                deviation--;
            } else if(currentSize > 0) {
                gapSizes[indexOfAdaptedGap] = --currentSize;
                deviation++;
            }
        }
    }

    private long calculateDeviation(long[] gapSizes, long overallGapSize) {
        long deviation = overallGapSize;

        for (long gapSize : gapSizes) {
            deviation -= gapSize;
        }
        return deviation;
    }

    private long calculateRemainingBytes(File dataFile, int samplesPerFile){
        return readFileSize(dataFile) - samplesPerFile * getAverageDataSetSize();
    }

    private int calculateAverageGapSize(long summedUpGapSize, int samplesPerFile) {
        return (int) summedUpGapSize / (samplesPerFile + 1);
    }

    private int calculateRandomNumberRange(int averageGapSize) {
        return averageGapSize * 2;
    }

    private int calculateNumberOfGaps(int samplesPerFile) {
        return samplesPerFile > 0 ? samplesPerFile + 1 : 0;
    }

    // TODO snappy
    private long readFileSize(File dataFile){
        return dataFile.length();
    }
}
