package org.jumbodb.database.service.query.index.geohash.snappy;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Carsten Hufe
 */
public class GeohashContainer {
    // leftTop, rightTop
    // leftBottom, rightBottom

    private List<GeohashBoundaryBox> splittedBoxes = new LinkedList<GeohashBoundaryBox>();
    private Map<GeohashCoords, GeohashBoundaryBox> localCache = new HashMap<GeohashCoords, GeohashBoundaryBox>();
    private RangeMeterBox rangeMeterBox;

    public RangeMeterBox getRangeMeterBox() {
        return rangeMeterBox;
    }

    public void setRangeMeterBox(RangeMeterBox rangeMeterBox) {
        this.rangeMeterBox = rangeMeterBox;
    }

    public boolean add(GeohashBoundaryBox geohashBoundaryBox) {
        return splittedBoxes.add(geohashBoundaryBox);
    }

    public List<GeohashBoundaryBox> getSplittedBoxes() {
        return splittedBoxes;
    }

    private GeohashBoundaryBox retrieveMatchingBoundaryBox(GeohashCoords geohashCoords) {
        for (GeohashBoundaryBox splittedBox : splittedBoxes) {
            if(splittedBox.isResponsibleFor(geohashCoords.getLatitude(), geohashCoords.getLongitude())) {
                return splittedBox;
            }
        }
        throw new IllegalStateException("Must not happen, because there must be a responsible GeohashBoundaryBox for every part of the world (4 splitted parts)");
    }

    public GeohashBoundaryBox getAppropriateBoundaryBox(GeohashCoords geohashCoords) {
        GeohashBoundaryBox geohashBoundaryBox = localCache.get(geohashCoords);
        if(geohashBoundaryBox == null) {
            geohashBoundaryBox = retrieveMatchingBoundaryBox(geohashCoords);
            localCache.put(geohashCoords, geohashBoundaryBox);
        }
        return geohashBoundaryBox;
    }
}
