package org.dbanelas;

import org.hipparchus.linear.Array2DRowRealMatrix;
import org.hipparchus.linear.RealMatrix;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;

public class Batch implements Serializable {
    @Serial
    private static final long serialVersionUID = 2L;

    private final int id;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final List<DataPoint> dataPoints;
    private final int numDataPoints;
    private final RealMatrix BBt;

    public Batch(List<DataPoint> dataPoints, int id, long minTimestamp, long maxTimestamp) {
        this.dataPoints = dataPoints;
        this.numDataPoints = dataPoints.size();
        this.BBt = createBatchTransposeProduct();
        this.id = id;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
    }

    public List<DataPoint> getDataPoints() {
        return dataPoints;
    }

    public RealMatrix getBBt() {
        return BBt;
    }

    public int getId() {
        return id;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public int getNumDataPoints() {
        return numDataPoints;
    }

    /**
     * Create the batch transpose product matrix (BBt).
     *
     * @return The batch transpose product matrix.
     */
    public RealMatrix createBatchTransposeProduct() {
        double[][] matrix = new double[dataPoints.size()][dataPoints.get(0).getFeatures().size()];
        for (int i = 0; i < dataPoints.size(); i++) {
            for (int j = 0; j < dataPoints.get(i).getFeatures().size(); j++) {
                matrix[i][j] = dataPoints.get(i).getFeatures().get(j);
            }
        }

        RealMatrix Bt = new Array2DRowRealMatrix(matrix);
        RealMatrix B = Bt.transpose();
        return B.multiply(Bt);
    }

    @Override
    public String toString() {
        return "Batch{" +
                "id=" + id +
                "dataPoints=" + dataPoints +
                ", BBt=" + BBt.toString() +
                '}';
    }
}
