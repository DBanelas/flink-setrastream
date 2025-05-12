package org.dbanelas;

import org.hipparchus.linear.RealMatrix;

public class SimilarityMeasureUtil {

    public static double rv(Batch left, Batch right, double epsilon) {
        RealMatrix leftBBt = left.getBBt();
        RealMatrix rightBBt = right.getBBt();
        return rv(leftBBt, rightBBt, epsilon);
    }

    public static double rv(RealMatrix left, RealMatrix right, double epsilon) {
        double numerator = left.multiply(right).getTrace();
        double traceLeft2 = left.multiply(left).getTrace();
        double traceRight2 = right.multiply(right).getTrace();

        double denominator = Math.sqrt(traceLeft2 * traceRight2);
        if (denominator < epsilon) {
            if (traceLeft2 < epsilon && traceRight2 < epsilon) {
                return 1.0;
            } else {
                return 0.0;
            }
        }

        return numerator / denominator;
    }

}
