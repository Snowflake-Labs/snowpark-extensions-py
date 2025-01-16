import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PartitionUtils {

    public static class Partition {
        private final String whereClause;
        private final int partitionId;

        public Partition(String whereClause, int partitionId) {
            this.whereClause = whereClause;
            this.partitionId = partitionId;
        }

        public String getWhereClause() {
            return whereClause;
        }

        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public String toString() {
            return "Partition{" +
                    "whereClause='" + whereClause + '\'' +
                    ", partitionId=" + partitionId +
                    '}';
        }
    }

    public static class JDBCPartitioningInfo {
        private final String column;
        private final long lowerBound;
        private final long upperBound;
        private final int numPartitions;

        public JDBCPartitioningInfo(String column, long lowerBound, long upperBound, int numPartitions) {
            this.column = column;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.numPartitions = numPartitions;
        }

        public String getColumn() {
            return column;
        }

        public long getLowerBound() {
            return lowerBound;
        }

        public long getUpperBound() {
            return upperBound;
        }

        public int getNumPartitions() {
            return numPartitions;
        }
    }

    public static List<Partition> columnPartition(Map<String, String> partitioningInfoMap) {
        String column = partitioningInfoMap.get("partitionColumn");
        long lowerBound = Long.parseLong(partitioningInfoMap.get("lowerBound"));
        long upperBound = Long.parseLong(partitioningInfoMap.get("upperBound"));
        int numPartitions = Integer.parseInt(partitioningInfoMap.get("numPartitions"));

        if (numPartitions <= 1 || lowerBound == upperBound) {
            List<Partition> singlePartition = new ArrayList<>();
            singlePartition.add(new Partition(null, 0));
            return singlePartition;
        }

        if (lowerBound > upperBound) {
            throw new IllegalArgumentException(
                "Operation not allowed: the lower bound of partitioning column is larger than the upper bound. " +
                "Lower bound: " + lowerBound + "; Upper bound: " + upperBound
            );
        }

        if ((upperBound - lowerBound) < numPartitions || (upperBound - lowerBound) < 0) {
            System.out.println(
                "Warning: The number of partitions is reduced because the specified number of " +
                "partitions is less than the difference between upper bound and lower bound. " +
                "Updated number of partitions: " + (upperBound - lowerBound) + "; Input number of partitions: " +
                numPartitions + "; Lower bound: " + lowerBound + "; Upper bound: " + upperBound
            );
            numPartitions = (int) (upperBound - lowerBound);
        }

        long stride = upperBound / numPartitions - lowerBound / numPartitions;
        List<Partition> partitions = new ArrayList<>();

        long currentValue = lowerBound;
        for (int i = 0; i < numPartitions; i++) {
            String lBound = (i != 0) ? column + " >= " + currentValue : null;
            currentValue += stride;
            String uBound = (i != numPartitions - 1) ? column + " < " + currentValue : null;

            String whereClause;
            if (uBound == null) {
                whereClause = lBound;
            } else if (lBound == null) {
                whereClause = uBound + " OR " + column + " IS NULL";
            } else {
                whereClause = lBound + " AND " + uBound;
            }
            partitions.add(new Partition(whereClause, i));
        }
        return partitions;
    }

    public static void main(String[] args) {
        Map<String, String> partitioningInfoMap = Map.of(
                "column", "id",
                "lowerBound", "1",
                "upperBound", "100",
                "numPartitions", "5"
        );

        List<Partition> partitions = columnPartition(partitioningInfoMap);
        partitions.forEach(System.out::println);
    }
}
