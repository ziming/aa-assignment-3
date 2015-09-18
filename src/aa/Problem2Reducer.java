package aa;

import java.util.HashMap;
import java.util.List;

public class Problem2Reducer implements Reducer {

    /**
     * Implement this to provide the reducer function.
     * It is STRONGLY recommended that the reducer have no side-effects (no use of non-local variables).
     *
     * @param key  the key given to this reducer to work on; only one key is given, all values for that key are given
     * @param data a list of all values for this key
     * @return a map of key-value pairs which will be placed in the final output.  Normally this will be a one entry
     * map, with the provided key and the results of the reduce
     */
    @Override
    public HashMap reduce(Object key, List data) {

        List<Double> dataList = (List<Double>) data;

        HashMap<String, Double> resultMap = new HashMap<>();


        // both are the same REVIEW_WITH_WORD_TOTAL_SCORE and REVIEW_WITH_WORD_COUNT
        double totalCount = 0.0;

        for (double count : dataList) {
            totalCount += count;
        }

        resultMap.put((String) key, totalCount);

        return resultMap;
    }
}
