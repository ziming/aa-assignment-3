package aa;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Problem1Reducer implements Reducer {

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

        List<AtomicInteger> countList = (List<AtomicInteger>) data;

        HashMap<String, Integer> resultMap = new HashMap<>();

        int finalCount = 0;

        for (AtomicInteger count : countList) {
            finalCount += count.intValue();
        }

        resultMap.put((String) key, finalCount);

        return resultMap;
    }
}
