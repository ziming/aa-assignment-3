package aa;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Problem1Mapper implements Mapper {

    /**
     * Implement this to provide the mapping function.
     * It is STRONGLY recommended that the map have no side-effects (no use of non-local variables).
     *
     * @param list the data shard for this mapper
     * @return a map of key-value pairs which will be combined from all mappers and given to the reducers
     */
    @Override
    public HashMap map(List list) {

        // I believe the list is the list of words
        List<String> wordList = (List<String>) list;

        // prof interface returns HashMap so use HashMap instead of Map
        HashMap<String, AtomicInteger> wordCountMap = new HashMap<>();

        for (String word : wordList) {

            AtomicInteger currentWordCount = wordCountMap.get(word);

            if (currentWordCount != null) {
                currentWordCount.getAndIncrement();
            } else {
                wordCountMap.put(word, new AtomicInteger(1));
            }

        }

        return wordCountMap;
    }
}
