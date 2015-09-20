package aa;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author kevinsteppe
 *         based on ideas from Google (http://labs.google.com/papers/mapreduce.html)
 *         and the implementation from http://semanticlinguine.blogspot.com
 *         <p>
 *         The intention is to create a structured teaching framework to emphasize coding in a MapReduce parralel style for
 *         java-taught students.  It focuses on structuring the problem into an explicit Map step (producing key-value pairs
 *         as in the google version) followed by a Reduce step (intaking a map of key-List(value) objects and outputing
 *         another map of key-value pairs.
 *         <p>
 *         This is a 'poor man' implementation - nothing distributed, no fault tolerance, no straggler recovery.  It creates
 *         a number of threads for mapping and reducing, then runs everything locally, with maps for data passing.
 */
public class MapReduce {
    protected static boolean _verbose;

    /**
     * @param mapper  instance to provide map function.  Because only one instance is used: 1) don't synchronize it, 2) it should have no non-local effects
     * @param reducer instance to provide reduce funtion.  Same warnings
     * @param data    a list of data.  This could also be a list of *references* to the data location, such as a list of filenames.
     * @param shards  approximate number of shards to split the data into.
     *                A shard size is generated, and the list broken into shards of that size:
     *                shard Size ~ ceiling(data.size / shards)
     *                the last shard being the remainder, ie: data.size % shard Size
     *                for example: if data.size = 30 and shards = 3, then 3 shards of 10 items are generated
     *                if data.size = 30 and shards = 4, then 3 shards of 8 items and 1 of 6 items
     *                if data.size = 30 and shards = 13, then 10 shards of 3 items are generated
     *                Thus for some shard sizes, the actual number of shards may be smaller then specified.  It will not be more.
     *                One mapper is launched for each shard generated.
     *                <p>
     *                One reducer is launched for every key generated by the mappers
     * @param verbose true will make the framework output (S.O.P) a bunch of text about what it is doing.
     * @return a HashMap with keys from the reducer, and a list of all outputs for that key (across all reducers), if your
     * reducers only use the key given to them then the list will always be length 0.
     */
    public static HashMap<Object, List> mapReduce(Mapper mapper, Reducer reducer, List data, int shards, boolean verbose) throws InterruptedException {
        _verbose = verbose;

        //creates mapper tasks, allocates each a shard
        List<Base> mapperTasks = createMappers(data, shards, mapper);

        //creates a thread for each task, starts the thread, waits for all threads to finish
        runThreads(mapperTasks);

        //produces a merged hashmap from the mapper results.  mapper values with the same key are added to a list
        HashMap<Object, List> intermediates = getResults(mapperTasks);

        //create reducer tasks.  takes the intermediates as input.
        List<Base> reducerTasks = createReducers(intermediates, reducer);

        //creates a thread for each task, starts the thread, waits for all threads to finish
        runThreads(reducerTasks);

        //produces a merged hashmap from the reducer results.  reducer values with the same key are added to a list
        HashMap<Object, List> finalResults = getResults(reducerTasks);

        return finalResults;
    }


    private static List<Base> createMappers(List data, int shards, Mapper mapper) {
        ArrayList<Base> mappers = new ArrayList<Base>(shards);
        int shardSize = (int) Math.ceil(data.size() / (float) shards);

        if (_verbose)
            System.out.println("Sharding data -> " + data.size() / shardSize + " shards in the input to send to mappers");

        for (int pointer = 0; pointer < data.size(); pointer += shardSize) {
            int end = Math.min(pointer + shardSize, data.size());
            List dataSubList = data.subList(pointer, end);
            mappers.add(new MapperBase(mapper, dataSubList));

            if (_verbose) System.out.println((end - pointer) + " values sent to mapper -> " + dataSubList);
        }
        return mappers;
    }

    private static List<Base> createReducers(HashMap<Object, List> intermediates, Reducer reducer) {
        ArrayList<Base> reducers = new ArrayList<Base>(intermediates.size());
        for (Object key : intermediates.keySet()) {
            List dataList = intermediates.get(key);
            reducers.add(new ReducerBase(reducer, key, dataList));

            if (_verbose) System.out.println(dataList.size() + " values sent to reducer -> " + dataList);
        }
        return reducers;
    }


    private static void runThreads(List<Base> tasks) throws InterruptedException {
        List<Thread> threads = new ArrayList<Thread>(tasks.size());
        for (Base task : tasks) {
            Thread thread = new Thread(task);
            threads.add(thread);
            thread.start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    private static HashMap<Object, List> getResults(List<Base> tasks) {
        HashMap<Object, List> resultMap = new HashMap<Object, List>(tasks.size() / 3);
        for (Base t : tasks) {
            for (Object key : t.results.keySet()) {
                Object value = t.results.get(key);
                List list = resultMap.get(key);
                if (list == null) {
                    list = new ArrayList();
                    resultMap.put(key, list);
                }
                list.add(value);
            }
        }

        return resultMap;
    }


    private abstract static class Base implements Runnable {
        protected HashMap results = new HashMap();
        List dataList;
    }

    private static class MapperBase extends Base implements Runnable {
        Mapper mapper;

        private MapperBase(Mapper mapper, List data) {
            this.mapper = mapper;
            this.dataList = data;
        }

        @Override
        public void run() {
            results = mapper.map(dataList);
        }
    }

    private static class ReducerBase extends Base implements Runnable {
        Reducer reducer;
        Object key;

        private ReducerBase(Reducer reducer, Object key, List data) {
            this.reducer = reducer;
            this.key = key;
            this.dataList = data;
        }

        @Override
        public void run() {
            results = reducer.reduce(key, dataList);
        }
    }

}
