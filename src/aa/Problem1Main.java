package aa;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Problem1Main {

    public static void main(String[] args) {

        List<String> wordList = readFile();
        Mapper problem1Mapper = new Problem1Mapper();
        Reducer problem1Reducer = new Problem1Reducer();
        int numShards = 1;

        // true will make the framework output (S.O.P) a bunch of text about what it is doing.
        boolean verbose = false;

        try {

            Map<Object, List> finalResults = MapReduce.mapReduce(problem1Mapper, problem1Reducer, wordList, numShards, verbose);

            for (Object word : finalResults.keySet()) {
                System.out.println(word + ", " + finalResults.get(word).get(0));
            }

        } catch (InterruptedException e) {

        }


        // Hmm MapReduce class is mainly static method, probably meant to be used in a static way.
    }

    private static List<String> readFile() {

        List<String> wordList = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader("shakespeare.txt"))) {

            String currentLine;

            while ((currentLine = br.readLine()) != null) {


                // too many length check. Whatever.
                if (currentLine.length() != 0) {

                    // should I do it here or at the mapper?
                    // remove punctuation, non letter characters and convert to lowercase
                    // frowning count should be 16 but final result is 15.
                    // should not have 275 count of empty.

                    // maybe I shouldn't use split...
                    currentLine = currentLine.replaceAll("[^-A-Za-z'\"\\s]", "").trim().toLowerCase();

                    if (currentLine.length() != 0) {
                        String[] words = currentLine.split("\\s+");


                        // Remove end quotes. but if it is like don't then keep them
                        for (String word : words) {
                            word = word
                                    .replaceAll("^[-'\"]+", "")
                                    .replaceAll("[-'\"]+$", "");

                            if (word.length() != 0) {
                                wordList.add(word);
                            }


                        }
//                        wordList.addAll(Arrays.asList(words));
                    }

                }

            }

        } catch (IOException e) {

            e.printStackTrace();
        }

//        System.out.println(wordList.size());

        // pittance, , baptista, it, likes, hmmm got empty space. still got bug. but assume it work now.
//        for (String word : wordList) {
//            System.out.println(word);
//        }
        return wordList;

    }

}
