package aa;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Problem2Main {

    public static final String WORD_TO_CHECK = "coffee";
    // this is for the string[] in foodReviewList
    private static final int PRODUCT_ID = 0;
    private static final int USER_ID = 1;
    private static final int PROFILE_NAME = 2;
    private static final int HELPFULNESS = 3;
    private static final int SCORE = 4;
    private static final int TIME = 5;
    private static final int SUMMARY = 6;
    private static final int TEXT = 7;

    public static void main(String[] args) {

        List<String[]> foodReviewList = readFile();

        Mapper problem2Mapper = new Problem2Mapper();
        Reducer problem2Reducer = new Problem2Reducer();
        int numShards = 8;

        // true will make the framework output (S.O.P) a bunch of text about what it is doing.
        boolean verbose = false;

        try {

            Map<Object, List> finalResults = MapReduce.mapReduce(problem2Mapper, problem2Reducer, foodReviewList, numShards, verbose);

            double reviewWithWordCount = (double) finalResults.get("REVIEW_WITH_WORD_COUNT").get(0);
            double reviewWithWordTotalScore = (double) finalResults.get("REVIEW_WITH_WORD_TOTAL_SCORE").get(0);
            double reviewWithWordAvgScore = reviewWithWordTotalScore / reviewWithWordCount;

            System.out.println("Review with word '" + Problem2Main.WORD_TO_CHECK + "' count is " + reviewWithWordCount);
            System.out.println("Review with word '" + Problem2Main.WORD_TO_CHECK + "' average score is " + reviewWithWordAvgScore);

        } catch (InterruptedException e) {

        }

    }

    private static List<String[]> readFile() {

        /*
            product/productId: B0026Y3YBK
            review/userId: A38BUM0OXH38VK
            review/profileName: singlewinder
            review/helpfulness: 0/0
            review/score: 5.0
            review/time: 1347667200
            review/summary: Best everyday cookie!
            review/text: In the 1980s I spent several
            product/productId: B0026Y3YSS
            ...
         */

        List<String[]> foodReviewList = new ArrayList<>(568454);

        // Hmmm
        try (BufferedReader br = new BufferedReader(new FileReader("foods.txt"))) {

            String currentLine;

            while ((currentLine = br.readLine()) != null) {

                String[] row = new String[8];

                row[PRODUCT_ID] = currentLine.replace("product/productId: ", "");

                currentLine = br.readLine();
                row[USER_ID] = currentLine.replace("review/userId: ", "");

                currentLine = br.readLine();
                row[PROFILE_NAME] = currentLine.replace("review/profileName: ", "");

                currentLine = br.readLine();
                row[HELPFULNESS] = currentLine.replace("review/helpfulness: ", "");

                currentLine = br.readLine();
                row[SCORE] = currentLine.replace("review/score: ", "");

                currentLine = br.readLine();
                row[TIME] = currentLine.replace("review/time: ", "");

                currentLine = br.readLine();
                row[SUMMARY] = currentLine.replace("review/summary: ", "");

                currentLine = br.readLine();
                row[TEXT] = currentLine.replace("review/text: ", "");

                // read the empty line
                br.readLine();

                foodReviewList.add(row);

            }

        } catch (IOException e) {

            e.printStackTrace();
        }

        return foodReviewList;

    }
}
