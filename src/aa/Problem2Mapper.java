package aa;

import java.util.HashMap;
import java.util.List;

public class Problem2Mapper implements Mapper {

    // this is for the string[] in foodReviewList
    // It's all final so no worries about side effects!
    private static final int PRODUCT_ID = 0;
    private static final int USER_ID = 1;
    private static final int PROFILE_NAME = 2;
    private static final int HELPFULNESS = 3;
    private static final int SCORE = 4;
    private static final int TIME = 5;
    private static final int SUMMARY = 6;
    private static final int TEXT = 7;

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
        List<String[]> foodReviewList = (List<String[]>) list;

        // prof interface returns HashMap so use HashMap instead of Map
        HashMap<String, Double> foodReviewMap = new HashMap<>();

        for (String[] foodReview : foodReviewList) {

            if (wordExistInReview(Problem2Main.WORD_TO_CHECK, foodReview)) {

                // 1. Count reviews with word cofee in it
                Double foodReviewWithWordCount = foodReviewMap.get("REVIEW_WITH_WORD_COUNT");

                if (foodReviewWithWordCount != null) {
                    foodReviewWithWordCount++;
                } else {
                    foodReviewMap.put("REVIEW_WITH_WORD_COUNT", 1.0);
                }

                // accumulate the score for later average calculation

                double score = Double.parseDouble(foodReview[SCORE]);

                Double foodReviewWithWordTotalScore = foodReviewMap.get("REVIEW_WITH_WORD_TOTAL_SCORE");

                if (foodReviewWithWordTotalScore != null) {
                    foodReviewWithWordTotalScore += score;
                } else {
                    foodReviewMap.put("REVIEW_WITH_WORD_TOTAL_SCORE", score);
                }

            }


        }

        return foodReviewMap;

    }

    private boolean wordExistInReview(String wordToCheck, String[] foodReview) {

        // assumption any where in the review coffee appear, even profile name, it is counted.
        for (String field : foodReview) {
            if (field.toLowerCase().contains(wordToCheck)) {
                return true;
            }
        }

        return false;
    }
}
