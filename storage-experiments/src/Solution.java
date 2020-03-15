import java.util.Arrays;

class Solution {

    private final int[] mapping = new int[26];
    private int sumLeft;
    private int sumRight;
    private int maxLength;
    private final boolean[] used = new boolean[10];

    public boolean isSolvable(String[] words, String result) {
        Arrays.fill(mapping, -1);
        Arrays.fill(used, false);
        maxLength = result.length();
        sumLeft = sumRight = 0;
        for (String word : words) {
            maxLength = Math.max(maxLength, word.length());
        }
        if (result.length() < maxLength) {
            return false;
        }

        return solve(0, 0, words, result);
    }

    private boolean solve(int index, int pos, String[] words, String result) {
        System.out.println(index + "\t" + pos + "\t" + sumLeft + "\t" + sumRight);
        if (pos >= maxLength) {
            return sumLeft == sumRight;
        }
        // check index
        if (index < words.length) {
            String word = words[index];
            if (pos >= word.length()) {
                return solve(index + 1, pos, words, result);
            } else {
                char c = word.charAt(word.length() - pos - 1);
                if (mapping[c - 'A'] != -1) {
                    // already mapped
                    sumLeft += (mapping[c - 'A'] * (int) Math.pow(10, pos));
                    if (solve(index + 1, pos, words, result)) {
                        return true;
                    } else {
                        sumLeft -= (mapping[c - 'A'] * (int) Math.pow(10, pos));
                        return false;
                    }
                } else {
                    for (int i = 0; i <= 9; i++) {
                        if (!used[i]) {
                            used[i] = true;
                            mapping[c - 'A'] = i;
                            sumLeft += (i * (int) Math.pow(10, pos));
                            if (solve(index + 1, pos, words, result)) {
                                return true;
                            }
                            mapping[c - 'A'] = -1;
                            used[i] = false;
                            sumLeft -= (i * (int) Math.pow(10, pos));
                        }
                    }
                }
            }
        } else {
            // check the result
            char c = result.charAt(result.length() - pos - 1);
            if (mapping[c - 'A'] != -1) {
                sumRight += (mapping[c - 'A'] * (int) Math.pow(10, pos));
                if ((sumLeft % (int) Math.pow(10, pos + 1)) == sumRight && solve(0, pos + 1, words, result)) {
                    return true;
                } else {
                    sumRight -= (mapping[c - 'A'] * (int) Math.pow(10, pos));
                    return false;
                }
            } else {
                int start = (pos == result.length() - 1) ? 1 : 0;
                for (int i = start; i <= 9; i++) {
                    if (!used[i]) {
                        used[i] = true;
                        mapping[c - 'A'] = i;
                        sumRight += (i * (int) Math.pow(10, pos));
                        if ((sumLeft % (int) Math.pow(10, pos + 1)) == sumRight && solve(0, pos + 1, words, result)) {
                            return true;
                        }
                        mapping[c - 'A'] = -1;
                        used[i] = false;
                        sumRight -= (i * (int) Math.pow(10, pos));
                    }
                }
            }
        }
        return false;
    }

    public static void main(String[] args) {
        System.out.println(new Solution().isSolvable(new String[] { "AB", "CD", "EF" }, "GHJI"));
    }
}