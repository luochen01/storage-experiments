import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class Pair {
    final int num;
    final int index;

    public Pair(int num, int index) {
        this.num = num;
        this.index = index;
    }

    @Override
    public String toString() {
        return num + "@" + index;
    }
}

class Solution {

    public int longestSubarray(int[] nums, int k) {
        //        TreeSet<Pair> set = new TreeSet<>((p1, p2) -> {
        //            int cmp = Integer.compare(p1.num, p2.num);
        //            if (cmp != 0) {
        //                return cmp;
        //            } else {
        //                return Integer.compare(p1.index, p2.index);
        //            }
        //        });
        List<Pair> list = new ArrayList<>();
        int max = 1;
        int sum = 0;
        for (int i = 0; i < nums.length; i++) {
            sum += nums[i];
            if (sum <= k) {
                max = Math.max(max, i + 1);
            }
        }
        int min = Integer.MAX_VALUE;
        for (int i = nums.length - 1; i >= 0; i--) {
            if (sum < min) {
                list.add(new Pair(sum, i));
                min = sum;
            }
            sum -= nums[i];
        }
        Collections.reverse(list);
        int right = 0;
        int maxIndex = 0;
        for (int i = 0; i < nums.length; i++) {
            sum += nums[i];
            while (right < list.size() && list.get(right).num - sum <= k) {
                maxIndex = list.get(right).index;
                right++;
            }
            max = Math.max(max, maxIndex - i);
        }

        return max;
    }

    public static void main(String[] args) {
        int[] array = new int[] { 2000, 10, 2000, 10, -100, 200, -1000 };
        System.out.println(new Solution().longestSubarray(array, 20));
    }
}