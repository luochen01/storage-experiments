package edu.uci.asterixdb.storage.experiments;

import java.util.Stack;

class TreeNode {
    final int val;

    TreeNode left;
    TreeNode right;

    public TreeNode(int val) {
        this.val = val;
    }
}

class TreeIterator {
    private final Stack<TreeNode> stack = new Stack<>();
    private final Stack<Integer> visitedCount = new Stack<>();

    private Integer next;

    public TreeIterator(TreeNode root) {
        if (root != null) {
            stack.add(root);
            visitedCount.add(0);
        }
    }

    public Integer getNext() {
        if (next == null) {
            doGetNext();
        }
        Integer value = next;
        next = null;
        return value;
    }

    public boolean hasNext() {
        if (next == null) {
            doGetNext();
        }
        return next != null;
    }

    private void doGetNext() {
        while (!stack.isEmpty()) {
            TreeNode node = stack.pop();
            int count = visitedCount.pop();
            if (node == null) {
                continue;
            }
            if (count == 2) {
                next = node.val;
                break;
            } else if (count == 0) {
                stack.push(node);
                visitedCount.push(1);
                stack.push(node.left);
                visitedCount.push(0);
            } else if (count == 1) {
                stack.push(node);
                visitedCount.push(2);
                stack.push(node.right);
                visitedCount.push(0);
            }
        }
    }

}

class Solution {

    public static void main(String[] args) {
        TreeNode root = new TreeNode(0);
        root.left = new TreeNode(1);
        root.left.left = new TreeNode(2);
        root.left.right = new TreeNode(3);

        root.right = new TreeNode(4);

        TreeIterator it = new TreeIterator(root);
        while (it.hasNext()) {
            System.out.println(it.getNext());
        }
    }

}