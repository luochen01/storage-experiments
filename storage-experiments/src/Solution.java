import java.util.Iterator;

class Node {
    final int val;

    Node left;
    Node right;
    Node parent;

    public Node(int val) {
        this.val = val;
    }

    public void setLeft(Node left) {
        this.left = left;
        left.parent = this;
    }

    public void setRight(Node right) {
        this.right = right;
        right.parent = this;
    }

}

class TreeIterator implements Iterator<Integer> {

    private Node node;
    private Integer next = null;

    public TreeIterator(Node node) {
        this.node = getFirstChild(node);
    }

    private Node getFirstChild(Node node) {
        while (node.left != null || node.right != null) {
            if (node.left != null) {
                node = node.left;
            } else {
                node = node.right;
            }
        }
        return node;
    }

    @Override
    public boolean hasNext() {
        if (next == null) {
            doGetNext();
        }
        return next != null;
    }

    @Override
    public Integer next() {
        if (next == null) {
            doGetNext();
        }
        return next;
    }

    private void doGetNext() {
        if (node != null) {
            next = node.val;
            Node parent = node.parent;
            if (parent != null) {
                if (parent.left == node && parent.right != null) {
                    node = parent.right;
                    while (node.left != null || node.right != null) {
                        if (node.left != null) {
                            node = node.left;
                        } else {
                            node = getFirstChild(node.right);
                        }
                    }
                } else {
                    node = parent;
                }
            } else {
                node = null;
            }
        }
    }

}

class Solution {

    public static void main(String[] args) {
        Node root = new Node(1);
        Node left = new Node(2);
        root.setLeft(left);
        Node leftLeft = new Node(3);

    }
}