import java.util.concurrent.atomic.AtomicReference;

class Node {
    final int key;
    int value;

    AtomicReference<Node> next = new AtomicReference<>();

    public Node(int key, int value) {
        this.key = key;
        this.value = value;
    }
}

class HashMap {
    private static final int INIT_SIZE = 10;

    private AtomicReference[] nodes;
    private int count;

    public HashMap() {
        this.nodes = new AtomicReference[INIT_SIZE];

        for (int i = 0; i < INIT_SIZE; i++) {
            this.nodes[i] = new AtomicReference<Node>();
        }
    }

    public void put(int key, int value) {
        int hash = hash(nodes, key);
        Node newNode = null;
        while (true) {
            Node oldHead = (Node) nodes[hash].get();
            Node node = oldHead;
            while (node != null) {
                if (node.key == key) {
                    node.value = value;
                    return;
                } else {
                    node = node.next.get();
                }
            }
            if (newNode == null) {
                newNode = new Node(key, value);
            }
            newNode.next.set(oldHead);
            // if CAS fails, the bucket has been modified by concurrent writers.
            // Must retry the search to avoid duplicate keys
            if (nodes[hash].compareAndSet(oldHead, newNode)) {
                count++;
                break;
            }
        }
    }

    private Node doGet(int key) {
        int hash = hash(nodes, key);
        Node node = (Node) nodes[hash].get();
        while (node != null) {
            if (node.key == key) {
                return node;
            } else {
                node = node.next.get();
            }
        }
        return null;
    }

    private int hash(AtomicReference[] nodes, int key) {
        return Math.abs(key) % nodes.length;
    }

    public int get(int key) {
        Node node = doGet(key);
        if (node != null) {
            return node.value;
        } else {
            return -1;
        }
    }

}