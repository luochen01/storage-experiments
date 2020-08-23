import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ThreadLocalRandom;

class Size {
    private final int height;
    private final int width;
    private final int length;

    public Size(int height, int width, int length) {
        this.height = height;
        this.width = width;
        this.length = length;
    }

    public int getHeight() {
        return height;
    }

    public int getWidth() {
        return width;
    }

    public int getLength() {
        return length;
    }

    public boolean contains(Size other) {
        return height >= other.height && width >= other.width && length >= other.length;
    }
}

enum LockerType {
    SMALL,
    MEDIUM,
    LARGE,
    XLARGE
}

class LockerSpec {
    private final LockerType type;
    private final Size size;

    public static final LockerSpec LOCKER_SMALL = new LockerSpec(LockerType.SMALL, new Size(1, 1, 1));
    public static final LockerSpec LOCKER_MEDIUM = new LockerSpec(LockerType.MEDIUM, new Size(2, 2, 2));
    public static final LockerSpec LOCKER_LARGE = new LockerSpec(LockerType.LARGE, new Size(3, 3, 3));
    public static final LockerSpec LOCKER_XLARGE = new LockerSpec(LockerType.XLARGE, new Size(4, 4, 4));

    public static final LockerSpec[] LOCKER_ALL = { LOCKER_SMALL, LOCKER_MEDIUM, LOCKER_LARGE, LOCKER_XLARGE };

    private LockerSpec(LockerType type, Size size) {
        this.type = type;
        this.size = size;
    }

    public Size getSize() {
        return size;
    }

    public LockerType getType() {
        return type;
    }
}

class Item {
    private final Size size;

    public Item(Size size) {
        this.size = size;
    }

    public Size getSize() {
        return size;
    }
}

class Locker {
    public enum LockerState {
        FREE,
        USED
    }

    private final int positionX;
    private final int positionY;
    private final LockerSpec spec;
    private LockerState state;
    private Item item;

    public Locker(LockerSpec spec, int positionX, int positionY) {
        this.spec = spec;
        this.positionX = positionX;
        this.positionY = positionY;
        this.state = LockerState.FREE;
        this.item = null;
    }

    public void addItem(Item item) {
        if (state != LockerState.FREE || item != null) {
            throw new IllegalStateException("Illegal state of locker. State :" + state + " item: " + item);
        }
        this.state = LockerState.USED;
        this.item = item;
    }

    public Item takeItem() {
        if (state != LockerState.USED || item == null) {
            throw new IllegalStateException();
        }
        this.state = LockerState.FREE;
        Item result = this.item;
        this.item = null;
        return result;
    }

    public LockerSpec getSpec() {
        return spec;
    }
}

class LockerManager {

    private final List<Locker> allLockers = new ArrayList<>();
    private final Map<LockerType, Queue<Locker>> freeLockers = new HashMap<>();
    private final Map<String, Locker> usedLockers = new HashMap<>();

    public LockerManager() {
        // initialize all lockers
    }

    /**
     *
     * @param item:
     *            the item to be stored in the locker
     * @return a unique barcode that identifies the locker
     */
    public String depositeItem(Item item) {
        for (LockerSpec spec : LockerSpec.LOCKER_ALL) {
            if (spec.getSize().contains(item.getSize())) {
                Queue<Locker> queue = freeLockers.get(spec.getType());
                if (!queue.isEmpty()) {
                    Locker locker = queue.poll();
                    String barcode = generateBarcode(locker);
                    locker.addItem(item);
                    usedLockers.put(barcode, locker);
                    return barcode;
                }
            }
        }
        // cannot find a suitable locker to store this item
        return null;
    }

    private String generateBarcode(Locker locker) {
        while (true) {
            String num = String.valueOf(ThreadLocalRandom.current().nextInt(999999 + 1));
            if (!usedLockers.containsKey(num)) {
                return num;
            }
        }
    }

    public Item takeItem(String barcode) {
        Locker locker = usedLockers.get(barcode);
        if (locker == null) {
            return null;
        }
        Item item = locker.takeItem();
        freeLockers.get(locker.getSpec().getType()).add(locker);
        return item;
    }

}
