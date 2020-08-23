import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

enum VehicleType {
    CAR,
    MOTOCYCLE,
    BUS
}

abstract class Vehicle {
    private final String license;

    public Vehicle(String license) {
        this.license = license;
    }

    public abstract VehicleType getType();

    public abstract boolean canPark(ParkingSpot spot);
}

class Bus extends Vehicle {
    public Bus(String license) {
        super(license);
    }

    @Override
    public VehicleType getType() {
        return VehicleType.BUS;
    }

    @Override
    public boolean canPark(ParkingSpot spot) {
        return spot.getType() == SpotType.LARGE;
    }
}

class Car extends Vehicle {
    public Car(String license) {
        super(license);
    }

    @Override
    public VehicleType getType() {
        return VehicleType.CAR;
    }

    @Override
    public boolean canPark(ParkingSpot spot) {
        return spot.getType() == SpotType.MEDIUM || spot.getType() == SpotType.LARGE;
    }
}

class Motorcycle extends Vehicle {
    public Motorcycle(String license) {
        super(license);
    }

    @Override
    public VehicleType getType() {
        return VehicleType.MOTOCYCLE;
    }

    @Override
    public boolean canPark(ParkingSpot spot) {
        return spot.getType() == SpotType.SMALL || spot.getType() == SpotType.MEDIUM;
    }
}

enum SpotType {
    SMALL,
    MEDIUM,
    LARGE
}

class ParkingSpot {
    private final int floor;
    private final int row;
    private final int col;
    private final SpotType type;

    private Vehicle vehicle;
    private long parkTime;

    public SpotType getType() {
        return type;
    }

    public ParkingSpot(int floor, int row, int col, SpotType type) {
        this.floor = floor;
        this.row = row;
        this.col = col;
        this.type = type;
    }

    public void park(Vehicle vehicle) {
        if (this.vehicle != null || !vehicle.canPark(this)) {
            throw new IllegalStateException();
        }
        this.vehicle = vehicle;
        // get current time
        this.parkTime = System.currentTimeMillis();
    }

    public long leave() {
        if (this.vehicle == null) {
            throw new IllegalStateException();
        }
        this.vehicle = null;
        long duration = System.currentTimeMillis() - parkTime;
        this.parkTime = 0;
        return duration;
    }

    public int getFloor() {
        return floor;
    }

    public int getRow() {
        return row;
    }

    public int getCol() {
        return col;
    }
}

interface ParkingEventListener {
    void onPark(ParkingSpot spot, Vehicle vehicle);

    void onLeave(ParkingSpot spot);

    void onOverStay(ParkingSpot spot);
}

class DispalyBoard implements ParkingEventListener {

    private static final int FREE = 0;
    private static final int USED = 1;
    private static final int OVER_STAY = 2;

    // floor, row, col
    private int[][][] board = new int[10][100][100];

    @Override
    public synchronized void onPark(ParkingSpot spot, Vehicle vehicle) {
        board[spot.getFloor()][spot.getRow()][spot.getCol()] = USED;
    }

    @Override
    public synchronized void onLeave(ParkingSpot spot) {
        board[spot.getFloor()][spot.getRow()][spot.getCol()] = FREE;
    }

    @Override
    public synchronized void onOverStay(ParkingSpot spot) {
        board[spot.getFloor()][spot.getRow()][spot.getCol()] = OVER_STAY;
    }

}

class ParkingLot {

    private final List<List<List<ParkingSpot>>> parkingSpots = new ArrayList<>();

    private final Set<ParkingEventListener> listeners = new HashSet<>();

    private final Map<ParkingSpot, ParkingTask> taskMap = new HashMap<>();

    private final Timer timer = new Timer();

    class ParkingTask extends TimerTask {

        private final ParkingSpot spot;
        private boolean isCompleted = false;

        public ParkingTask(ParkingSpot spot) {
            this.spot = spot;
        }

        @Override
        public boolean cancel() {
            boolean result = super.cancel();
            setCompletion();
            return result;
        }

        @Override
        public void run() {
            synchronized (listeners) {
                for (ParkingEventListener listener : listeners) {
                    listener.onOverStay(spot);
                }
            }
            setCompletion();
        }

        public void waitForCompletion() throws InterruptedException {
            synchronized (this) {
                while (!isCompleted) {
                    this.wait();
                }
            }
        }

        public void setCompletion() {
            synchronized (this) {
                isCompleted = true;
                this.notifyAll();
            }
        }

    }

    public void register(ParkingEventListener listener) {
        synchronized (listeners) {
            this.listeners.add(listener);
        }
    }

    public void unregister(ParkingEventListener listener) {
        synchronized (listeners) {
            this.listeners.remove(listener);
        }
    }

    public void park(ParkingSpot spot, Vehicle vehicle) {
        spot.park(vehicle);
        for (ParkingEventListener listener : listeners) {
            listener.onPark(spot, vehicle);
        }
        ParkingTask task = new ParkingTask(spot);
        taskMap.put(spot, task);
        timer.schedule(task, TimeUnit.DAYS.toMillis(1));

    }

    public void leave(ParkingSpot spot) {
        long duration = spot.leave();
        ParkingTask task = taskMap.remove(spot);
        task.cancel();
        try {
            task.waitForCompletion();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        synchronized (listeners) {
            // calculate prices
            for (ParkingEventListener listener : listeners) {
                listener.onLeave(spot);
            }
        }
    }

}
