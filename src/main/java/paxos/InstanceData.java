package paxos;

import java.util.ArrayList;

public class InstanceData {

    public record State(int estballot, int est) {}

    public InstanceData(int ID, int N) {
        this.ballot = ID - N;
        this.impose_ballot = ID - N;
        this.states = new ArrayList<>(N - 1);
        for (int i = 0; i < N; i++) {
            states.add(null);
        }
    }

    public int ballot;
    public int impose_ballot;
    public ArrayList<State> states;
    public int proposal = -1;
    public int read_ballot = 0;
    public int estimate = -1;
    public int ack_count = 0;

    public int statesCount() {
        int count = 0;
        for (State state : states) {
            if (state != null) {
                count++;
            }
        }
        return count;
    }

    public State highestState() {
        State highest = null;
        for (State state : states) {
            if (state == null) {
                continue;
            }
            if (highest == null || state.estballot() > highest.estballot()) {
                highest = state;
            }
        }
        return highest;
    }

    public void resetStates() {
        for (int i = 0; i < states.size(); i++) {
            states.set(i, null);
        }
    }
}
