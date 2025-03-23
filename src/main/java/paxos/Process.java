package paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.util.ArrayList;
import java.util.List;

public class Process extends AbstractActor {

    private final int ID;
    private final int N;
    private int ballot;
    private int readBallot;
    private int proposal;
    private int imposeBallot;
    private int estimate;
    private List<State> states;
    private float crash_prob;
    private List<ActorRef> myNeighbors;
    private ActorRef reportActor;
    private boolean isCrashed;
    private int ackCount = 0;
    private int gatherCount = 0;

    public static Props props(int ID, int N) {
        return Props.create(Process.class, ID, N);
    }

    private final LoggingAdapter logger = Logging.getLogger(
        getContext().getSystem(),
        this
    );

    public Process(int ID, int N) {
        this.ID = ID;
        this.N = N;
        this.ballot = ID - N;
        this.imposeBallot = ID - N;
        this.estimate = -1;
        this.proposal = -1;
        this.states = new ArrayList<State>(N);
        this.myNeighbors = new ArrayList<ActorRef>(N - 1);
        this.readBallot = 0;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(Paxos.ActorListMessage.class, this::onActorList)
            .match(Message_GATHER.class, this::onGather)
            .match(Message_ACK.class, this::onAccept)
            .match(Message_READ.class, this::onRead)
            .match(Message_IMPOSE.class, this::onImpose)
            .match(Message_DECIDE.class, this::onDecide)
            .match(Paxos.CrashMessage.class, this::onCrash)
            .match(Message_ABORT.class, this::onAbort)
            .match(Paxos.LaunchMessage.class, this::onLaunch)
            //hold message missing
            .build();
    }

    private void onActorList(Paxos.ActorListMessage message) {
        myNeighbors = message.actors();
        reportActor = message.report_actor();
    }

    private void propose(Message_PROPOSE message) {
        decideIfCrash();
        if (!isCrashed) {
            logger.info("Proposing: {}", message.proposal);
            this.proposal = message.proposal; // setting the proposal
            this.ballot += N; // incrementing the ballot by N
            for (State state : states) {
                state = new State(-1, 0); // initializing the states
            }
            for (ActorRef neighbor : myNeighbors) {
                neighbor.tell(new Message_READ(ballot, ID), getSelf());
            }
        }
    }

    private void onRead(Message_READ message) {
        decideIfCrash();
        if (!isCrashed) {
            if (readBallot > message.ballot || imposeBallot > message.ballot) {
                getSender()
                    .tell(new Message_ABORT(message.ballot, ID), getSelf());
            } else {
                readBallot = message.ballot;
                getSender()
                    .tell(
                        new Message_GATHER(
                            message.ballot,
                            imposeBallot,
                            estimate,
                            ID
                        ),
                        getSelf()
                    );
            }
        }
    }

    private String onAbort(Message_ABORT message) {
        decideIfCrash();
        logger.info("Received abort message: {}", message);
        return "abort";
    }

    private void onGather(Message_GATHER message) {
        decideIfCrash();
        if (!isCrashed) {
            states.set(
                message.ID,
                new State(message.imposeBallot, message.estimate)
            );
            gatherCount++;
            if (gatherCount > (int) N / 2) {
                int maxBallot = Integer.MIN_VALUE;
                int maxEstimate = -1;
                int index = 0;
                for (State state : states) {
                    if (state.ballot > maxBallot) {
                        maxBallot = state.ballot;
                        maxEstimate = state.estimate;
                        index = states.indexOf(state);
                    }
                }
                if (maxBallot > 0) {
                    proposal = maxEstimate;
                }
                for (ActorRef neighbor : myNeighbors) {
                    neighbor.tell(
                        new Message_IMPOSE(ballot, proposal, ID),
                        getSelf()
                    );
                }
            }
        }
    }

    private void onImpose(Message_IMPOSE message) {
        decideIfCrash();
        if (!isCrashed) {
            if (readBallot > message.ballot || imposeBallot > message.ballot) {
                getSender()
                    .tell(new Message_ABORT(message.ballot, ID), getSelf());
            } else {
                imposeBallot = message.ballot;
                estimate = message.proposal;
                getSender()
                    .tell(new Message_ACK(message.ballot, ID), getSelf());
            }
        }
    }

    private void onAccept(Message_ACK message) {
        decideIfCrash();
        if (!isCrashed) {
            ackCount++;
            if (ackCount > (int) N / 2) {
                for (ActorRef neighbor : myNeighbors) {
                    neighbor.tell(new Message_DECIDE(proposal, ID), getSelf());
                }
                ackCount = 0;
            }
        }
    }

    private int onDecide(Message_DECIDE message) {
        decideIfCrash();
        for (ActorRef neighbor : myNeighbors) {
            neighbor.tell(new Message_DECIDE(message.value, ID), getSelf());
        }
        gatherCount = 0;
        reportActor.tell(new Paxos.ReportMessage(), getSelf());
        logger.info("Decided: {}", message.value);
        return message.value;
    }

    private void onCrash(Paxos.CrashMessage message) {
        logger.info("Received crash message: {}", message);
        this.crash_prob = message.alpha();
    }

    private void onLaunch(Paxos.LaunchMessage message) {
        int propose = (int) (Math.random() * 2);
        if (propose == 0) {
            propose(new Message_PROPOSE(0, ID));
        } else {
            propose(new Message_PROPOSE(1, ID));
        }
    }

    private void decideIfCrash() {
        float rand = (float) Math.random();
        if (rand < crash_prob) {
            isCrashed = true;
        }
    }

    public void setBallot(int ballot) {
        this.ballot = ballot;
    }

    public int getBallot() {
        return this.ballot;
    }

    public void setReadBallot(int readBallot) {
        this.readBallot = readBallot;
    }

    public int getReadBallot() {
        return this.readBallot;
    }

    public void setProposal(int proposal) {
        this.proposal = proposal;
    }

    public int getProposal() {
        return this.proposal;
    }

    public void setImposeBallot(int imposeBallot) {
        this.imposeBallot = imposeBallot;
    }

    public int getImposeBallot() {
        return this.imposeBallot;
    }

    public void setEstimate(int estimate) {
        this.estimate = estimate;
    }

    public int getEstimate() {
        return this.estimate;
    }

    public int getID() {
        return this.ID;
    }

    // a record to build the States list
    public record State(int ballot, int estimate) {}

    public record Message_READ(int ballot, int ID) {}

    // ballot can be negative
    public record Message_ABORT(int ballot, int ID) {}

    // same for these ballot et imposeballot
    public record Message_GATHER(
        int ballot,
        int imposeBallot,
        int estimate,
        int ID
    ) {}

    public record Message_ACK(int ballot, int ID) {}

    public record Message_IMPOSE(int ballot, int proposal, int ID) {}

    public record Message_PROPOSE(int proposal, int ID) {}

    // value is either 0 or 1
    public record Message_DECIDE(int value, int ID) {}
}
