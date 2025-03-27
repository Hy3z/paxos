package paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

import java.time.Duration;
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
    private long startTime;
    private int instanceCounter = 0;
    private boolean onHold = false;

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
        for (int i = 0; i < N; i++) {
            states.add(new State(-1, 0)); // initializing the states
        }
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
                .match(Paxos.HoldMessage.class, this::onHold)
            .build();
    }

    private void onActorList(Paxos.ActorListMessage message) {
        myNeighbors = message.actors();
        reportActor = message.report_actor();
    }

    private void propose(Message_PROPOSE message) {
        decideIfCrash();
        if (!isCrashed) {
            // TODO : Timer ?
            this.startTime = System.nanoTime();
            logger.info("Proposing: {}", message.proposal);
            this.proposal = message.proposal; // setting the proposal
            this.ballot += N; // incrementing the ballot by N
            for (State state : states) {
                state = new State(-1, 0); // resetting the states
            }
            for (ActorRef neighbor : myNeighbors) {
                neighbor.tell(new Message_READ(ballot, ID), getSelf());
            }
        }
    }

    private void onRead(Message_READ message) {
        decideIfCrash();
        if (!isCrashed) {
            logger.info("Received read message: {}", message);
            if (readBallot > message.ballot || imposeBallot > message.ballot) {
                getSender().tell(new Message_ABORT(message.ballot, ID), getSelf());
                logger.info("Aborting instance no: {}", instanceCounter);
                if (!onHold) {
                    proposing();
                }
            } else {
                readBallot = message.ballot;
                getSender().tell(new Message_GATHER(message.ballot, imposeBallot, estimate, ID), getSelf());
            }
        }
    }

    private void onAbort(Message_ABORT message) {
        decideIfCrash();
        if (!isCrashed) {
            logger.info("Received abort message: {}, aborting instance no {}", message, instanceCounter);
            if (!onHold) {
                proposing();
            }
        }
    }

    private void onGather(Message_GATHER message) {
        decideIfCrash();
        if (!isCrashed) {
            states.set(message.ID, new State(message.imposeBallot, message.estimate));
            logger.info("Setting state for ID: {}", message.ID);
            gatherCount++;
            if (gatherCount >= (int) N / 2) {
                logger.info("Gathered enough states, proceeding to impose.");
                int maxBallot = Integer.MIN_VALUE;
                int maxEstimate = -1;
                for (State state : states) {
                    if (state.ballot > maxBallot) {
                        maxBallot = state.ballot;
                        maxEstimate = state.estimate;
                    }
                }
                if (maxBallot > 0) {
                    proposal = maxEstimate;
                }
                for (State state : states) {
                    state = new State(-1, 0); // resetting the states
                }
                for (ActorRef neighbor : myNeighbors) {
                    neighbor.tell(new Message_IMPOSE(ballot, proposal, ID), getSelf());
                }
            }
        }
    }

    private void onImpose(Message_IMPOSE message) {
        decideIfCrash();
        if (!isCrashed) {
            if (readBallot > message.ballot || imposeBallot > message.ballot) {
                getSender().tell(new Message_ABORT(message.ballot, ID), getSelf());
                logger.info("Aborting instance no: {}", instanceCounter);
                proposing();
            } else {
                imposeBallot = message.ballot;
                estimate = message.proposal;
                getSender().tell(new Message_ACK(message.ballot, ID), getSelf());
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
                long endTime = System.nanoTime();
                reportActor.tell(new Paxos.DecidedMessage((endTime - startTime), instanceCounter), getSelf());
                logger.info("Instance no: {} terminated with a decided value: {}.", instanceCounter, proposal);
                //if (!onHold) {
                //    proposing();
                //}
            }
        }
    }

    private void onDecide(Message_DECIDE message) {
        decideIfCrash();
        if (!isCrashed) {
            long endTime = System.nanoTime();
            reportActor.tell(new Paxos.DecidedMessage((endTime - startTime), instanceCounter), getSelf());
            logger.info("Instance no: {} terminated with a decided value: {}.", instanceCounter, message.value);
            //if (!onHold) {
            //    proposing();
            //}
        }
    }

    private void onCrash(Paxos.CrashMessage message) {
        logger.info("Received crash message: {}", message);
        this.crash_prob = message.alpha();
    }

    private void onLaunch(Paxos.LaunchMessage message) throws InterruptedException{
        proposing();
    }

    private void onHold(Paxos.HoldMessage message) {
        onHold = true;
        logger.info("On hold !");
    }

    private void proposing() {
        instanceCounter++;
        ackCount = 0;
        gatherCount = 0;
        logger.info("Launching new instance: {}", instanceCounter);
        int propose = (int) (Math.random() * 2);
        if (propose == 0) {
            propose(new Message_PROPOSE(0, ID));
        } else {
            propose(new Message_PROPOSE(1, ID));
        }
    }

    private void decideIfCrash() {
        float rand = (float) Math.random();
        if (rand < crash_prob && !isCrashed) {
            isCrashed = true;
            logger.info("Crashing !");
        }
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

}
