package paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Process extends AbstractActor {

    private static final int TIMEOUT = 250;
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
    //private long startTime;
    private int instanceCounter = 0;
    private boolean onHold = false;
    private boolean decided = false;
    private final ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(1);

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
        myNeighbors = message.actors(); // Receive message from the Paxos actor, setting the list of the other actors to send messages to
        reportActor = message.report_actor(); // And setting up the reference to the Paxos actor to send him the time taken to decide on a value
    }

    private void propose(Message_PROPOSE message) {
        decideIfCrash();
        if (!isCrashed && !decided) { // For every action in the algorithm, check if the process is correct and has not yet decided
            /*getContext()
                .system()
                .scheduler()
                .scheduleOnce(
                    Duration.ofMillis(1000),
                    getSelf(),
                    "Waiting 1s for next instance.",
                    getContext().system().dispatcher(),
                    ActorRef.noSender()
                );*/
            //this.startTime = System.nanoTime(); // Time is measured up in nanoseconds since it usually takes less than a millisecond with N = 3
            /*logger.info(
                "{}. Instance {}, proposing: {}.",
                this.ID,
                instanceCounter,
                message.proposal
                );*/
            this.proposal = message.proposal; // setting the proposal
            this.ballot += N; // incrementing the ballot by N
            for (State state : states) {
                state = new State(-1, 0); // resetting the states
            }
            for (ActorRef neighbor : myNeighbors) { // Sending a READ message to all, as for evey message sent, we send our ID and the instance number we are in
                neighbor.tell(
                    new Message_READ(ballot, ID, instanceCounter),
                    getSelf()
                );
            }
        }
    }

    private void onRead(Message_READ message) {
        decideIfCrash();
        if (!isCrashed && !decided) {
            /*logger.info(
                "{}. Instance {}. Received READ from {},",
                this.ID,
                message.instanceNumber,
                message.ID
                );*/
            if (readBallot > message.ballot || imposeBallot > message.ballot) { // Condition from the algo if not met, we abort the current instance
                getSender()
                    .tell(
                        new Message_ABORT(message.ballot, ID, instanceCounter),
                        getSelf()
                    ); // Sending ABORT to all
                /*logger.info(
                    "{}. Instance {} : ABORT",
                    this.ID,
                    instanceCounter
                    );*/
                if (!onHold) {
                    // When aborting we start a new instance (if not put on hold by the Paxos actor), so on and so forth until decided
                    scheduler.schedule(
                        () -> proposing(),
                        TIMEOUT,
                        TimeUnit.MILLISECONDS
                    );
                }
            } else {
                readBallot = message.ballot; // Sending GATHER to all and updating our readballot
                getSender()
                    .tell(
                        new Message_GATHER(
                            message.ballot,
                            imposeBallot,
                            estimate,
                            ID,
                            instanceCounter
                        ),
                        getSelf()
                    );
            }
        }
    }

    private void onAbort(Message_ABORT message) {
        decideIfCrash();
        if (!isCrashed && !decided) { // Received an ABORT message, we abort the corresponding instance
            /*logger.info(
                "{}. Received ABORT from actor {}, aborting instance {}",
                this.ID,
                message.ID,
                message.instanceNumber
                );*/
            if (!onHold) {
                scheduler.schedule(
                    () -> proposing(),
                    TIMEOUT,
                    TimeUnit.MILLISECONDS
                );
            }
        }
    }

    private void onGather(Message_GATHER message) {
        decideIfCrash();
        if (!isCrashed && !decided) {
            states.set(
                message.ID,
                new State(message.imposeBallot, message.estimate)
            );
            /*logger.info(
                "{}. Received GATHER from actor {} with impose ballot = {}",
                this.ID,
                message.ID,
                message.imposeBallot
                );*/
            gatherCount++;
            if (gatherCount >= (int) N / 2) {
                /*logger.info(
                    "{}. Gathered enough states, proceeding to impose.",
                    this.ID
                    );*/
                int maxBallot = Integer.MIN_VALUE;
                int maxEstimate = -1;
                for (State state : states) { // Computing the max ballot from the States list and then checking if it is positive
                    if (state.ballot > maxBallot) { // In the reverse order from the algorithm since it is simpler this way
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
                    neighbor.tell(
                        new Message_IMPOSE(
                            ballot,
                            proposal,
                            ID,
                            instanceCounter
                        ),
                        getSelf()
                    );
                }
            }
        }
    }

    private void onImpose(Message_IMPOSE message) {
        decideIfCrash();
        if (!isCrashed && !decided) {
            if (readBallot > message.ballot || imposeBallot > message.ballot) { // Another condition from the algorithm if met -> ABORT current instance
                getSender()
                    .tell(
                        new Message_ABORT(message.ballot, ID, instanceCounter),
                        getSelf()
                    ); // Send ABORT to all
                /*logger.info(
                    "{}. On IMPOSE from actor {}. Aborting instance no: {} because " +
                    readBallot +
                    " > " +
                    message.ballot +
                    " or " +
                    imposeBallot +
                    " > " +
                    message.ballot,
                    message.ID,
                    this.ID,
                    instanceCounter
                    );*/
                if (!onHold) {
                    scheduler.schedule(
                        () -> proposing(),
                        TIMEOUT,
                        TimeUnit.MILLISECONDS
                    );
                }
            } else {
                imposeBallot = message.ballot;
                estimate = message.proposal; // Updating the imposeBallot and estimation
                getSender()
                    .tell(
                        new Message_ACK(message.ballot, ID, instanceCounter),
                        getSelf()
                    ); // Sending ACK back to the sender
            }
        }
    }

    private void onAccept(Message_ACK message) {
        decideIfCrash();
        if (!isCrashed && !decided) {
            ackCount++;
            if (ackCount > (int) N / 2) { // When received ACK messages from a quorum the decided value is set
                decided = true;
                //long endTime = System.nanoTime(); // Computing the elapsed time to send to the Paxos actor
                reportActor.tell(
                    new Paxos.DecidedMessage(
                        System.currentTimeMillis(), //(endTime - startTime),
                        instanceCounter
                    ),
                    getSelf()
                );
                logger.info(
                    "{}. Instance no: {} terminated with a decided value: {}.",
                    this.ID,
                    instanceCounter,
                    proposal
                );
                for (ActorRef neighbor : myNeighbors) {
                    neighbor.tell(
                        new Message_DECIDE(proposal, ID, instanceCounter),
                        getSelf()
                    ); // Sending DECIDE to all
                    /*logger.info(
                        "{}. Instance {}. Sending {} to neighbor {}.",
                        this.ID,
                        message.instanceNumber,
                        proposal,
                        message.ID
                        );*/
                }
            }
        }
    }

    private void onDecide(Message_DECIDE message) {
        decideIfCrash();
        if (!isCrashed && !decided) {
            //long endTime = System.nanoTime(); // Computing the elapsed time to send to the Paxos actor
            decided = true;
            reportActor.tell(
                new Paxos.DecidedMessage(
                    System.currentTimeMillis(), //(endTime - startTime),
                    message.instanceNumber()
                ),
                getSelf()
            );
            logger.info(
                "{}. Instance no: {} terminated with a decided value: {} as received by {}.",
                this.ID,
                message.instanceNumber(),
                message.value,
                message.ID
            );
            //if (!onHold) {
            //    proposing();
            //}
        }
    }

    private void onCrash(Paxos.CrashMessage message) {
        //logger.info("{}. Received crash message: {}", this.ID, message);
        this.crash_prob = message.alpha(); // Setting the crash probability
    }

    private void onLaunch(Paxos.LaunchMessage message)
        throws InterruptedException {
        if (!isCrashed && !decided && !onHold) {
            proposing();
        }
    }

    private void onHold(Paxos.HoldMessage message) {
        onHold = true;
        logger.info("{}. On hold !", this.ID); // Setting the onHold boolean to go quiet and not propose anymore
    }

    // Main function to send PROPOSE messages, used a bunch of times for readability
    private void proposing() {
        instanceCounter++;
        ackCount = 0; // Resetting counters
        gatherCount = 0;
        logger.info("{}. Launching new instance: {}", this.ID, instanceCounter);
        int propose = (int) (Math.random() * 2); // Choosing a random proposal between 0 and 1
        if (propose == 0) {
            propose(new Message_PROPOSE(0, ID, instanceCounter));
        } else {
            propose(new Message_PROPOSE(1, ID, instanceCounter));
        }
    }

    private void decideIfCrash() {
        float rand = (float) Math.random();
        if (rand < crash_prob && !isCrashed) {
            isCrashed = true;
            //logger.info("{}. Crashing !", this.ID);
        }
    }

    // a record to build the States list
    public record State(int ballot, int estimate) {}

    public record Message_READ(int ballot, int ID, int instanceNumber) {}

    // ballot can be negative
    public record Message_ABORT(int ballot, int ID, int instanceNumber) {}

    // same for these ballot et imposeballot
    public record Message_GATHER(
        int ballot,
        int imposeBallot,
        int estimate,
        int ID,
        int instanceNumber
    ) {}

    public record Message_ACK(int ballot, int ID, int instanceNumber) {}

    public record Message_IMPOSE(
        int ballot,
        int proposal,
        int ID,
        int instanceNumber
    ) {}

    public record Message_PROPOSE(int proposal, int ID, int instanceNumber) {}

    // Decided value is either 0 or 1
    public record Message_DECIDE(int value, int ID, int instanceNumber) {}

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
