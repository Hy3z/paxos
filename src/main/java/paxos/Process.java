package paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor.AbortPolicy;
import java.util.concurrent.TimeUnit;
import paxos.InstanceData;
import paxos.InstanceData.State;
import paxos.Paxos.ActorListMessage;
import paxos.Paxos.CrashMessage;
import paxos.Paxos.DecidedMessage;
import paxos.Paxos.HoldMessage;
import paxos.Paxos.LaunchMessage;

public class Process extends AbstractActor {

    private record ReadMessage(int ballot, int instance) {}

    private record AbortMessage(int ballot, int instance) {}

    private record GatherMessage(
        int ID,
        int ballot,
        int est_ballot,
        int estimate,
        int instance
    ) {}

    private record ImposeMessage(int ballot, int proposal, int instance) {}

    private record AckMessage(int ballot, int instance) {}

    private record DecideMessage(int proposal, int instance) {}

    /*
    private static final int TIMEOUT = 1000;
    private final ScheduledExecutorService scheduler =
        Executors.newScheduledThreadPool(1);
    */

    private int ID;
    private int N;
    private final List<ActorRef> actors = new ArrayList<>();
    private ActorRef report_actor;
    private float alpha = 0f;
    private boolean hold = false;
    private boolean crashed = false;
    private boolean decided = false;
    private final HashMap<Integer, InstanceData> instances_data =
        new HashMap<>();
    private int proposed_instance = 0;

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
    }

    private InstanceData computeIfAbsent(int instance) {
        if (instances_data.containsKey(instance)) {
            return instances_data.get(instance);
        }
        InstanceData instance_data = new InstanceData(ID, N);
        instances_data.put(instance, instance_data);
        return instance_data;
    }

    private void propose() {
        int v = (int) (Math.random() * 2);
        int instance_number = ++proposed_instance; //start at 1
        logger.info("{}: Proposing {} for instance {}", ID, v, instance_number);
        InstanceData instance_data = computeIfAbsent(instance_number);
        instance_data.proposal = v;
        instance_data.ballot = instance_data.ballot + N;
        instance_data.resetStates();
        actors.forEach(actor -> {
            actor.tell(
                new ReadMessage(instance_data.ballot, instance_number),
                getSelf()
            );
        });
    }

    private boolean tryReturnCrash() {
        if (crashed) {
            return true;
        }
        if (alpha == 0) {
            return false;
        }
        if (Math.random() < alpha) {
            //logger.info("{}: crashed", ID);
            crashed = true;
        }
        return crashed;
    }

    private void report(int instance_number) {
        report_actor.tell(
            new DecidedMessage(System.currentTimeMillis(), instance_number),
            getSelf()
        );
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(DecideMessage.class, this::onDecideMessage)
            .match(AckMessage.class, this::onAckMessage)
            .match(ImposeMessage.class, this::onImposeMessage)
            .match(GatherMessage.class, this::onGatherMessage)
            .match(AbortMessage.class, this::onAbortMessage)
            .match(ReadMessage.class, this::onReadMessage)
            .match(LaunchMessage.class, this::onLaunchMessage)
            .match(HoldMessage.class, this::onHoldMessage)
            .match(CrashMessage.class, this::onCrashMessage)
            .match(ActorListMessage.class, this::onActorListMessage)
            .build();
    }

    private void onDecideMessage(DecideMessage decideMessage) {
        if (tryReturnCrash() || decided) {
            return;
        }
        decided = true;
        logger.info(
            "{}: Someone decided {} at instance {}",
            ID,
            decideMessage.proposal(),
            decideMessage.instance()
        );
        //No need to send to all actors, since everyone has reference to everyone
        /*
        actors.forEach(actor -> {
            actor.tell(
                new DecideMessage(decideMessage.proposal()),
                getSender()
            );
            });
        */

    }

    private void onAckMessage(AckMessage ackMessage) {
        if (tryReturnCrash() || decided) {
            return;
        }
        InstanceData instance_data = computeIfAbsent(ackMessage.instance());
        if (ackMessage.ballot() != instance_data.ballot) {
            return; //this should not be possible
        }
        instance_data.ack_count++;
        if (instance_data.ack_count >= N / 2) {
            decided = true;
            logger.info(
                "{}: Decided {} at instance {}",
                ID,
                instance_data.proposal,
                ackMessage.instance()
            );
            report(ackMessage.instance());
            actors.forEach(actor -> {
                actor.tell(
                    new DecideMessage(
                        instance_data.proposal,
                        ackMessage.instance()
                    ),
                    getSelf()
                );
            });
        }
    }

    private void onImposeMessage(ImposeMessage imposeMessage) {
        if (tryReturnCrash()) {
            return;
        }
        InstanceData instance_data = computeIfAbsent(imposeMessage.instance());
        if (
            instance_data.read_ballot > imposeMessage.ballot() ||
            instance_data.impose_ballot > imposeMessage.ballot()
        ) {
            getSender()
                .tell(
                    new AbortMessage(
                        imposeMessage.ballot(),
                        imposeMessage.instance()
                    ),
                    getSelf()
                );
        } else {
            instance_data.estimate = imposeMessage.proposal();
            instance_data.impose_ballot = imposeMessage.ballot();
            getSender()
                .tell(
                    new AckMessage(
                        imposeMessage.ballot(),
                        imposeMessage.instance()
                    ),
                    getSelf()
                );
        }
    }

    private void onGatherMessage(GatherMessage gatherMessage) {
        if (tryReturnCrash()) {
            //logger.info("{}: Gather message not for me", ID);
            return;
        }
        InstanceData instance_data = computeIfAbsent(gatherMessage.instance());
        if (gatherMessage.ballot() != instance_data.ballot) {
            return; //this should not be possible
        }
        instance_data.states.set(
            gatherMessage.ID(),
            new State(gatherMessage.est_ballot(), gatherMessage.estimate())
        );
        if (instance_data.statesCount() >= N / 2) {
            //logger.info("{}: Majority gathered", ID);
            State highest = instance_data.highestState();
            if (highest != null && highest.estballot() > 0) {
                instance_data.proposal = highest.est();
            }
            instance_data.resetStates();
            actors.forEach(actor -> {
                actor.tell(
                    new ImposeMessage(
                        instance_data.ballot,
                        instance_data.proposal,
                        gatherMessage.instance()
                    ),
                    getSelf()
                );
            });
        }
    }

    private void onAbortMessage(AbortMessage abortMessage) {
        if (decided || hold || abortMessage.instance() != proposed_instance) {
            return;
        }
        InstanceData instance_data = computeIfAbsent(abortMessage.instance());
        if (abortMessage.ballot() != instance_data.ballot) {
            return; //this should not be possible
        }
        propose();
        //Propose another value after some time
        //scheduler.schedule(() -> propose(), TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void onReadMessage(ReadMessage readMessage) {
        if (tryReturnCrash()) {
            return;
        }
        InstanceData instance_data = computeIfAbsent(readMessage.instance());
        //logger.info("{}: Read", ID);
        if (
            instance_data.read_ballot > readMessage.ballot() ||
            instance_data.impose_ballot > readMessage.ballot()
        ) {
            /*logger.info(
                "abort {} {} {} {}",
                readMessage.ballot(),
                readMessage.instance(),
                instance_data.read_ballot,
                instance_data.impose_ballot
                );*/
            getSender()
                .tell(
                    new AbortMessage(
                        readMessage.ballot(),
                        readMessage.instance()
                    ),
                    getSelf()
                );
        } else {
            instance_data.read_ballot = readMessage.ballot();
            getSender()
                .tell(
                    new GatherMessage(
                        ID,
                        readMessage.ballot(),
                        instance_data.impose_ballot,
                        instance_data.estimate,
                        readMessage.instance()
                    ),
                    getSelf()
                );
        }
    }

    private void onLaunchMessage(LaunchMessage launchMessage) {
        if (decided || hold || tryReturnCrash()) {
            return;
        }
        propose();
    }

    private void onHoldMessage(HoldMessage holdMessage) {
        hold = true;
        //logger.info("{}: Hold", ID);
    }

    private void onCrashMessage(CrashMessage crashMessage) {
        alpha = crashMessage.alpha();
    }

    private void onActorListMessage(ActorListMessage actorListMessage) {
        actors.addAll(actorListMessage.actors());
        report_actor = actorListMessage.report_actor();
    }
}
