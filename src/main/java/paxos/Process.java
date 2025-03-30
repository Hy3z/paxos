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
import paxos.Paxos.ActorListMessage;
import paxos.Paxos.CrashMessage;
import paxos.Paxos.DecidedMessage;
import paxos.Paxos.HoldMessage;
import paxos.Paxos.LaunchMessage;
import paxos.SynodData;
import paxos.SynodData.State;

public class Process extends AbstractActor {

    private record ReadMessage(int ballot) {}

    private record AbortMessage(int ballot) {}

    private record GatherMessage(
        int ID,
        int ballot,
        int est_ballot,
        int estimate
    ) {}

    private record ImposeMessage(int ballot, int proposal) {}

    private record AckMessage(int ballot) {}

    private record DecideMessage(int proposal) {}

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
    private final SynodData synod_data;
    private final int proposal = (int) (Math.random() * 2);

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
        synod_data = new SynodData(ID, N);
    }

    /*
    private InstanceData computeIfAbsent(int instance) {
        if (instances_data.containsKey(instance)) {
            return instances_data.get(instance);
        }
        InstanceData synod_data = new InstanceData(ID, N);
        instances_data.put(instance, synod_data);
        return synod_data;
    }*/

    private void propose() {
        logger.info("{}: Proposing {}", ID, proposal);
        synod_data.proposal = proposal;
        synod_data.ballot = synod_data.ballot + N;
        synod_data.resetStates();
        actors.forEach(actor -> {
            actor.tell(new ReadMessage(synod_data.ballot), getSelf());
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

    private void report(int value) {
        report_actor.tell(
            new DecidedMessage(System.currentTimeMillis(), ID, value),
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
        logger.info("{}: Someone decided {} ", ID, decideMessage.proposal());
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
        if (
            tryReturnCrash() ||
            decided ||
            ackMessage.ballot() != synod_data.ballot
        ) {
            return;
        }
        synod_data.ack_count++;
        if (synod_data.ack_count >= N / 2) {
            decided = true;
            logger.info("{}: Decided {}", ID, synod_data.proposal);
            report(synod_data.proposal);
            actors.forEach(actor -> {
                actor.tell(new DecideMessage(synod_data.proposal), getSelf());
            });
        }
    }

    private void onImposeMessage(ImposeMessage imposeMessage) {
        if (tryReturnCrash()) {
            return;
        }
        if (
            synod_data.read_ballot > imposeMessage.ballot() ||
            synod_data.impose_ballot > imposeMessage.ballot()
        ) {
            getSender()
                .tell(new AbortMessage(imposeMessage.ballot()), getSelf());
        } else {
            synod_data.estimate = imposeMessage.proposal();
            synod_data.impose_ballot = imposeMessage.ballot();
            getSender().tell(new AckMessage(imposeMessage.ballot()), getSelf());
        }
    }

    private void onGatherMessage(GatherMessage gatherMessage) {
        if (tryReturnCrash() || gatherMessage.ballot() != synod_data.ballot) {
            return;
        }
        synod_data.states.set(
            gatherMessage.ID(),
            new State(gatherMessage.est_ballot(), gatherMessage.estimate())
        );
        if (synod_data.statesCount() >= N / 2) {
            //logger.info("{}: Majority gathered", ID);
            State highest = synod_data.highestState();
            if (highest != null && highest.estballot() > 0) {
                synod_data.proposal = highest.est();
            }
            synod_data.resetStates();
            actors.forEach(actor -> {
                actor.tell(
                    new ImposeMessage(synod_data.ballot, synod_data.proposal),
                    getSelf()
                );
            });
        }
    }

    private void onAbortMessage(AbortMessage abortMessage) {
        if (decided || hold || abortMessage.ballot() != synod_data.ballot) {
            return;
        }
        propose();
        //Propose another value after some time
        //scheduler.schedule(() -> propose(), TIMEOUT, TimeUnit.MILLISECONDS);
    }

    private void onReadMessage(ReadMessage readMessage) {
        if (tryReturnCrash()) {
            return;
        }
        if (
            synod_data.read_ballot > readMessage.ballot() ||
            synod_data.impose_ballot > readMessage.ballot()
        ) {
            getSender().tell(new AbortMessage(readMessage.ballot()), getSelf());
        } else {
            synod_data.read_ballot = readMessage.ballot();
            getSender()
                .tell(
                    new GatherMessage(
                        ID,
                        readMessage.ballot(),
                        synod_data.impose_ballot,
                        synod_data.estimate
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
