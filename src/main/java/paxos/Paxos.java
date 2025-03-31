package paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Paxos extends AbstractActor {

    public record RunMessage(
        int N,
        int f,
        float alpha,
        int time_initialisation_ms,
        int timeout_ms
    ) {}

    // system_time is the time at which the system decided, get it by calling
    // System.currentTimeMillis()
    public record DecidedMessage(long system_time, int ID, int value) {}

    // tell the report actor (here,the system actor) to report the time taken
    public record ReportMessage() {}

    // give the list of actors & the actor to report to (here, the system actor)
    public record ActorListMessage(
        ActorRef report_actor,
        List<ActorRef> actors
    ) {}

    // crash the actor with probability alpha
    public record CrashMessage(float alpha) {}

    // launch the actor
    public record LaunchMessage() {}

    // hold the actor
    public record HoldMessage() {}

    public static void main(final String[] args) throws InterruptedException {
        final ActorSystem system = ActorSystem.create("system");
        final ActorRef system_actor = system.actorOf(
            Props.create(Paxos.class),
            "system_actor"
        );
        system_actor.tell(
            // new RunMessage(3, 1, 0f, 100, 500),
            // new RunMessage(10, 4, .1f, 100, 1000),
            new RunMessage(3, 1, 1f, 1000, 1500),
            ActorRef.noSender()
        );
        Thread.sleep(7500); // Wait for the system to finish
        system_actor.tell(new ReportMessage(), ActorRef.noSender()); // Report the time taken
        Thread.sleep(1000); // Wait for the response
        system.terminate();
        System.exit(0);
    }

    final List<ActorRef> actors = new ArrayList<>();

    final List<ActorRef> alive_actors = new ArrayList<>();

    final LoggingAdapter logger = Logging.getLogger(
        getContext().getSystem(),
        this
    );

    long start_time = 0;
    long end_time = Long.MAX_VALUE;
    int decided_ID = -1;
    int decided_value = -1;

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(DecidedMessage.class, decidedMessage -> {
                end_time = decidedMessage.system_time();
                decided_ID = decidedMessage.ID();
                decided_value = decidedMessage.value();
            })
            .match(ReportMessage.class, report_message -> {
                logger.info(
                    "Decided by {} with value {} in {}ms",
                    decided_ID,
                    decided_value,
                    end_time - start_time
                );
            })
            .match(RunMessage.class, this::run)
            .build();
    }

    private void run(final RunMessage run_message) throws InterruptedException {
        // Create N actors
        logger.info("Creating N actors...");
        for (int i = 0; i < run_message.N(); i++) {
            actors.add(
                getContext()
                    .getSystem()
                    .actorOf(
                        Process.props(i, run_message.N()),
                        String.valueOf(i)
                    )
            );
        }
        alive_actors.addAll(actors);
        logger.info("Created.");

        // Send the list of actors to all actors
        logger.info("Sending list of actors...");
        for (final ActorRef actor : actors) {
            final ArrayList<ActorRef> actor_neighbours = new ArrayList<>(
                actors
            );
            actor_neighbours.remove(actor); // remove self from the list
            actor.tell(
                new ActorListMessage(getSelf(), actor_neighbours),
                getSelf()
            );
        }
        logger.info("Sent.");

        // Crash f actors
        logger.info("Crashing f actors...");
        Collections.shuffle(actors);
        for (int i = 0; i < run_message.f(); i++) {
            final ActorRef actor = actors.get(i);
            actor.tell(new CrashMessage(run_message.alpha()), getSelf());
            alive_actors.remove(actor);
        }
        logger.info("Crashed.");

        // Wait for all actors to be ready
        Thread.sleep(run_message.time_initialisation_ms());

        // Launch the actors
        logger.info("Launching actors...");
        start_time = System.currentTimeMillis();
        for (final ActorRef actor : actors) {
            actor.tell(new LaunchMessage(), getSelf());
        }
        logger.info("Launched.");

        // Wait for a fixed amount of time
        Thread.sleep(run_message.timeout_ms());

        // Send a hold message to all alive actors but one
        logger.info("Sending hold messages...");
        Collections.shuffle(alive_actors);
        ActorRef leader = alive_actors.get(0);
        logger.info("Leader is {}", leader.path().name());
        for (ActorRef actor : actors) {
            if (!actor.equals(leader)) {
                actor.tell(new HoldMessage(), getSelf());
            }
        }
        logger.info("Sent.");
    }
}
