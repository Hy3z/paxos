package paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class Paxos extends AbstractActor {

    final List<ActorRef> actors = new ArrayList<>();
    final List<ActorRef> alive_actors = new ArrayList<>();
    final LoggingAdapter logger = Logging.getLogger(
        getContext().getSystem(),
        this
    );
    long start_time = 0;
    long end_time = Long.MAX_VALUE;

    public static void main(final String[] args) throws InterruptedException {
        final ActorSystem system = ActorSystem.create("system");
        final ActorRef system_actor = system.actorOf(
            Props.create(Paxos.class),
            "system_actor"
        );
        system_actor.tell(
            new RunMessage(50, 10, .1f, 100, 50),
            ActorRef.noSender()
        );
        Thread.sleep(10000); //Wait for the system to finish
        system_actor.tell(new ReportMessage(), ActorRef.noSender()); //Report the time taken
        system.terminate();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            .match(DecidedMessage.class, decidedMessage -> {
                end_time = Math.min(end_time, decidedMessage.system_time());
            })
            .match(ReportMessage.class, report_message -> {
                logger.info("Time taken: " + (end_time - start_time) + "ms");
            })
            .match(RunMessage.class, this::run)
            .build();
    }

    private void run(RunMessage run_message) throws InterruptedException {
        //Create N actors
        logger.info("Creating N actors...");
        for (int i = 0; i < run_message.N(); i++) {
            actors.add(
                getContext()
                    .getSystem()
                    .actorOf(Process.props(), String.valueOf(i))
            );
        }
        alive_actors.addAll(actors);
        logger.info("Created.");

        //Send the list of actors to all actors
        logger.info("Sending list of actors...");
        for (final ActorRef actor : actors) {
            final ArrayList<ActorRef> actor_neighbours = new ArrayList<>(
                actors
            );
            actor_neighbours.remove(actor); //remove self from the list
            actor.tell(
                new ActorListMessage(actor_neighbours),
                ActorRef.noSender()
            );
        }
        logger.info("Sent.");

        //Send the system actor to all actors
        logger.info("Sending system actor...");
        for (final ActorRef actor : actors) {
            actor.tell(
                new SystemActorMessage(getContext().getSelf()),
                ActorRef.noSender()
            );
        }
        logger.info("Sent.");

        //Crash f actors
        logger.info("Crashing f actors...");
        Collections.shuffle(actors);
        for (int i = 0; i < run_message.f(); i++) {
            final ActorRef actor = actors.get(i);
            actor.tell(
                new CrashMessage(run_message.alpha()),
                ActorRef.noSender()
            );
            alive_actors.remove(actor);
        }
        logger.info("Crashed.");

        //Wait for all actors to be ready
        Thread.sleep(run_message.hold_ms());

        //Launch the actors
        logger.info("Launching actors...");
        start_time = System.currentTimeMillis();
        for (final ActorRef actor : actors) {
            actor.tell(new LaunchMessage(), ActorRef.noSender());
        }
        logger.info("Launched.");

        //Wait for a fixed amount of time
        Thread.sleep(run_message.timeout_ms());

        //Send a hold message to all alive actors but one
        logger.info("Sending hold messages...");
        Collections.shuffle(alive_actors);
        for (int i = 1; i < alive_actors.size(); i++) {
            final ActorRef alive_actor = alive_actors.get(i);
            alive_actor.tell(new HoldMessage(), ActorRef.noSender());
        }
        logger.info("Sent.");
    }

    public record RunMessage(
        int N,
        int f,
        float alpha,
        int hold_ms,
        int timeout_ms
    ) {}

    public record DecidedMessage(long system_time) {}

    public record ReportMessage() {}
}
