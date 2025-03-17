package paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Paxos {

    public static void main(String[] args) {
        new Paxos().run(50, 10, 50, 1000, .1f);
    }

    private void run(int N, int f, int timeout_ms, int hold_ms, float alpha) {
        final List<ActorRef> actors = new ArrayList<>();
        final List<ActorRef> alive_actors = new ArrayList<>();
        final ActorSystem system = ActorSystem.create("system");
        final LoggingAdapter logger = Logging.getLogger(system, this);

        //Create N actors
        logger.info("Creating N actors...");
        for (int i = 0; i < N; i++) {
            actors.add(system.actorOf(Process.props(), String.valueOf(i)));
        }
        alive_actors.addAll(actors);
        logger.info("Created.");

        //Send the list of actors to all actors
        logger.info("Sending list of actors...");
        for (ActorRef actor : actors) {
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

        //Crash f actors
        logger.info("Crashing f actors...");
        Collections.shuffle(actors);
        for (int i = 0; i < f; i++) {
            ActorRef actor = actors.get(i);
            actor.tell(new CrashMessage(alpha), ActorRef.noSender());
            alive_actors.remove(actor);
        }
        logger.info("Crashed.");

        //Launch the actors
        logger.info("Launching actors...");
        for (ActorRef actor : actors) {
            actor.tell(new LaunchMessage(), ActorRef.noSender());
        }
        logger.info("Launched.");

        //Wait for a fixed amount of time
        Thread.sleep(timeout_ms);

        //Send a hold message to all correct actors but one
        logger.info("Sending hold messages...");
        Collections.shuffle(alive_actors);
        for (int i = 1; i < alive_actors.size(); i++) {
            ActorRef actor = alive_actors.get(i);
            actor.tell(new HoldMessage(), ActorRef.noSender());
        }
        logger.info("Sent.");

        //Wait a fixed amount of time for the system to stabilize
        Thread.sleep(hold_ms);

        //Terminate system
        system.terminate();
        System.out.println("Terminated.");
    }
}
