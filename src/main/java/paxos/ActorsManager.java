package paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ActorsManager extends AbstractActor {

    private final int N;
    private final int f;
    private final float alpha;
    private final int hold_ms;
    private final int timeout_ms;
    private final ActorRef reportActor;
    private final List<ActorRef> actors = new ArrayList<>();
    private final List<ActorRef> aliveActors = new ArrayList<>();

    public static Props props(Paxos.RunMessage runMsg, ActorRef reportActor) {
        return Props.create(ActorsManager.class, () -> new ActorsManager(runMsg, reportActor));
    }

    public ActorsManager(Paxos.RunMessage runMsg, ActorRef reportActor) {
        this.N = runMsg.N();
        this.f = runMsg.f();
        this.alpha = runMsg.alpha();
        this.hold_ms = runMsg.time_initialisation_ms();
        this.timeout_ms = runMsg.timeout_ms();
        this.reportActor = reportActor;
    }

    @Override
    public void preStart() throws Exception {
        // Création des acteurs
        for (int i = 0; i < N; i++) {
            ActorRef actor = getContext().actorOf(Process.props(i, N), "p" + i);
            actors.add(actor);
        }
        aliveActors.addAll(actors);

        // Envoi des listes de voisins
        for (ActorRef actor : actors) {
            List<ActorRef> neighbors = new ArrayList<>(actors);
            neighbors.remove(actor);
            actor.tell(new Paxos.ActorListMessage(reportActor, neighbors), getSelf());
        }

        // Crash de f acteurs aléatoires
        Collections.shuffle(actors);
        for (int i = 0; i < f; i++) {
            ActorRef actor = actors.get(i);
            actor.tell(new Paxos.CrashMessage(alpha), getSelf());
            aliveActors.remove(actor);
        }

        // Lancement après pause
        getContext().getSystem().scheduler().scheduleOnce(
                scala.concurrent.duration.Duration.create(hold_ms, java.util.concurrent.TimeUnit.MILLISECONDS),
                getSelf(),
                "launch",
                getContext().getSystem().dispatcher(),
                getSelf());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .matchEquals("launch", m -> {
                    long start = System.currentTimeMillis();
                    for (ActorRef actor : actors) {
                        actor.tell(new Paxos.LaunchMessage(), getSelf());
                    }

                    // Stoppe les autres après un timeout
                    getContext().getSystem().scheduler().scheduleOnce(
                            scala.concurrent.duration.Duration.create(timeout_ms,
                                    java.util.concurrent.TimeUnit.MILLISECONDS),
                            getSelf(),
                            "hold_rest",
                            getContext().getSystem().dispatcher(),
                            getSelf());
                })
                .matchEquals("hold_rest", m -> {
                    Collections.shuffle(aliveActors);
                    for (int i = 1; i < aliveActors.size(); i++) {
                        aliveActors.get(i).tell(new Paxos.HoldMessage(), getSelf());
                    }
                })
                .build();
    }
}
