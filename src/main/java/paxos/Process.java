package paxos;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class Process extends AbstractActor {

    public static Props props() {
        return Props.create(Process.class);
    }

    private final LoggingAdapter logger = Logging.getLogger(
            getContext().getSystem(),
            this);

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // .match(.class, this::)
                .build();
    }

    // certain entier peuvent être nul, on mettre une valeur très petit (min_int ?)
    // pour ces valeurs
    // ballot peut être négatif
    public record Message_READ(int ballot) {
    }

    // ballot peut être négatif
    public record Message_ABORT(int ballot) {
    }

    // ballot et imposeballot peut être négatif
    public record Message_GATHER(int ballot, int imposeballot) {
    }

    // ballot peut être négatif
    public record Message_ACK(int ballot) {
    }

    // ballot peut être négatif, proposal peut être "nil"
    public record Message_IMPOSE(int ballot, int proposal) {

    }

    // value est une valeur entre 0 et 1
    public record Message_DECIDE(int value) {
    }

}
