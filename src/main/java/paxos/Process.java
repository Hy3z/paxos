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
        this
    );

    @Override
    public Receive createReceive() {
        return receiveBuilder()
            //.match(.class, this::)
            .build();
    }
}
