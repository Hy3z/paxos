package paxos;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;

public class Paxos {

    public static void main(String[] args) {
        System.out.println("Hello, World!");
        new Paxos().run();
    }

    private void run() {
        final ActorSystem system = ActorSystem.create("system");
        System.out.println("Running...");
        system.terminate();
        System.out.println("Terminated.");
    }
}
