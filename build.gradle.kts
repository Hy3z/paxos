plugins { application }

repositories {
    mavenCentral()
    maven("https://repo.akka.io/maven")
}

dependencies {
    implementation("com.typesafe.akka:akka-actor_2.12:2.5.22")
    implementation("com.typesafe.akka:akka-testkit_2.12:2.5.22")
}

java { toolchain { languageVersion = JavaLanguageVersion.of(21) } }

application { mainClass = ("paxos.Paxos") }
