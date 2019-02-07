package akkatour;

import akka.Done;
import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.japi.Pair;
import akka.stream.*;
import akka.stream.javadsl.*;
import akka.util.ByteString;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;

public class HelloApp {
    public static void main(final String[] args) {
        work5();
    }

    public static void impl1() {
        final ActorSystem system = ActorSystem.create("hello-app");
        final Materializer materializer = ActorMaterializer.create(system);

        final ByteString lineSeparator = ByteString.fromString(System.getProperty("line.separator"));

        final CompletionStage<IOResult> eventualResult = FileIO.fromPath(Paths.get("pom.xml"))
                .via(Framing.delimiter(lineSeparator, 200).map(ByteString::utf8String))
                .filter(line -> line.contains("<artifactId>"))
                .map(String::trim)
                .alsoTo(Sink.foreach(System.out::println))
                .map(ByteString::fromString)
                .intersperse(lineSeparator)
                .runWith(FileIO.toPath(Paths.get("dependencies.txt")), materializer);

        eventualResult.whenComplete((s, f) -> system.terminate());
    }

    public static void impl2() {
        final ActorSystem system = ActorSystem.create("hello-app");
        final Materializer materializer = ActorMaterializer.create(system);

        final ByteString lineSeparator = ByteString.fromString(System.getProperty("line.separator"));
        final Flow<ByteString, String, NotUsed> unmarshall = Framing.delimiter(lineSeparator, 200).map(ByteString::utf8String);
        final Flow<String, ByteString, NotUsed> marshall = Flow.of(String.class).map(ByteString::fromString).intersperse(lineSeparator);

        final RunnableGraph<CompletionStage<IOResult>> artifacts = FileIO.fromPath(Paths.get("pom.xml"))
                .via(unmarshall)
                .filter(line -> line.contains("<artifactId>"))
                .map(String::trim)
                .alsoTo(Sink.foreach(System.out::println))
                .via(marshall)
                .to(FileIO.toPath(Paths.get("dependencies.txt")));

        final CompletionStage<IOResult> eventualResult = artifacts.run(materializer);
        eventualResult.whenComplete((s, f) -> system.terminate());
    }

    /**
     * https://doc.akka.io/docs/alpakka/current/data-transformations/csv.html
     */
    public static void impl3() {
    }

    public static void work1() {
        final ActorSystem system = ActorSystem.create("hello-app");
        final Materializer materializer = ActorMaterializer.create(system);

        final Source<Pair<Integer, String>, NotUsed> numbers = Source.range(1, 10).zip(Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "BIP"));
        final Sink<Pair<Integer, String>, CompletionStage<Done>> output = Sink.foreach(System.out::println);
        final RunnableGraph<CompletionStage<Done>> graph = numbers.toMat(output, Keep.right());

        final CompletionStage<Done> eventualResult = graph.run(materializer);
        eventualResult.whenComplete((r, e) -> system.terminate());
    }

    public static void work2() {
        final ActorSystem system = ActorSystem.create("hello-app");
        final Materializer materializer = ActorMaterializer.create(system);

        final CompletionStage<Done> eventualResult = Source.range(1, 10).zip(Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "BIP"))
                .runForeach(System.out::println, materializer);

        eventualResult.whenComplete((r, e) -> system.terminate());
    }

    public static void work3() {
        final ActorSystem system = ActorSystem.create("hello-app");
        final Materializer materializer = ActorMaterializer.create(system);

        final Source<Pair<Integer, String>, NotUsed> numberTickPairs = Source.range(1, 10)
                        .zip(Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "BIP"));

        final Flow<Pair<Integer, String>, Integer, NotUsed> keepNumber = Flow.<Pair<Integer, String>>create().map(Pair::first);
        final Sink<Integer,CompletionStage<Done>> display = Sink.foreach(System.out::println);

        final RunnableGraph<CompletionStage<Done>> graph = numberTickPairs.via(keepNumber).toMat(display, Keep.right());

        final CompletionStage<Done> eventualResult = graph.run(materializer);
        eventualResult.whenComplete((r, e) -> system.terminate());
    }

    public static void work4() {
        final ActorSystem system = ActorSystem.create("hello-app");
        final Materializer materializer = ActorMaterializer.create(system);

        final CompletionStage<Done> eventualResult = Source.range(1, 10)
                .zip(Source.tick(Duration.ofSeconds(1), Duration.ofSeconds(1), "BIP"))
                .map(Pair::first)
                .runForeach(System.out::println, materializer);

        eventualResult.whenComplete((r, e) -> system.terminate());
    }

    public static void work5() {
        final ActorSystem system = ActorSystem.create("hello-app");
        final Materializer materializer = ActorMaterializer.create(system);

        final ByteString lineSeparator = ByteString.fromString(System.getProperty("line.separator"));
        final Source<ByteString, CompletionStage<IOResult>> inBytes = FileIO.fromPath(Paths.get("pom.xml"));
        final Sink<ByteString, CompletionStage<IOResult>> outBytes = FileIO.toPath(Paths.get("dependencies.txt"));

        final Flow<ByteString, String, NotUsed> unmarshaller = Framing.delimiter(lineSeparator, 200)
                .map(ByteString::utf8String);

        final Flow<String, ByteString, NotUsed> marshaller = Flow.of(String.class)
                .map(ByteString::fromString)
                .intersperse(ByteString.empty(), lineSeparator, lineSeparator);

        final Flow<String, String, NotUsed> handleLine = Flow.of(String.class)
                .filter(line -> line.contains("<artifactId>"))
                .map(String::trim);

        final CompletionStage<IOResult> eventualResult = inBytes
                .via(unmarshaller)
                .via(handleLine)
                .alsoTo(Sink.foreach(System.out::println))
                .via(marshaller)
                .alsoTo(Sink.foreach(System.out::println))
                .toMat(outBytes, Keep.right())
                .run(materializer);

        eventualResult.whenComplete((r, e) -> system.terminate());
    }
}
