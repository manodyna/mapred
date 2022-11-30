package dyna.mapreddemo

import akka.actor.Actor
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import dyna.mapred._

class SimpleExample extends Actor {
  implicit val system = context.system
  implicit val materializer = ActorMaterializer()

  val theData = "[INFO] [11/30/2022 13:48:13.250] [Main-akka.actor.default-dispatcher-8] [akka://Main/user/$a/flow-1-3-actorSubscriberSink] DONE: 2 chunks\n[INFO] [11/30/2022 13:48:13.250] [Main-akka.actor.default-dispatcher-8] [akka://Main/user/$a/flow-1-3-actorSubscriberSink] DONE: 3 chunks\n[INFO] [11/30/2022 13:48:13.250] [Main-akka.actor.default-dispatcher-8] [akka://Main/user/$a/flow-1-3-actorSubscriberSink] DONE: 4 chunks\n[INFO] [11/30/2022 13:48:13.379] [Main-akka.actor.default-dispatcher-7] [akka://Main/user/$a/flow-1-3-actorSubscriberSink] REDUCER AGGREGATION COMPLETED\nFINAL RESULTS".toList

  val nWorkers = 8
  val chunkSize = theData.size / nWorkers

  def mapFun(ch: Char) = Some(MapredWorker.KeyVal(ch.toLower, 1))

  val mapredProps = Mapred.props(nWorkers)(mapFun)(_ + _) { counters =>
    println(s"FINAL RESULTS\n$counters")
    context stop self
  }

  Source(theData).grouped(chunkSize).to(Sink.actorSubscriber(mapredProps)).run()

  override def receive = {
    case _ =>
  }
}
