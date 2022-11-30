package dyna.mapred

import akka.actor.{ActorLogging, Props}
import akka.routing._
import akka.stream.actor.ActorSubscriberMessage.{OnComplete, OnError, OnNext}
import akka.stream.actor.{ActorSubscriber, MaxInFlightRequestStrategy}
import dyna.mapred.MapredWorker._

import scala.reflect.ClassTag

class Mapred[A: ClassTag, K: ClassTag, V: ClassTag](nWorkers: Int,
                                                    mf: A => TraversableOnce[MapredWorker.KeyVal[K, V]],
                                                    rf: (V, V) => V,
                                                    ff: Map[K, V] => Unit) extends ActorSubscriber with ActorLogging {

  val MAX_QUEUE_SIZE = 2 * nWorkers
  var queue = 0
  var nChunksDone = 0

  override val requestStrategy = new MaxInFlightRequestStrategy(max = MAX_QUEUE_SIZE) {
    override def inFlightInternally: Int = queue
  }

  def workerProps = SmallestMailboxPool(nWorkers).props(MapredWorker.props(mf, rf, ff))

  val mapredRouter = context.actorOf(workerProps, "mapred-router")

  override def receive = {
    case OnNext(chunk: TraversableOnce[A]) =>
      queue += 1
      mapredRouter ! DataChunk(chunk, self)

    case OnError(err: Exception) =>
      log.error(err.toString, "Error during processing")
      context stop self

    case OnComplete =>
      log.info("INPUT CONSUMED - FINISHING PROCESSING")
      mapredRouter ! FinishWorkers(nWorkers - 1, self)
      context.become(reduceReducers(nWorkers - 1))

    case ChunkAck =>
      queue -= 1
      nChunksDone += 1
      log.info(s"DONE: $nChunksDone chunks")
  }

  def reduceReducers(toAck: Int, acked: Int = 0): Receive = {
    case ChunkAck =>
      queue -= 1
      nChunksDone += 1
      log.info(s"DONE: $nChunksDone chunks")

    case KeyValChunkAck =>
      if (acked + 1 < toAck)
        context.become(reduceReducers(toAck, acked + 1))
      else {
        mapredRouter ! FinishWorkers(toAck - 1, self)
        context.become(reduceReducers(toAck - 1))
      }

    case ResultData(acc) =>
      log.info("REDUCER AGGREGATION COMPLETED")
      ff(acc.asInstanceOf[Map[K, V]])
      context stop self
  }
}

object Mapred {
  def props[A: ClassTag, K: ClassTag, V: ClassTag](nWorkers: Int)
                                                  (mf: A => TraversableOnce[MapredWorker.KeyVal[K, V]])
                                                  (rf: (V, V) => V)
                                                  (ff: Map[K, V] => Unit)
                                                  (implicit context: akka.actor.ActorContext) =
    Props(new Mapred(nWorkers, mf, rf, ff))
}
