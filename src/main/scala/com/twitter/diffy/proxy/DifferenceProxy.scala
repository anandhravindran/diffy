package com.twitter.diffy.proxy

import javax.inject.Singleton
import com.google.inject.Provides
import com.twitter.diffy.analysis._
import com.twitter.diffy.lifter.Message
import com.twitter.finagle._
import com.twitter.inject.TwitterModule
import com.twitter.logging.Logger
import com.twitter.util._

object DifferenceProxyModule extends TwitterModule {
  @Provides
  @Singleton
  def providesDifferenceProxy(
    settings: Settings,
    collector: InMemoryDifferenceCollector,
    joinedDifferences: JoinedDifferences,
    analyzer: DifferenceAnalyzer): DifferenceProxy =
    settings.protocol match {
      case "thrift" => ThriftDifferenceProxy(settings, collector, joinedDifferences, analyzer)
      case "http" => SimpleHttpDifferenceProxy(settings, collector, joinedDifferences, analyzer)
    }
}

object DifferenceProxy {
  object NoResponseException extends Exception("No responses provided by diffy")
  val NoResponseExceptionFuture = Future.exception(NoResponseException)
  val log = Logger(classOf[DifferenceProxy])
}

trait DifferenceProxy {
  import DifferenceProxy._

  type Req
  type Rep
  type Srv <: ClientService[Req, Rep]

  val server: ListeningServer
  val settings: Settings
  var lastReset: Time = Time.now

  def serviceFactory(serverset: String, label: String): Srv

  def liftRequest(req: Req): Future[Message]
  def liftResponse(rep: Try[Rep]): Future[Message]

  // Clients for services
  val candidate = serviceFactory(settings.candidate.path, "candidate")
  val primary = serviceFactory(settings.primary.path, "primary")
  val secondary = serviceFactory(settings.secondary.path, "secondary")

  val collector: InMemoryDifferenceCollector

  val joinedDifferences: JoinedDifferences

  val analyzer: DifferenceAnalyzer

  private[this] lazy val multicastHandlerPrimary =
    new SequentialMulticastService(Seq(primary.client))

  private[this] lazy val multicastHandlerCandidate =
    new SequentialMulticastService(Seq(candidate.client))

  private[this] lazy val multicastHandlerSecondary =
    new SequentialMulticastService(Seq(secondary.client))
  var requestNo: Int = 0

  var rawResponses: Future[Seq[Try[Rep]]] = Future.Nil

  var rawResponsesPrimary: Future[Seq[Try[Rep]]] = Future.Nil

  var rawResponsesCandidate: Future[Seq[Try[Rep]]] = Future.Nil

  var rawResponsesSecondary: Future[Seq[Try[Rep]]] = Future.Nil

  def proxy = new Service[Req, Rep] {
    override def apply(req: Req): Future[Rep] = {
      var currentResponse: Future[Seq[Try[Rep]]] = Future.Nil
      if (requestNo == 0) {
        rawResponsesPrimary = multicastHandlerPrimary(req) respond {
          case Return(_) => log.debug("success networking")
          case Throw(t) => log.debug(t, "error networking")
        }
        log.info("Raw response : " + rawResponsesPrimary.get().toString())
        currentResponse = rawResponsesPrimary
      } else if (requestNo == 1) {
        rawResponsesCandidate =
          multicastHandlerCandidate(req) respond {
            case Return(_) => log.debug("success networking")
            case Throw(t) => log.debug(t, "error networking")
          }
        log.info("Raw response : " + rawResponsesCandidate.get().toString())
        currentResponse = rawResponsesCandidate
      } else if (requestNo == 2) {
        rawResponsesSecondary =
          multicastHandlerSecondary(req) respond {
            case Return(_) => log.debug("success networking")
            case Throw(t) => log.debug(t, "error networking")
          }
        log.info("Raw response : " + rawResponsesSecondary.get().toString())
        currentResponse = rawResponsesSecondary
      }

      if (requestNo == 2) {
        rawResponses = Future.value(Seq(rawResponsesPrimary.get().head, rawResponsesCandidate.get().head, rawResponsesSecondary.get().head))
        log.info("Raw response : " + rawResponses.get().toString())
        val responses: Future[Seq[Message]] =
          rawResponses flatMap { reps =>
            Future.collect(reps map liftResponse) respond {
              case Return(rs) =>
                log.debug(s"success lifting ${rs.head.endpoint}")

              case Throw(t) => log.debug(t, "error lifting")
            }
          }
        log.info("Raw response : " + responses.get().toString())
        log.info("Request number : " + requestNo)

        responses foreach {
          case Seq(primaryResponse, candidateResponse, secondaryResponse) =>
            liftRequest(req) respond {
              case Return(m) =>
                log.debug(s"success lifting request for ${m.endpoint}")

              case Throw(t) => log.debug(t, "error lifting request")
            } foreach { req =>
              analyzer(req, candidateResponse, primaryResponse, secondaryResponse)
            }
        }

        rawResponses = Future.Nil
      }
      requestNo = (requestNo + 1) % 3
      NoResponseExceptionFuture
    }
  }

  def clear() = {
    lastReset = Time.now
    analyzer.clear()
  }
}
