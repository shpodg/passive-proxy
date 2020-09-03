package me.shpodg.passiveproxy.client

import io.rsocket.Payload
import io.rsocket.RSocket
import io.rsocket.SocketAcceptor
import io.rsocket.frame.decoder.PayloadDecoder
import io.rsocket.util.DefaultPayload
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.reactivestreams.Publisher
import org.reactivestreams.Subscriber
import org.reactivestreams.Subscription
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.messaging.rsocket.RSocketRequester
import org.springframework.util.MimeTypeUtils
import reactor.core.CoreSubscriber
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.UnicastProcessor
import reactor.kotlin.core.publisher.toFlux
import reactor.netty.tcp.TcpClient
import java.util.concurrent.CountDownLatch
import kotlin.concurrent.timer


@SpringBootApplication
class PassiveProxyClientApplication : ApplicationRunner, RSocket {

    @Value("\${proxy.server.ip}")
    private lateinit var serverHost: String

    @Value("\${proxy.server.port}")
    private var serverPort: Int = 9898

    val shutdown = CountDownLatch(1)

    override fun run(args: ApplicationArguments?) {
        val client = RSocketRequester.builder()
                .dataMimeType(MimeTypeUtils.ALL)
                .metadataMimeType(MimeTypeUtils.ALL)
                .rsocketConnector {
                    it.payloadDecoder(PayloadDecoder.ZERO_COPY)
                    it.acceptor(SocketAcceptor.with(this@PassiveProxyClientApplication))
                }
                .connectTcp(serverHost, serverPort).block()!!
        client.rsocket().onClose().doFinally {
            println("server exited!")
            shutdown.countDown()
        }.subscribe()
        shutdown.await()

    }

    override fun requestChannel(payloads: Publisher<Payload>): Flux<Payload> {
        val processor = UnicastProcessor.create<Payload>()
        TcpClient.create()
                .host("127.0.0.1").port(1080).handle { inbound, outbound ->
                    val inThen = inbound.receive()
                            .doOnComplete { processor.onComplete() }
                            .doOnNext { processor.onNext(DefaultPayload.create(it.retain())) }
                            .then()
                    val outThen = outbound.send(payloads.toFlux().map { it.data() })
                            .neverComplete()
                    Flux.zip(inThen, outThen).then()
                }.connect().subscribe()
        return processor
    }

//    override fun requestChannel(payloads: Publisher<Payload>): Flux<Payload> {
//        return payloads.toFlux().map { it.dataUtf8 }.map { "echo :${it}" }.map { DefaultPayload.create(it) }
//    }


}

fun main(args: Array<String>) {
    runApplication<PassiveProxyClientApplication>(*args)
}
