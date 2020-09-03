package me.shpodg.passiveproxy.server

import io.rsocket.util.DefaultPayload
import org.reactivestreams.Publisher
import org.slf4j.LoggerFactory
import org.springframework.boot.ApplicationArguments
import org.springframework.boot.ApplicationRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.messaging.rsocket.RSocketRequester
import org.springframework.messaging.rsocket.annotation.ConnectMapping
import org.springframework.stereotype.Controller
import reactor.core.publisher.Flux
import reactor.kotlin.core.publisher.toFlux
import reactor.netty.NettyInbound
import reactor.netty.NettyOutbound
import reactor.netty.tcp.TcpClient
import reactor.netty.tcp.TcpServer
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.random.Random

@Controller
@SpringBootApplication
class PassiveProxyServerApplication : ApplicationRunner {

    val log = LoggerFactory.getLogger(PassiveProxyServerApplication::class.java)

    private val random = Random.Default

    private val clients = CopyOnWriteArrayList<RSocketRequester>()


    @ConnectMapping
    fun handle(requester: RSocketRequester) {
        requester.rsocket().onClose().doFirst {
            clients.add(requester)
            log.info("client accept {}", clients.size)
        }.doOnError {
            log.warn(it.message)
        }.doFinally {
            clients.remove(requester)
            log.info("client removed {}", clients.size)
        }.subscribe()

    }

    private fun handle(inbound: NettyInbound, outbound: NettyOutbound): Publisher<Void> {
        val client = selectClient()
        val request = inbound.receive().map { DefaultPayload.create(it.retain()) }
        val response = client.rsocket().requestChannel(request)

        return outbound.send(response.map { it.data() }).neverComplete()
    }

    override fun run(args: ApplicationArguments?) {
        TcpServer.create()
                .handle(this::handle)
                .host("0.0.0.0")
                .port(1081).bindNow()
    }

    fun selectClient(): RSocketRequester {
        val nextInt = random.nextInt(clients.size)
        return clients[nextInt]
    }


}

fun main(args: Array<String>) {
    runApplication<PassiveProxyServerApplication>(*args)
}
