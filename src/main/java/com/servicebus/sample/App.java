package com.servicebus.sample;

import com.azure.core.util.Configuration;
import com.azure.core.util.ConfigurationBuilder;
import com.azure.core.util.CoreUtils;
import com.azure.core.util.logging.ClientLogger;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import com.azure.messaging.servicebus.ServiceBusReceivedMessage;
import com.azure.messaging.servicebus.ServiceBusReceiverAsyncClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import mutiny.zero.flow.adapters.AdaptersToFlow;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;

public class App {
    private final static String CON_STR = System.getenv("CON_STR");
    private final static String Q_NAME = System.getenv("Q_NAME");
    private final static String DISABLE_BETA = System.getenv("DISABLE_BETA");
    private final ClientLogger logger;


    public static void main( String[] args ) throws InterruptedException {
        final App app = new App();
        app.sample();
    }

    private App() {
        this.logger = new ClientLogger(App.class);
    }

    private void sample() throws InterruptedException {
        final ServiceBusReceiverAsyncClient lowLevelReceiver;
        final boolean shouldDisableBeta = !CoreUtils.isNullOrEmpty(DISABLE_BETA);
        if (shouldDisableBeta) {
            // Explicitly Disable preview (beta,v2) path for the Service Bus SDK version 7.15.0-beta.2 to function exactly same as GA'd version 7.14.2.
            final Configuration configuration = new ConfigurationBuilder()
                .putProperty("com.azure.messaging.servicebus.nonSession.asyncReceive.v2", "false")
                .build();
            lowLevelReceiver = new ServiceBusClientBuilder()
                    .connectionString(CON_STR)
                    .configuration(configuration)
                    .receiver()
                    // IMPORTANT: Disable flawed & deprecated auto-complete & auto-lockrenew feature in Low-level Receiver
                    // https://github.com/Azure/azure-sdk-for-java/issues/26084
                    .disableAutoComplete()
                    .maxAutoLockRenewDuration(Duration.ZERO)
                    .queueName(Q_NAME)
                    .buildAsyncClient();
        } else {
            // By default, Service Bus SDK version 7.15.0-beta.2 has preview (beta,v2) path enabled.
            lowLevelReceiver = new ServiceBusClientBuilder()
                    .connectionString(CON_STR)
                    .receiver()
                    // IMPORTANT: Disable flawed & deprecated auto-complete & auto-lockrenew feature in Low-level Receiver
                    // https://github.com/Azure/azure-sdk-for-java/issues/26084
                    .disableAutoComplete()
                    .maxAutoLockRenewDuration(Duration.ZERO)
                    .queueName(Q_NAME)
                    .buildAsyncClient();
        }

        final Flow.Publisher<ServiceBusReceivedMessage> messagePublisher = AdaptersToFlow.publisher(lowLevelReceiver.receiveMessages());

        Multi<ServiceBusReceivedMessage> asbMessages = Multi.createFrom().publisher(messagePublisher)
                .onFailure().invoke(throwable -> logger.error("Message Streaming error", throwable))
                .onCompletion().invoke(() -> logger.error("Message Streaming completed"))
                //.log("Beginning")
                .select()
                .where(message -> isRunning())
                //.log("Before onRequest")
                .onRequest().call(acceptRequestNowOrDelay(logger))
                //.log("After onRequest")
                .select()
                .when(shouldAcceptMessage());
                //.log("After select");

        asbMessages.onItem().transformToUniAndMerge(message -> {
            logger.atInfo().log("Processed and completing Message: " + message.getLockToken());
            final Mono<ServiceBusReceivedMessage>  dispositionMessage = lowLevelReceiver.complete(message)
                            .onErrorResume(e -> {
                                // Do not terminate the message publishing stream on disposition error.
                                logger.atInfo().log("Ignoring the disposition error to continue...", e);
                                return Mono.empty();
                            })
                            .thenReturn(message);
            final Flow.Publisher<ServiceBusReceivedMessage> completionPublisher = AdaptersToFlow.publisher(dispositionMessage);
            final Uni<ServiceBusReceivedMessage> completionUni = Uni.createFrom().publisher(completionPublisher);
            // IMPORTANT: Immediately free the Reactor IO (Single) Thread delivering Complete ACK by returning on Worker Pool.
            return completionUni.emitOn(Infrastructure.getDefaultWorkerPool());
        })
        .subscribe(new Flow.Subscriber<ServiceBusReceivedMessage>() {
            private Flow.Subscription subscription;
            @Override
            public void onSubscribe(Flow.Subscription subscription) {
                logger.atInfo().log("onSubscribe: Requesting First.");
                this.subscription = subscription;
                this.subscription.request(1);
            }

            @Override
            public void onNext(ServiceBusReceivedMessage message) {
                logger.atInfo().log("onNext: Requesting Next");
                this.subscription.request(1);
            }

            @Override
            public void onError(Throwable throwable) {
                logger.atInfo().log("ON_ERROR", throwable);
            }

            @Override
            public void onComplete() {
                logger.atInfo().log("ON_COMPLETE: ");
            }
        });

        TimeUnit.MINUTES.sleep(5);
        lowLevelReceiver.close();
    }

    private boolean isRunning() {
        return true;
    }

    private Function<ServiceBusReceivedMessage, Uni<Boolean>> shouldAcceptMessage() {
        return message -> Uni.createFrom().item(true);
    }

    private static Supplier<Uni<?>> acceptRequestNowOrDelay(ClientLogger logger) {
        return () -> {
            Random rand = new Random();
            if (rand.nextInt(100) <= 25) {
                Duration delayRequestFor = Duration.ofSeconds(5);
                logger.info("Delaying request for {}.", delayRequestFor);
                return Uni.createFrom().voidItem().onItem().delayIt().by(delayRequestFor);
            } else {
                return Uni.createFrom().voidItem();
            }
        };
    }
}