# Reactor vs RxJava guide

The purpose of this guide is to help those who are more familiar with the RxJava framework to familiarize themselves with the Reactor framework and Azure Cosmos DB Java SDK 4.0 for Core (SQL) API ("Java SDK 4.0" from here on out.)

Users of Async Java SDK 2.x.x should read this guide to understand how familiar async tasks can be performed in Reactor. We recommend first reading the [Reactor pattern guide](reactor-pattern-guide.md) for more general Reactor introduction.

A quick refresher on Java SDK versions:

| Java SDK                | Release Date | Bundled APIs         | Maven Jar                               | Java package name                |API Reference                                              | Release Notes                                                                            |
|-------------------------|--------------|----------------------|-----------------------------------------|----------------------------------|-----------------------------------------------------------|------------------------------------------------------------------------------------------|
| Async 2.x.x             | June 2018    | Async(RxJava)        | com.microsoft.azure::azure-cosmosdb     | com.microsoft.azure.cosmosdb.rx  | [API](https://azure.github.io/azure-cosmosdb-java/2.0.0/) | [Release Notes](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-sdk-async-java) |
| "Legacy" Sync 2.x.x     | Sept 2018    | Sync                 | com.microsoft.azure::azure-documentdb   | com.microsoft.azure.cosmosdb     | [API](https://azure.github.io/azure-cosmosdb-java/2.0.0/) | [Release Notes](https://docs.microsoft.com/en-us/azure/cosmos-db/sql-api-sdk-java)       |
| 3.x.x                   | July 2019    | Async(Reactor)/Sync  | com.microsoft.azure::azure-cosmos       | com.azure.data.cosmos            | [API](https://azure.github.io/azure-cosmosdb-java/3.0.0/) | -                                                                                        |
| 4.0                     | April 2020   | Async(Reactor)/Sync  | com.azure::azure-cosmos                 | com.azure.cosmos                 | -                                                         | -                                                                                        |

## Background

[Reactive Streams](http://www.reactive-streams.org/) is an industry standard for declarative dataflow programming in an asynchronous environment. More detail on design principles can be found in the [Reactive Manifesto](https://www.reactivemanifesto.org/). It is the basis for Azure's async Java SDKs going forward.

A Reactive Streams framework implements the Reactive Streams Standard for specific programming languages. 

The [RxJava](https://github.com/ReactiveX/RxJava) ([ReactiveX](http://reactivex.io/) for JVM) framework was the basis of past Azure Java SDKs, but will not be going forward. Async Java SDK 2.x.x was implemented using RxJava 1; in this guide we will assume that RxJava 1 is the version you are already familiar with i.e. as a result of working with the Async Java SDK 2.x.x.

[Project Reactor](https://projectreactor.io/) or just *Reactor* is the Reactive Programming framework being used for new Azure Java SDKs. The purpose of the rest of this document is to help you get started with Reactor.

## Comparison between Reactor and RxJava

RxJava 1 provides a framework for implementing the **Observer Pattern** in your application. In the Observer Pattern,
* ```Observable```s are entities that receive events and data (i.e. UI, keyboard, TCP, ...) from outside sources, and make those events and data available to your program.
* ```Observer```s are the entities which subscribe to the Observable events and data.

The [Reactor pattern guide](reactor-pattern-guide.md) gives a brief conceptual overview of Reactor. In summary:
* ```Publisher```s are the entities which make events and data from outside sources available to the program
* ```Subscriber```s subscribe to the events and data from the ```Publisher```

Both frameworks facilitate asynchronous, event-driven programming. Both frameworks allow you to chain together a pipeline of operations between Observable/Observer or Publisher/Subscriber.

Roughly, what you would use an ```Observable``` for in RxJava, you would use a ```Flux``` for in Reactor. And what you would use a ```Single``` for in RxJava, you would use a ```Mono``` for in Reactor.

The critical difference between the two frameworks is really in the core implementation: 
Reactor operates a service which receives event/data pairs serially from a ```Publisher```, demultiplexes them, and forwards them to registered ```Subscribers```. This model was designed to help servers efficiently dispatch requests in a distributed system.
The RxJava approach is more general-purpose. ```Observer```s subscribe directly to the ```Observable``` and the ```Observable``` sends events and data directly to ```Observer```s, with no central service handling dispatch. 

### Summary: rules of thumb to convert RxJava code into Reactor code

* An RxJava ```Observable``` will become a Reactor ```Flux```

* An RxJava ```Single``` will become a Reactor ```Mono```

* An RxJava ```Subscriber``` is still a ```Subscriber``` in Reactor

* Operators such as ```map()```, ```filter()```, and ```flatMap()``` are the same

## Examples of tasks in Reactor and RxJava

* Reminder app example from the [Reactor pattern guide](reactor-pattern-guide.md)

**Reactor:**
```java
ReminderAsyncService.getRemindersPublisher() // Pipeline Stage 1
                    .flatMap(reminder -> "Don't forget: " + reminder) // Stage 2
                    .flatMap(strIn -> LocalDateTime.now().toString() + ": "+ strIn); // Stage 3
                    .subscribe(System.out::println);
```

**RxJava:**
```java
ReminderAsyncService.getRemindersObservable() // Pipeline Stage 1
                    .flatMap(reminder -> "Don't forget: " + reminder) // Stage 2
                    .flatMap(strIn -> LocalDateTime.now().toString() + ": "+ strIn); // Stage 3
                    .subscribe(item -> System.out.println(item));
```

* Three-event ```Publisher``` example from the [Reactor pattern guide](reactor-pattern-guide.md)

**Reactor:**
```java
Flux.just("Wash the dishes","Mow the lawn","Sleep") // Publisher, 3 events
    .flatMap(reminder -> "Don't forget: " + reminder)
    .flatMap(strIn -> LocalDateTime.now().toString() + ": "+ strIn); // Nothing executed yet
    .subscribe(strIn -> {""
           System.out.println(strIn);
    },
    err -> {
        err.printStackTrace();
    },
    () -> {
        System.out.println("End of reminders.");
});
```

**RxJava:**
```java
Observable.just("Wash the dishes","Mow the lawn","Sleep") // Observable, 3 events
    .flatMap(reminder -> "Don't forget: " + reminder)
    .flatMap(strIn -> LocalDateTime.now().toString() + ": "+ strIn); // Nothing executed yet
    .subscribe(strIn -> System.out.println(strIn),
               err -> err.printStackTrace(),
               () -> System.out.println("End of reminders.")
);
```

* Mono example from the [Reactor pattern guide](reactor-pattern-guide.md)

**Reactor:**
```java
Mono.just("Are you sure you want to cancel your Reminders service?") // Publisher, 1 event
    .flatMap(reminder -> "Act now: " + reminder)
    .flatMap(strIn -> LocalDateTime.now().toString() + ": "+ strIn);
    .subscribe(System.out::println);
```

**RxJava:**'
```java
Single.just("Are you sure you want to cancel your Reminders service?") // Publisher, 1 event
    .flatMap(reminder -> "Act now: " + reminder)
    .flatMap(strIn -> LocalDateTime.now().toString() + ": "+ strIn);
    .subscribe(item -> System.out.println(item));
```
