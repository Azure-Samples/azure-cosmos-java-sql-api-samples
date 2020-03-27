# Reactive Pattern Guide

## Background: Reactive Programming, Reactive Streams, Reactor, Rx Java, and Project Reactive

### 1. ***Reactive Programming and Standards***

Reactive Programming is a declarative programming paradigm in which program operation and control flow are described as a stream of data items passing through a pipeline of operations in which each operation affects the data which flows downstream.

**Imperative programming** is the more common or "familiar" programming paradigm in which program operation and control flow are expressed by sequential commands which manipulate program state (variables). A simple imperative program in pseudocode is

    If input data available, read into variable x
    Do operation1 on variable x
    Then do operation2 on variable y
    Then do operation3 on variable z
    And then print the result

Reactive Programming is a **declarative** paradigm - specifically a **dataflow** paradigm - in which the programmer must describe a directed graph of operations which represents the logic of the program. A simple declarative dataflow representation of the above program in pseudocode is:

    asynchronous data source => operation1 => operation2 => operation3 => print

How this differs from imperative programming, is that the coder is describing the high-level process of execution but letting the language implementation decide when and how to implement these operations. This is exemplified by the concept of *back-pressure* which is baked into some implementations of Reactive Programming. Back-pressure essentially rate-limits dataflow in a Reactive Stream based on the slowest pipelined operation. An imperative implementation of back-pressure would require the programmer to describe a complicated flow-control process - whereas in a declarative dataflow language with back-pressure, the programmer specifies the directed graph of pipelined operations while the language handles scheduling of operations at the implementation level to ensure that no operation receives data faster than it can process.

[Reactive Streams](http://www.reactive-streams.org/) is an industry standard for declarative dataflow programming in an asynchronous environment. More detail on design principles can be found in the [Reactive Manifesto](https://www.reactivemanifesto.org/). It is the basis for the asynchronous programming libraries which have been used in the Cosmos DB Async Java SDKs.

### 2. ***Available Reactive Streams Frameworks for Java/JVM***
[RxJava](https://github.com/ReactiveX/RxJava) ([ReactiveX](reactivex.io/) for JVM) is no longer being used after Java SDK v2.x.x.

[Project Reactor](https://projectreactor.io/) or just *Reactor* is the Reactive Programming framework used in Java SDK v3.x.x and above.

The purpose of the rest of this document is to help you start using Reactor with as little trouble as possible. 

## Reactor Design Patterns

To write a program using Reactor, you will need to describe one or more Reactive Streams. In typical uses of Reactor, you describe a stream by (1) creating a *Publisher* (which originates data asynchronously) and a *Subscriber* (which consumes data and operates on it asynchronously), and (2) describing a pipeline from Publisher to Subscriber, in which the data from Publisher is transformed at each pipeline stage before eventually ending in Subscriber. 

Reactor follows a "hybrid push-pull model": your code is triggered on an event-driven basis by the Publisher, but ***only*** once you signal the Publisher via a Subscription.

Consider a "typical" program you might be used to writing. You are writing a piece of code, but it takes a dependency on other code with unpredictable response time. For example, maybe you wrote a function to perform a calculation, and one input comes from calling a function that requests data over TCP/IP. You might typically deal with this by implementing a control flow in which you first call the dependency code, wait for it to return output, and then provide that output to the other piece of code as input. So your code is “pulling” output from its dependency on an on-demand basis. This can be inefficient if there is latency in the dependency (as is the case for a TCP/IP request); the next piece of code has to wait.

In a "push" model the dependency signals the next piece of code to consume output when it becomes available; otherwise, your code is dormant, freeing up CPU cycles. This is a more event-driven concept. But in order for the dependency to signal the next piece of code, it has to know that it is a dependency – in a Reactive program we have to define the dependency relations in advance.

```java
Assembly phase (define dependency relations as a pipeline)
Subscribe phase (execute pipeline on incoming events)

Flux<String> reminderPipeline = 
ReminderAsyncService.getRemindersPublisher()
                    .flatMap(reminder -> “Don’t forget: ” + reminder)
                    .flatMap(strIn -> LocalDateTime.now().toString() + “: ”+ strIn); // Nothing executed yet

reminderPipeline.subscribe(System.out::println); // Async – returns immediately
while (true) doOtherThings(); // We’re freed up to do other tasks 😊
```

A Flux in Reactor represents … and is the general-purpose class for doing so. In the assembly phase, you are describing program logic as an async operation pipeline, but not actually executing it yet. So ReminderAsyncService.getRemindersPublisher() returns a Flux<String> representing just the reminders publisher. ReminderAsyncService.getRemindersPublisher().flatMap(reminder -> “Don’t forget: ” + reminder) returns an augmented Flux<String> that represents the reminders publisher followed by the “Don’t forget: ” + reminder operation that consumes the publisher’s output and prepends “Don’t forget: ”. And ReminderAsyncService.getRemindersPublisher().flatMap(reminder -> “Don’t forget: ” + reminder).flatMap(strIn -> LocalDateTime.now().toString() + “: ”+ strIn) returns an even further augmented Flux<String> that represents the reminders publisher followed by the “Don’t forget: ” + reminder operation, followed subsequently by an operation which prepends a timestamp to the output of the previous step. In each case, the output is Flux<T>, where T is the output type of the transformation applied at that stage. So hypothetically if you defined an async operation pipeline which ate int’s and spat out Strings, the output of the assembly phase would be a Flux<String> representing the pipeline.
In the subscription phase you execute what you defined in the assembly phase. Here is how that works. You call
    
    ```java
	reminderPipeline.subscribe(System.out::println); //Async – returns immediately
    ```
    
and subscribe() will generate a Subscription instance requesting all events that RemindersPublisher will ever produce. Reactor framework propagates this Subscription instance backwards up the pipeline to the RemindersPublisher instance. The RemindersPublisher instance reads this Subscription and responds by pushing an event into the pipeline every time there is a new reminder. The publisher will continue to push an event every time a reminder becomes available, until it has pushed as many events as were requested in the Subscription (which is infinity in this case, so the publisher will just keep going.)
When I say that the publisher pushes events into the pipeline, I mean that the publisher issues an onNext signal to the next pipeline stage (“Don’t forget:” + reminder) paired with a String argument containing the reminder. flatMap() responds to an onNext signal by taking the String argument and applying the specified transformation to it (in this case, prepending the words “Don’t forget:”). This signal propagates down the pipeline: the “Don’t forget:” + reminder stage issues an onNext signal to the next stage with its output as the argument; then the LocalDateTime.now().toString() + “:” + strIn stage issues an onNext signal to the next stage with its output as the argument. Now what happens after that is special – we reached the last pipeline stage, so what happens to final-stage onNext signal and its associated String argument? The answer is that the Subscription created by the subscribe() call implements a method for handling onNext signals, which you can customize for your application. This method is expected to handle the pipeline output with some finality, i.e. by printing it to the terminal, displaying it in a GUI, or doing something else before discarding the data entirely. Therefore the Subscription instance serves a dual purpose as the true “last stage” of the pipeline. The argument to subscribe() is the content of the onNext handler. Since we called

```java
reminderPipeline.subscribe(System.out::println); //Async – returns immediately
```

with System.out::println as the argument, now each time Subscription.onNext() is called the associated String output from the pipeline will be printed to the terminal using System.out::println.

That was a lot. So let’s step back for a moment and mention a few key points.
* Assembly phase is when you define a series of async operations and a dependency graph (pipeline) connecting them. 
* Subscribe phase is when you actually execute that logic by creating a subscription. The Subscription doubles as the terminal pipeline stage.
* The async operation pipeline starts with a publisher, which will push events into the pipeline once it receives a Subscription for N events in the subscribe phase. 
* Observe that a Subscription for N events is a type of pull operation, because one piece of code (Subscription) is calling for output from other code that is depends on (Publisher). Publisher controls the rate and timing of events, until it exhausts the N events requested by the Subscriber.
* So keep in mind that Reactor is following a hybrid push-pull model where async events are published at a rate requested by the subscriber. This enables the implementation of backpressure, whereby the subscriber can size subscriptions to adjust the rate of publisher events if they are coming too slow or too fast to process.
* subscribe() is Reactor’s built-in subscription generator, it 

Flux supports publishers with 0, 1, or N events, where N can be finite or infinite. The assembly stage for a publisher with N=3 events is shown below

```java
Flux<String> reminderPipeline = 
    Flux.just(“Wash the dishes”,“Mow the lawn”,”Sleep”) // Publisher, 3 events
        .flatMap(reminder -> “Don’t forget: ” + reminder)
        .flatMap(strIn -> LocalDateTime.now().toString() + “: ”+ strIn); // Nothing executed yet
```

And upon subscription,

```java
reminderPipeline.subscribe(System.out::println);
```

will output the three Strings shown (corresponding to three publisher events pushed into the pipeline) and then stop. Suppose now we want to add two special behaviors to our program: (1) After all N Strings have been printed, print “End of reminders.” so the user knows we are finished. (2) Print any exceptions which occur during execution. A modification to the subscribe() call handles all of this:

```java
reminderPipeline.subscribe(strIn -> {
    System.out.println(strIn);
},
err -> {
    err.printStackTrace();
},
() -> {
    System.out.println(“End of reminders.”);
});
```

Let’s break this down. Remember we said that the argument to subscribe() determines how it handles incoming signals such as onNext? Reactor actually has three important signals which propagate state information along the pipeline: onNext, onComplete, and onError. As shown below, we just modified the subscribe() argument to handle all three:

```java
reminderPipeline.subscribe(strIn -> {
    System.out.println(strIn);
},
err -> {
    err.printStackTrace();
},
() -> {
    System.out.println(“End of reminders.”);
});
```


```java
Mono<String> exampleMono = Mono
```

## For More Information

* If you would like to learn more about Project Reactor and Reactive Streams, or get started writing code using Reactor, you can visit [the Project Reactor website.](https://projectreactor.io/)

* [A gentle introduction to Reactor from tech.io](https://tech.io/playgrounds/929/reactive-programming-with-reactor-3/Intro)

* Reactive Extensions for the JVM (RxJava), a project of ReactiveX **which is no longer used by Cosmos DB** but was previously used to facilitate non-blocking access in Async Java SDK v2.x.x and below.
