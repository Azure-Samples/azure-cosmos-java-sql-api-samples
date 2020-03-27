# Reactor Pattern Guide

The purpose of this guide is to help you get started using Reactor-based Java SDKs by understanding Reactor-based design patterns. It is recommended to read the [Project Reactor](https://projectreactor.io/docs/core/3.1.2.RELEASE/reference/) documentation if you want to learn more.

## Background

### 1. Reactive Programming and the Reactive Streams Standard

Reactive Programming is a declarative programming paradigm in which program operation and control flow are described as a stream of data items passing through a pipeline of operations. Each operation affects the data which flows downstream from it. Reactive Programming is a useful technique (through not the only technique) for event-driven asynchronous programming.

**Imperative programming** is the more common or "familiar" programming paradigm in which program operation and control flow are expressed by sequential commands which manipulate program state (variables). A simple imperative program in pseudocode is

    If input data available, read into variable x
    Do operation1 on variable x
    Then do operation2 on variable y
    Then do operation3 on variable z
    And then print the result

Specifically, Reactive Programming is a **declarative dataflow** paradigm - the programmer must describe a directed acyclic graph (DAG) of operations which represents the logic of the program and the flow of data. A simple declarative dataflow representation of the above program in pseudocode is:

    asynchronous data source => operation1 => operation2 => operation3 => print

How this differs from imperative programming, is that the coder is describing the high-level process of execution but letting the language implementation decide when and how to implement these operations. This is exemplified by the concept of *back-pressure* which is baked into some implementations of Reactive Programming. Back-pressure essentially rate-limits dataflow in a Reactive Stream based on what the recipient of the data can handle. An imperative implementation of back-pressure would require the programmer to describe a complicated flow-control process for each async operation to respond to events. In a declarative dataflow language with back-pressure, the programmer specifies the directed graph of pipelined operations while the language handles scheduling of operations at the implementation level to ensure that no operation receives data faster than it can process.

[Reactive Streams](http://www.reactive-streams.org/) is an industry standard for declarative dataflow programming in an asynchronous environment. More detail on design principles can be found in the [Reactive Manifesto](https://www.reactivemanifesto.org/). It is the basis for new Azure's new async SDKs.

### 2. Reactive Streams Frameworks for Java/JVM

Reactive Streams frameworks implement the Reactive Streams Standard for specific programming languages. [RxJava](https://github.com/ReactiveX/RxJava) ([ReactiveX](reactivex.io/) for JVM) was the basis of past Azure Java SDKs, but will not be going forward.

[Project Reactor](https://projectreactor.io/) or just *Reactor* is the Reactive Programming framework being used for new Azure Java SDKs. The purpose of the rest of this document is to help you get started with Reactor.

## Reactor design patterns

### 1. Assemble and Subscribe phases

To write a program using Reactor, you will need to describe one or more async operation pipelines for processing Reactive Streams. In typical uses of Reactor, you describe a pipeline by

1. Creating a ```Publisher``` (which pushes events and data into the pipeline asynchronously) and a ```Subscriber``` (which consumes events and data from the pipeline and operates on them asynchronously)

2. Describing each stage in the pipeline programmatically, in terms of how it processes data from the previous stage.

Reactor follows a "hybrid push-pull model": the ```Publisher``` pushes events and data into the pipeline as they are available, but ***only*** once you request events and data from the ```Publisher``` via a ```Subscription```.

To put this in context, consider a "normal" non-Reactor program you might write that takes takes a dependency on some other code with unpredictable response time. For example, maybe you write a function to perform a calculation, and one input comes from calling a function that requests data over TCP/IP. You might deal with this by implementing a control flow which first calls the dependency code, waits for it to return output, and then provides that output to your code as input. So your code is “pulling” output from its dependency on an on-demand basis. This can be inefficient if there is latency in the dependency (as is the case for a TCP/IP request example); your code has to loop waiting for the dependency.

In a "push" model the dependency signals your code to consume the TCP/IP request response on an "on-availability" basis; the rest of the time, your code lies dormant, freeing up CPU cycles. This is an event-driven and async approach. But in order for the dependency to signal your code, ***the dependency has to know that your code depends on it*** – and that is the purpose of defining async operation pipelines in Reactor; each pipeline stage is really a piece of async code servicing events and data from the previous stage on an on-availability basis. By defining the pipeline, you tell each stage where to forward events and data to.

Now I will illustrate this with Reactor code examples.

**Assembly phase (define dependency relations as a pipeline)**
```java
Flux<String> reminderPipeline = 
ReminderAsyncService.getRemindersPublisher()
                    .flatMap(reminder -> “Don’t forget: ” + reminder)
                    .flatMap(strIn -> LocalDateTime.now().toString() + “: ”+ strIn); // Nothing executed yet
```

**Subscribe phase (execute pipeline on incoming events)**
```java
reminderPipeline.subscribe(System.out::println); // Async – returns immediately, pipeline executes in the background

while (true) doOtherThings(); // We’re freed up to do other tasks 😊
```

The ```Flux<T>``` class internally represents an async operation pipeline as a DAG and provides instance methods for operating on the pipeline. As we will see ```Flux<T>``` is not the only Reactor class for representing pipelines but it is the general-purpose option. 

In the **Assembly phase** shown above, you describe program logic as an async operation pipeline (a ```Flux<T>```), but don't actually execute it just yet. Let's break down how the async operation pipeline is built in the **Assembly phase** snippet above:

* **Stage 1**: ```ReminderAsyncService.getRemindersPublisher()``` returns a ```Flux<String>``` representing a ```Publisher``` instance for publishing reminders. 

* **Stage 2**: ```.flatMap(reminder -> “Don’t forget: ” + reminder)``` modifies the ```Flux<String>``` from **Stage 1** and returns an augmented ```Flux<String>``` that represents a two-stage pipeline consisting of the reminders ```Publisher``` followed by the ```reminder -> “Don’t forget: ” + reminder``` operation which prepends "Don't forget: " to the ```reminder``` string (```reminder``` is a variable that can have any name and represents the previous stage output.)
	
* **Stage 3**: ```.flatMap(strIn -> LocalDateTime.now().toString() + “: ”+ strIn)``` modifies the ```Flux<String>``` from **Stage 2** and returns a further-augmented ```Flux<String>``` that represents a three-stage pipeline consisting of the reminders ```Publisher```, the **Stage 2** operation, and finally the ```strIn -> LocalDateTime.now().toString() + “: ”+ strIn``` operation, which timestamps the **Stage 2** output string.  So hypothetically if you defined an async operation pipeline which ate int’s and spat out Strings, the output of the assembly phase would be a Flux<String> representing the pipeline.

In the **Subscribe phase** you execute the pipeline that you defined in the Assembly phase. Here is how that works. You call
    
```java
reminderPipeline.subscribe(System.out::println); //Async – returns immediately
```
    
and 

* ```subscribe()``` will generate a ```Subscription``` instance requesting ***all*** events that ```RemindersPublisher``` will ever produce. 

* Reactor framework propagates this ```Subscription``` instance backwards up the pipeline to the ```RemindersPublisher``` instance. 

* The ```RemindersPublisher``` instance reads this ```Subscription``` and responds by pushing an event into the pipeline every time there is a new reminder. The publisher will continue to push an event every time a reminder becomes available, until it has pushed as many events as were requested in the ```Subscription``` (which is infinity in this case, so the ```Publisher``` will just keep going.)

When I say that the ```Publisher``` "pushes events into the pipeline", I mean that the ```Publisher``` issues an ```onNext``` signal to the second pipeline stage (```.flatMap(reminder -> “Don’t forget: ” + reminder)```) paired with a ```String``` argument containing the reminder. ```flatMap()``` responds to an ```onNext``` signal by taking the ```String``` data passed in and applying the transformation that is in ```flatMap()```'s argument parentheses to the input data (in this case, by prepending the words “Don’t forget: ”). This signal propagates down the pipeline: pipeline Stage 2 issues an ```onNext``` signal to pipeline Stage 3 (```.flatMap(strIn -> LocalDateTime.now().toString() + “: ”+ strIn)```) with its output as the argument; and then pipeline Stage 3 issues its own output along with an ```onNext``` signal. 

Now what happens after next is different – the ```onNext``` signal reached the last pipeline stage, so what happens to final-stage ```onNext``` signal and its associated ```String``` argument? The answer is that when you called ```subscribe()```, ```subscribe()``` also created a ```Subscriber``` instance which implements a method for handling ```onNext``` signals and serves as the last stage of the pipeline. The ```Subscriber```'s ```onNext``` handler will call whatever code you wrote in the argument parentheses of ```subscribe()```, allowing you to customize for your application. In the Subscribe phase snippet above, we called

```java
reminderPipeline.subscribe(System.out::println); //Async – returns immediately
``` 

which means that every time an ```onNext``` signal reaches the end of the operation pipeline, the ```Subscriber``` will call ```System.out.println()``` on the reminder ```String``` associated with the event and print it to the terminal.

In the TCP/IP example I touch on earlier, ```subscribe()``` is analogous to your program, and the rest of the pipeline is analogous to the TCP/IP request dependency which your program services on an on-availability basis. In ```subscribe()``` you typically want to handle the pipeline output with some finality, i.e. by printing it to the terminal, displaying it in a GUI, running a calculation on it, etc. or doing something else before discarding the data entirely. That said, Reactor does allow you can call ```subscribe()``` with no arguments and just discard incoming events and data - in that case you would implement all of the logic of your program in the preceding pipeline stages, including saving the results to a global variable or printing them to the terminal.

That was a lot. So let’s step back for a moment and mention a few key points.
* Keep in mind that Reactor is following a hybrid push-pull model where async events are published at a rate requested by the subscriber.
* Observe that a ```Subscription``` for N events is a type of pull operation from the ```Subscriber```. The ```Publisher``` controls the rate and timing of pushing events, until it exhausts the N events requested by the ```Subscriber```, and then it stops
* This enables the implementation of ***backpressure***, whereby the ```Subscriber``` can size ```Subscription``` counts to adjust the rate of ```Publisher``` events if they are coming too slow or too fast to process.
* ```subscribe()``` is Reactor’s built-in subscription generator, by default it requests all events from the ```Publisher```. [See the Project Reactor documentation here](https://projectreactor.io/docs/core/3.1.2.RELEASE/reference/) for more guidance on customizing the subscription process.

### 2. ```Flux<T>```, ```Mono<T>```, and ```subscribe()```

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

Mono example

```java
Mono<String> exampleMono = Mono
```

### 3. A selection of useful Reactor operations

## For More Information

* If you would like to learn more about Project Reactor and Reactive Streams, or get started writing code using Reactor, you can visit [the Project Reactor website.](https://projectreactor.io/)

* [A gentle introduction to Reactor from tech.io](https://tech.io/playgrounds/929/reactive-programming-with-reactor-3/Intro)

* Reactive Extensions for the JVM (RxJava), a project of ReactiveX **which is no longer used by Cosmos DB** but was previously used to facilitate non-blocking access in Async Java SDK v2.x.x and below.
