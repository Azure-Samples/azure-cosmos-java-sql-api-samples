# Reactive Pattern Guide: A Guide for Reactive Programming with Reactor

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

The purpose of the rest of this document is to help you start using Reactor with as little trouble as possible. This includes suggestions for upgrading your code from RxJava to Reactor and also Reactor design pattern guidelines.

## Reactor Design Patterns

To write a program using Reactor, you will need to describe one or more Reactive Streams. In typical uses of Reactor, you describe a stream
by (1) creating a *Publisher* (which originates data asynchronously) and a *Subscriber* (which consumes data and operates on it asynchronously), and (2)
describing a pipeline from Publisher to Subscriber, in which the data from Publisher is transformed at each pipeline stage before eventually
ending in Subscriber. In this section we will discuss this process in more detail and demonstrate how Reactor lets you define the transforming operation at each
pipeline stage.

### 1. ***Describing a Reactive Stream (A Publisher-Subscriber Pipeline)***

Here is a simple Reactive Stream:

```java
Flux.just("Hello","Cosmos DB")
    .subscribe(System.out::println);
```

The Publisher is ``` Flux.just("Hello","Cosmos DB") ```. ```Flux.just()``` is a *Reactor factory method* which allows you to define a Publisher.
``` Flux.just("Hello","Cosmos DB") ``` will asynchronously send ```Hello``` and ```Cosmos DB``` as two Strings to the next stage of the Publisher-Subscriber
pipeline.

Here, the Publisher-Subscriber pipeline is simple - the next pipeline stage after the Publisher is the Subscriber, ```.subscribe(System.out::println)```, which
will receive the two Strings as they arrive from upstream and process them by applying ```System.out::println``` to each one, again asynchronously.
The output would be

```java
Hello
Cosmos DB
```

This is a simple Publisher-Subscriber pipeline with no operations to transform the data.
The call to ```subscribe()``` is what ultimately triggers data to flow through the Reactive Stream
and carry out the logic of your program. Simply calling

```java
Flux.just("Hello","Cosmos DB");
```

without calling ```subscribe()``` will **not** execute the logic of your program; this line will simply return a ```Flux<String>``` which represents
the pipeline of operations starting from the Publisher (which in this case, consists only of the Publisher). This ```Flux<String>``` can be stored in a
variable and used like any other variable. For example you can return its value and use that value elsewhere in the program, i.e. by subscribing to it in another function:

```java
private Flux<String> some_function() {
    return Flux.just("Hello","Cosmos DB");
}

public void calling_function() {
    Flux<String> str_flux = some_function(); //Returns a representation of a Reactive Stream
    str_flux.subscribe(System.out::println); //Produces the same output as the original example, by subscribing to the Reactive Stream
}
```

This approach of defining Reactive Streams and then subscribing to them later can be useful - **just remember that the logic of your Reactive Stream will
not be executed until you ```subscribe()``` to it.**

TODO:
* Introduce ```Mono``` and ```Flux```
* Reactor Factory Methods
* Operations to transform data - ```flatMap()```, ```reduce()```, nested imperative code
* More about ```subscribe()``` and ```onNext()```/```onComplete```/```onError``` and ```block()```
* ```subscribeOn()```, ```publishOn```, schedulers
* ```onSuccess()```

## Reactor vs RxJava

* Compare all of the above between Reactor and RxJava
* rxJava observeOn <-> reactive stream publishOn
* rxJava subscribeOn <-> reactive stream subscribeOn

## For More Information

* If you would like to learn more about Project Reactor and Reactive Streams, or get started writing code using Reactor, you can visit [the Project Reactor website.](https://projectreactor.io/)

* [A gentle introduction to Reactor from tech.io](https://tech.io/playgrounds/929/reactive-programming-with-reactor-3/Intro)

* Reactive Extensions for the JVM (RxJava), a project of ReactiveX **which is no longer used by Cosmos DB** but was previously used to facilitate non-blocking access in Async Java SDK v2.x.x and below.
