# Guide to Reactive Programming Programming and Reactor Design Patterns with Azure Cosmos DB Java SDK

## Introduction

The purpose of this document is to enable
* users with RxJava experience (i.e. from working with Cosmos DB Async Java SDK v2.x.x)
* and users who are only familiar with the Cosmos DB Legacy Sync SDK

to get started programming with the **Cosmos DB Async Java SDK v3.x.x, v4.x.x and above** by providing background, Reactive Streams programming guidelines, and one-to-one use-case comparison between Project Reactive and RxJava. For Async Java SDK v3.x.x and above, non-blocking requests are implemented using [Project Reactive](https://projectreactor.io/), superseding RxJava which was used in the Async Java SDK v2.x.x. 

**Overview of Asynchronous Library by Cosmos DB Java Async SDK Version**
| Java Async SDK Version | Async Library    |
| :--------------------: | :--------------: |
| 1.x.x                  | RxJava           |
| 2.x.x                  | RxJava           |
| 3.x.x                  | Project Reactive |
| 4.x.x                  | Project Reactive | 



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

### 2. ***Reactive Streams Implementations***
[RxJava](https://github.com/ReactiveX/RxJava) ([ReactiveX](reactivex.io/) for JVM) is no longer being used after Java SDK v2.x.x.

[Project Reactor](https://projectreactor.io/) or just *Reactor* is the Reactive Programming framework used in Java SDK v3.x.x and above.

The purpose of the rest of this document is to help you start using Reactor with as little trouble as possible. This includes suggestions for upgrading your code from RxJava to Reactor and also Reactor design pattern guidelines.

## Getting started quickly with Reactor

### 1. ***At a Glance: RxJava vs Project Reactive: Design Pattern Comparison***

### 2. ***Design Patterns with Project Reactive: Building Publish-Subscribe Pipelines***

# => Functionality

# => Terminology

# Diagrams

# Tips and Tricks

# Troubleshooting

# Hazards

C10K problem

# Examples

# 5. For More Information

* If you would like to learn more about Project Reactor and Reactive Streams, or get started writing code using Reactor, you can visit [the Project Reactor website.](https://projectreactor.io/)

* [A gentle introduction to Reactor from tech.io](https://tech.io/playgrounds/929/reactive-programming-with-reactor-3/Intro)

* Reactive Extensions for the JVM (RxJava), a project of ReactiveX **which is no longer used by Cosmos DB** but was previously used to facilitate non-blocking access in Async Java SDK v2.x.x and below.