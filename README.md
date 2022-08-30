# aws-state-machine

This project addresses the problem of server side state. Server side state needs to be systematically addressed as there is a lot of ad-hoc code surrounding persistence.

Mistakes in state can lead to numerous synchronization and logical errors in server side code.

# Solving the problem

The problem of server side state can be greatly simplified by modeling the state as Finite State Machines clearly defining each state, and transitions from one state to another.

In this approach each method in the class becomes a handler for that state, and method decorators provide the transition logic for moving from state to state.

When a state is transitioned to the data for that state is synchronized from dynamodb. Timestamps and diff'ing of the object data automatically update the object.

# Implementation

StateMachine is a package with a base class providing the object polymorphism and a method decorater implementing the state transitions.
 

