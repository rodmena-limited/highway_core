this is an article i read. see if there is any idea can useful for our workflow engine.
we don't need to push ideas into our system, but we can consider them

if you find anything can that be featured, first create a PLAN.md and then i'll let you know if that's fine.

========================== IDEA ================


Absurd Workflows: Durable Execution With Just Postgres
written on November 03, 2025

It’s probably no surprise to you that we’re building agents somewhere. Everybody does it. Building a good agent, however, brings back some of the historic challenges involving durable execution.

Entirely unsurprisingly, a lot of people are now building durable execution systems. Many of these, however, are incredibly complex and require you to sign up for another third-party service. I generally try to avoid bringing in extra complexity if I can avoid it, so I wanted to see how far I can go with just Postgres. To this end, I wrote Absurd 1, a tiny SQL-only library with a very thin SDK to enable durable workflows on top of just Postgres — no extension needed.

Durable Execution 101
Durable execution (or durable workflows) is a way to run long-lived, reliable functions that can survive crashes, restarts, and network failures without losing state or duplicating work. Durable execution can be thought of as the combination of a queue system and a state store that remembers the most recently seen execution state.

Because Postgres is excellent at queues thanks to SELECT ... FOR UPDATE SKIP LOCKED, you can use it for the queue (e.g., with pgmq). And because it’s a database, you can also use it to store the state.

The state is important. With durable execution, instead of running your logic in memory, the goal is to decompose a task into smaller pieces (step functions) and record every step and decision. When the process stops (whether it fails, intentionally suspends, or a machine dies) the engine can replay those events to restore the exact state and continue where it left off, as if nothing happened.

Absurd At A High Level
Absurd at the core is a single .sql file (absurd.sql) which needs to be applied to a database of your choice. That SQL file’s goal is to move the complexity of SDKs into the database. SDKs then make the system convenient by abstracting the low-level operations in a way that leverages the ergonomics of the language you are working with.

The system is very simple: A task dispatches onto a given queue from where a worker picks it up to work on. Tasks are subdivided into steps, which are executed in sequence by the worker. Tasks can be suspended or fail, and when that happens, they execute again (a run). The result of a step is stored in the database (a checkpoint). To avoid repeating work, checkpoints are automatically loaded from the state storage in Postgres again.

Additionally, tasks can sleep or suspend for events and wait until they are emitted. Events are cached, which means they are race-free.

With Agents
What is the relationship of agents with workflows? Normally, workflows are DAGs defined by a human ahead of time. AI agents, on the other hand, define their own adventure as they go. That means they are basically a workflow with mostly a single step that iterates over changing state until it determines that it has completed. Absurd enables this by automatically counting up steps if they are repeated:

absurd.registerTask({name: "my-agent"}, async (params, ctx) => {
  let messages = [{role: "user", content: params.prompt}];
  let step = 0;
  while (step++ < 20) {
    const { newMessages, finishReason } = await ctx.step("iteration", async () => {
      return await singleStep(messages);
    });
    messages.push(...newMessages);
    if (finishReason !== "tool-calls") {
      break;
    }
  }
});
This defines a single task named my-agent, and it has just a single step. The return value is the changed state, but the current state is passed in as an argument. Every time the step function is executed, the data is looked up first from the checkpoint store. The first checkpoint will be iteration, the second iteration#2, iteration#3, etc. Each state only stores the new messages it generated, not the entire message history.

If a step fails, the task fails and will be retried. And because of checkpoint storage, if you crash in step 5, the first 4 steps will be loaded automatically from the store. Steps are never retried, only tasks.

How do you kick it off? Simply enqueue it:




========================== END OF IDEA ================
