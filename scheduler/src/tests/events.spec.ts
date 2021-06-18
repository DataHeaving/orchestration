import * as common from "@data-heaving/common";
import test, { ExecutionContext } from "ava";
import * as spec from "../events";

test("Console event logging works as expected", (t) => {
  performConsoleLoggingTest(t, undefined);
});

test("Passing existing event emitter builder will not make new one", (t) => {
  const givenBuilder = new common.EventEmitterBuilder<spec.VirtualSchedulerEvents>();
  const returnedBuilder = performConsoleLoggingTest(t, givenBuilder);
  t.is(returnedBuilder, givenBuilder);
});

const performConsoleLoggingTest = (
  t: ExecutionContext,
  existingBuilder:
    | common.EventEmitterBuilder<spec.VirtualSchedulerEvents>
    | undefined,
) => {
  const seenLogs: Array<string> = [];
  const seenErrors: Array<string> = [];
  existingBuilder = spec.consoleLoggingEventEmitterBuilder(
    undefined,
    existingBuilder,
    {
      log: (msg) => seenLogs.push(msg),
      error: (msg) => seenErrors.push(msg),
    },
  );
  const eventEmitter = existingBuilder.createEventEmitter();

  const jobID = "someJob";
  const timeToStartInMs = 0;
  const durationInMs = 100;
  eventEmitter.emit("jobScheduled", {
    jobID,
    timeToStartInMs,
  });
  eventEmitter.emit("jobStarting", {
    jobID,
  });
  eventEmitter.emit("jobEnded", {
    jobID,
    durationInMs,
  });

  const expectedLogs = [
    `Starting scheduling job ${jobID}, scheduled to start in ${timeToStartInMs} ms.`,
    `Executing job ${jobID}`,
    `Done executing job ${jobID}, duration: ${durationInMs}ms, completed without unexpected problems.`,
  ];
  t.deepEqual(seenLogs, expectedLogs);
  t.deepEqual(seenErrors, []);

  const error = new Error("DummyError");
  eventEmitter.emit("jobEnded", {
    jobID,
    durationInMs,
    error,
  });
  t.deepEqual(seenLogs, expectedLogs);
  t.deepEqual(seenErrors, [
    `Done executing job ${jobID}, duration: ${durationInMs}ms, completed with an error ${error}.`,
  ]);

  return existingBuilder;
};
