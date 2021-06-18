import * as common from "@data-heaving/common";

// This is virtual interface - no instances implementing this are ever created
export interface VirtualSchedulerEvents {
  jobScheduled: { jobID: string; timeToStartInMs: number };
  jobStarting: { jobID: string };
  jobEnded: { jobID: string; durationInMs: number; error?: Error };
}

export type SchedulerEventEmitter = common.EventEmitter<VirtualSchedulerEvents>;
export type SchedulerEventBuilder = common.EventEmitterBuilder<VirtualSchedulerEvents>;
export type SchedulerJobSpecificEventAddition = common.EventEmitterRegistrationAddition<VirtualSchedulerEvents>;

export const createEventEmitterBuilder = () =>
  new common.EventEmitterBuilder<VirtualSchedulerEvents>();

export const consoleLoggingEventEmitterBuilder = (
  logMessagePrefix?: Parameters<typeof common.createConsoleLogger>[0],
  builder?: common.EventEmitterBuilder<VirtualSchedulerEvents>,
  consoleAbstraction?: common.ConsoleAbstraction,
) => {
  if (!builder) {
    builder = createEventEmitterBuilder();
  }

  const logger = common.createConsoleLogger(
    logMessagePrefix,
    consoleAbstraction,
  );

  builder.addEventListener("jobScheduled", (arg) =>
    logger(
      `Starting scheduling job ${arg.jobID}, scheduled to start in ${arg.timeToStartInMs} ms.`,
    ),
  );
  builder.addEventListener("jobStarting", (arg) =>
    logger(`Executing job ${arg.jobID}`),
  );
  builder.addEventListener("jobEnded", (arg) =>
    logger(
      `Done executing job ${arg.jobID}, duration: ${
        arg.durationInMs
      }ms, completed ${
        "error" in arg
          ? `with an error ${arg.error}`
          : "without unexpected problems"
      }.`,
      "error" in arg,
    ),
  );
  return builder;
};
