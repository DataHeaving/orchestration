import * as utils from "@data-heaving/common";

// This is virtual interface - no instances implementing this are ever created
export interface VirtualSchedulerEvents {
  jobScheduled: { name: string; timeToStartInMs: number };
  jobStarting: { name: string };
  jobEnded: { name: string; durationInMs: number; error?: Error };
}

export type SchedulerEventEmitter = utils.EventEmitter<VirtualSchedulerEvents>;
export type SchedulerEventBuilder = utils.EventEmitterBuilder<VirtualSchedulerEvents>;
export type SchedulerJobSpecificEventAddition = utils.EventEmitterRegistrationAddition<VirtualSchedulerEvents>;

export const createEventEmitterBuilder = () =>
  new utils.EventEmitterBuilder<VirtualSchedulerEvents>();

export const consoleLoggingEventEmitterBuilder = (
  logMessagePrefix?: Parameters<typeof utils.createConsoleLogger>[0],
  builder?: utils.EventEmitterBuilder<VirtualSchedulerEvents>,
) => {
  if (!builder) {
    builder = createEventEmitterBuilder();
  }

  const logger = utils.createConsoleLogger(logMessagePrefix);

  builder.addEventListener("jobScheduled", (arg) =>
    logger(
      `Starting scheduling job ${arg.name}, scheduled to start in ${arg.timeToStartInMs} ms.`,
    ),
  );
  builder.addEventListener("jobStarting", (arg) =>
    logger(`Executing job ${arg.name}`),
  );
  builder.addEventListener("jobEnded", (arg) =>
    logger(
      `Done executing job ${arg.name}, duration: ${
        arg.durationInMs
      }, completed ${
        "error" in arg
          ? `with an error ${arg.error}`
          : "without unexpected problems."
      }.`,
    ),
  );
  return builder;
};
