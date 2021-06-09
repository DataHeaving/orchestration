import * as common from "@data-heaving/common";

// This is virtual interface - no instances implementing this are ever created
export interface VirtualSchedulerEvents {
  jobScheduled: { name: string; timeToStartInMs: number };
  jobStarting: { name: string };
  jobEnded: { name: string; durationInMs: number; error?: Error };
}

export type SchedulerEventEmitter = common.EventEmitter<VirtualSchedulerEvents>;
export type SchedulerEventBuilder = common.EventEmitterBuilder<VirtualSchedulerEvents>;
export type SchedulerJobSpecificEventAddition = common.EventEmitterRegistrationAddition<VirtualSchedulerEvents>;

export const createEventEmitterBuilder = () =>
  new common.EventEmitterBuilder<VirtualSchedulerEvents>();

export const consoleLoggingEventEmitterBuilder = (
  logMessagePrefix?: Parameters<typeof common.createConsoleLogger>[0],
  builder?: common.EventEmitterBuilder<VirtualSchedulerEvents>,
) => {
  if (!builder) {
    builder = createEventEmitterBuilder();
  }

  const logger = common.createConsoleLogger(logMessagePrefix);

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
