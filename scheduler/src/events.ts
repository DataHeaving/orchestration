import * as utils from "@data-heaving/common";

// This is virtual interface - no instances implementing this are ever created
export interface VirtualGlobalMutextJobSpecificEvents {
  jobScheduled: { name: string; timeToStartInMs: number };
  jobStarting: { name: string };
  jobEnded: { name: string; durationInMs: number; error?: Error };
}
export type VirtualGlobalMutexSchedulerEvents = VirtualGlobalMutextJobSpecificEvents;

export type GlobalMutexSchedulerEventEmitter = utils.EventEmitter<VirtualGlobalMutexSchedulerEvents>;
export type GlobalMutexSchedulerEventBuilder = utils.EventEmitterBuilder<VirtualGlobalMutexSchedulerEvents>;
export type GlobalMutexSchedulerJobSpecificEventAddition = utils.EventEmitterRegistrationAddition<VirtualGlobalMutextJobSpecificEvents>;

export const createEventEmitterBuilder = () =>
  new utils.EventEmitterBuilder<VirtualGlobalMutexSchedulerEvents>();

export const consoleLoggingEventEmitterBuilder = (
  logMessagePrefix?: Parameters<typeof utils.createConsoleLogger>[0],
  builder?: utils.EventEmitterBuilder<VirtualGlobalMutexSchedulerEvents>,
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
