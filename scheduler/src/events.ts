import * as utils from "@data-heaving/common";

// This is virtual interface - no instances implementing this are ever created
export interface VirtualGlobalMutextJobSpecificEvents<TArg> {
  jobScheduled: { name: string; arg: TArg; timeToStartInMs: number };
  jobStarting: { name: string; arg: TArg };
  jobEnded: { name: string; arg: TArg; durationInMs: number; error?: Error };
}
export type VirtualGlobalMutexSchedulerEvents<
  TArg
> = VirtualGlobalMutextJobSpecificEvents<TArg>;

export type GlobalMutexSchedulerEventEmitter<TArg> = utils.EventEmitter<
  VirtualGlobalMutexSchedulerEvents<TArg>
>;
export type GlobalMutexSchedulerEventBuilder<TArg> = utils.EventEmitterBuilder<
  VirtualGlobalMutexSchedulerEvents<TArg>
>;
export type GlobalMutexSchedulerJobSpecificEventAddition<
  TArg
> = utils.EventEmitterRegistrationAddition<
  VirtualGlobalMutextJobSpecificEvents<TArg>
>;

export const createEventEmitterBuilder = <TArg>() =>
  new utils.EventEmitterBuilder<VirtualGlobalMutexSchedulerEvents<TArg>>();

export const consoleLoggingEventEmitterBuilder = <TArg>(
  logMessagePrefix?: Parameters<typeof utils.createConsoleLogger>[0],
  builder?: utils.EventEmitterBuilder<VirtualGlobalMutexSchedulerEvents<TArg>>,
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
