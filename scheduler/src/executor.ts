import * as common from "@data-heaving/common";
import * as events from "./events";

export interface JobInfo {
  timeFromNowToNextInvocation: () => number;
  job: () => Promise<unknown>;
  jobSpecificEvents?:
    | common.EventEmitter<events.VirtualSchedulerEvents>
    | common.EventEmitterBuilder<events.VirtualSchedulerEvents>;
}

type SchedulerReturn = Promise<Array<void>>;
export type SchedulerStopCallback<TJobID extends string> = (
  jobID: TJobID,
) => boolean;

export function runScheduler<TJobID extends string>(
  jobs: Record<TJobID, JobInfo>,
  eventBuilder?: events.SchedulerEventBuilder,
  shouldStop?: SchedulerStopCallback<TJobID>,
): SchedulerReturn;
export function runScheduler<TJobID extends string>(
  jobs: Record<TJobID, (jobID: TJobID) => JobInfo>,
  eventBuilder?: events.SchedulerEventBuilder,
  shouldStop?: SchedulerStopCallback<TJobID>,
): SchedulerReturn;
export function runScheduler<TJobID extends string>(
  jobIDs: ReadonlyArray<TJobID>,
  jobSetup: (jobID: TJobID, index: number) => JobInfo,
  eventBuilder?: events.SchedulerEventBuilder,
  shouldStop?: SchedulerStopCallback<TJobID>,
): SchedulerReturn;

export function runScheduler<TJobID extends string>(
  jobsOrIDs:
    | Record<TJobID, JobInfo>
    | Record<TJobID, (jobID: TJobID) => JobInfo>
    | ReadonlyArray<TJobID>,
  jobSetupOrEventBuilder:
    | ((jobID: TJobID, index: number) => JobInfo)
    | (events.SchedulerEventBuilder | undefined),
  eventBuilderOrshouldStop:
    | events.SchedulerEventBuilder
    | SchedulerStopCallback<TJobID>
    | undefined = undefined,
  shouldStop?: SchedulerStopCallback<TJobID>,
): SchedulerReturn {
  const schedulerEvents =
    (typeof eventBuilderOrshouldStop === "function"
      ? undefined
      : eventBuilderOrshouldStop) ??
    (jobSetupOrEventBuilder instanceof common.EventEmitterBuilder
      ? jobSetupOrEventBuilder
      : events.createEventEmitterBuilder());
  const stopCallback =
    typeof eventBuilderOrshouldStop === "function"
      ? eventBuilderOrshouldStop
      : shouldStop ?? (() => false); // By default, never stop
  let jobs: Record<string, JobInfo>;
  if (Array.isArray(jobsOrIDs)) {
    if (typeof jobSetupOrEventBuilder !== "function") {
      throw new Error(
        "When giving array as first argument, second argument must be function.",
      );
    }
    jobs = jobsOrIDs.reduce<typeof jobs>((curJobs, jobID, idx) => {
      if (jobID in curJobs) {
        throw new Error(`Duplicate job ID ${jobID}`);
      }
      curJobs[jobID] = jobSetupOrEventBuilder(
        // getScopedEventBuilder(jobID),
        jobID,
        idx,
      );
      return curJobs;
    }, {});
  } else {
    jobs = Object.entries<JobInfo | ((jobID: TJobID) => JobInfo)>(
      jobsOrIDs,
    ).reduce<typeof jobs>((curJobs, [jobID, jobOrFactory]) => {
      curJobs[jobID] =
        typeof jobOrFactory === "function"
          ? jobOrFactory(jobID as TJobID)
          : jobOrFactory;
      return curJobs;
    }, {});
  }

  const eventEmitter = Object.entries(jobs).reduce(
    (emitter, [jobID, { jobSpecificEvents }]) => {
      return jobSpecificEvents
        ? emitter.combine(
            // Combine current event emitter with emitter scoped to this specific pipeline
            (jobSpecificEvents instanceof common.EventEmitterBuilder
              ? jobSpecificEvents.createEventEmitter()
              : jobSpecificEvents
            ).asScopedEventEmitter({
              jobScheduled: {
                name: jobID,
              },
              jobStarting: {
                name: jobID,
              },
              jobEnded: {
                name: jobID,
              },
            }),
          )
        : emitter;
    },
    schedulerEvents.createEventEmitter(),
  );
  const getDuration = (startTime: Date | undefined) =>
    startTime ? new Date().valueOf() - startTime.valueOf() : -1;
  const keepRunningJob = async (
    name: TJobID,
    { timeFromNowToNextInvocation, job }: typeof jobs[string],
  ) => {
    let start: Date | undefined = undefined;
    while (!stopCallback(name)) {
      const timeToStartInMs = timeFromNowToNextInvocation();
      eventEmitter.emit("jobScheduled", { name, timeToStartInMs });
      const endedEvent: events.VirtualSchedulerEvents["jobEnded"] = {
        name,
        durationInMs: -1,
      };
      try {
        await common.sleep(timeToStartInMs);
        eventEmitter.emit("jobStarting", { name });
        start = new Date();
        await job();
      } catch (e) {
        endedEvent.error = e as Error;
      } finally {
        endedEvent.durationInMs = getDuration(start);
        eventEmitter.emit("jobEnded", endedEvent);
      }
    }
  };
  return Promise.all(
    Object.entries(jobs).map(([name, job]) =>
      keepRunningJob(name as TJobID, job),
    ),
  );
}

// TS is in process of revamping its array methods, see:
// https://github.com/microsoft/TypeScript/issues/17002
// https://github.com/microsoft/TypeScript/pull/41849
// https://github.com/microsoft/TypeScript/issues/36554
// Meanwhile, we need to do with these hacks (code from comment in first link)
declare global {
  interface ArrayConstructor {
    isArray(
      arg: ReadonlyArray<unknown> | unknown,
    ): arg is ReadonlyArray<unknown>;
  }
}
