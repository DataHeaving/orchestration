import * as common from "@data-heaving/common";
import * as events from "./events";

export interface JobInfo<TResult> {
  timeFromNowToNextInvocation: (
    prevResult: TResult | undefined,
  ) => number | undefined;
  job: () => Promise<TResult>;
  jobSpecificEvents?:
    | common.EventEmitter<events.VirtualSchedulerEvents>
    | common.EventEmitterBuilder<events.VirtualSchedulerEvents>;
}

export type SchedulerReturn = Promise<Array<unknown>>;

export function runScheduler<TJobID extends string>(
  jobs: Record<TJobID, common.ItemOrFactory<JobInfo<unknown>, [TJobID]>>,
  eventBuilder?: events.SchedulerEventBuilder,
): SchedulerReturn;
export function runScheduler<TJobID extends string>(
  jobIDs: ReadonlyArray<TJobID>,
  jobSetup: common.ItemOrFactory<JobInfo<unknown>, [TJobID, number]>,
  eventBuilder?: events.SchedulerEventBuilder,
): SchedulerReturn;

export function runScheduler<TJobID extends string>(
  jobsOrIDs:
    | Record<TJobID, common.ItemOrFactory<JobInfo<unknown>, [TJobID]>>
    | ReadonlyArray<TJobID>,
  jobSetupOrEventBuilder:
    | common.ItemOrFactory<JobInfo<unknown>, [TJobID, number]>
    | (events.SchedulerEventBuilder | undefined),
  eventBuilderOrshouldStop:
    | events.SchedulerEventBuilder
    | undefined = undefined,
): SchedulerReturn {
  const schedulerEvents =
    (typeof eventBuilderOrshouldStop === "function"
      ? undefined
      : eventBuilderOrshouldStop) ??
    (jobSetupOrEventBuilder instanceof common.EventEmitterBuilder
      ? jobSetupOrEventBuilder
      : events.createEventEmitterBuilder());
  let jobs: Record<string, JobInfo<unknown>>;
  if (Array.isArray(jobsOrIDs)) {
    if (
      jobSetupOrEventBuilder instanceof common.EventEmitterBuilder ||
      jobSetupOrEventBuilder === undefined
    ) {
      throw new InvalidParametersError(
        "When giving array as first argument, second argument must be function or job specification.",
      );
    }
    jobs = jobsOrIDs.reduce<typeof jobs>((curJobs, jobID, idx) => {
      if (jobID in curJobs) {
        throw new DuplicateJobIDError(jobID);
      }
      curJobs[jobID] =
        typeof jobSetupOrEventBuilder === "function"
          ? jobSetupOrEventBuilder(
              // getScopedEventBuilder(jobID),
              jobID,
              idx,
            )
          : jobSetupOrEventBuilder;
      return curJobs;
    }, {});
  } else {
    jobs = Object.entries<common.ItemOrFactory<JobInfo<unknown>, [TJobID]>>(
      jobsOrIDs,
    ).reduce<typeof jobs>((curJobs, [jobID, jobOrFactory]) => {
      curJobs[jobID] =
        typeof jobOrFactory === "function"
          ? jobOrFactory(jobID as TJobID)
          : jobOrFactory;
      return curJobs;
    }, {});
  }

  const eventEmitter = Object.entries(jobs)
    .reduce((builder, [jobID, { jobSpecificEvents }]) => {
      return jobSpecificEvents
        ? common.combineEvents(
            builder,
            // Combine current event emitter with emitter scoped to this specific pipeline
            common.scopeEvents(jobSpecificEvents, {
              jobScheduled: {
                jobID: jobID,
              },
              jobStarting: {
                jobID: jobID,
              },
              jobEnded: {
                jobID: jobID,
              },
            }),
          )
        : builder;
    }, schedulerEvents)
    .createEventEmitter();
  const getDuration = (startTime: Date | undefined) =>
    startTime ? new Date().valueOf() - startTime.valueOf() : -1;
  const keepRunningJob = async (
    name: TJobID,
    { timeFromNowToNextInvocation, job }: typeof jobs[string],
  ) => {
    let start: Date | undefined = undefined;
    let prevResult = undefined;
    let timeToStartInMs: number | undefined;
    do {
      timeToStartInMs = timeFromNowToNextInvocation(prevResult);
      if (timeToStartInMs !== undefined && !isNaN(timeToStartInMs)) {
        eventEmitter.emit("jobScheduled", { jobID: name, timeToStartInMs });
        const endedEvent: events.VirtualSchedulerEvents["jobEnded"] = {
          jobID: name,
          durationInMs: -1,
        };
        try {
          await common.sleep(timeToStartInMs);
          eventEmitter.emit("jobStarting", { jobID: name });
          start = new Date();
          prevResult = await job();
        } catch (e) {
          prevResult = undefined;
          endedEvent.error = e as Error;
        } finally {
          endedEvent.durationInMs = getDuration(start);
          eventEmitter.emit("jobEnded", endedEvent);
        }
      }
    } while (timeToStartInMs !== undefined && !isNaN(timeToStartInMs));
    return prevResult;
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

export class DuplicateJobIDError extends Error {
  public constructor(public readonly jobID: string) {
    super(`Duplicate job ID "${jobID}".`);
  }
}

export class InvalidParametersError extends Error {}
