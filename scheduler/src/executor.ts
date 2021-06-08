import * as utils from "@data-heaving/common";
import * as events from "./events";

export interface JobInfo {
  timeFromNowToNextInvocation: () => number;
  job: () => Promise<unknown>;
}

type SchedulerReturn = Promise<Array<never>>;

export function runScheduler<TJobID extends string>(
  jobs: Record<TJobID, JobInfo>,
  eventBuilder?: events.GlobalMutexSchedulerEventBuilder,
): SchedulerReturn;
export function runScheduler<TJobID extends string>(
  jobs: Record<
    TJobID,
    (
      events: events.GlobalMutexSchedulerJobSpecificEventAddition,
      jobID: TJobID,
    ) => JobInfo
  >,
  eventBuilder?: events.GlobalMutexSchedulerEventBuilder,
): SchedulerReturn;
export function runScheduler<TJobID extends string>(
  jobIDs: ReadonlyArray<TJobID>,
  jobSetup: (
    events: events.GlobalMutexSchedulerJobSpecificEventAddition,
    jobID: TJobID,
    index: number,
  ) => JobInfo,
  eventBuilder?: events.GlobalMutexSchedulerEventBuilder,
): SchedulerReturn;

export function runScheduler<TJobID extends string>(
  jobsOrIDs:
    | Record<TJobID, JobInfo>
    | Record<
        TJobID,
        (
          events: events.GlobalMutexSchedulerJobSpecificEventAddition,
          jobID: TJobID,
        ) => JobInfo
      >
    | ReadonlyArray<TJobID>,
  jobSetupOrEventBuilder:
    | ((
        events: events.GlobalMutexSchedulerJobSpecificEventAddition,
        jobID: TJobID,
        index: number,
      ) => JobInfo)
    | (events.GlobalMutexSchedulerEventBuilder | undefined),
  eventBuilder: events.GlobalMutexSchedulerEventBuilder | undefined = undefined,
): SchedulerReturn {
  const schedulerEvents =
    eventBuilder ??
    (jobSetupOrEventBuilder instanceof utils.EventEmitterBuilder
      ? jobSetupOrEventBuilder
      : events.createEventEmitterBuilder());
  const getScopedEventBuilder = (name: string) => {
    const nameMatcher = {
      name,
    } as const;
    return schedulerEvents.createScopedEventBuilder({
      jobScheduled: nameMatcher,
      jobStarting: nameMatcher,
      jobEnded: nameMatcher,
    });
  };
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
        getScopedEventBuilder(jobID),
        jobID,
        idx,
      );
      return curJobs;
    }, {});
  } else {
    jobs = Object.entries<
      | JobInfo
      | ((
          events: events.GlobalMutexSchedulerJobSpecificEventAddition,
          jobID: TJobID,
        ) => JobInfo)
    >(jobsOrIDs).reduce<typeof jobs>((curJobs, [jobID, jobOrFactory]) => {
      curJobs[jobID] =
        typeof jobOrFactory === "function"
          ? jobOrFactory(getScopedEventBuilder(jobID), jobID as TJobID)
          : jobOrFactory;
      return curJobs;
    }, {});
  }

  const eventEmitter = schedulerEvents.createEventEmitter();
  const getDuration = (startTime: Date | undefined) =>
    startTime ? new Date().valueOf() - startTime.valueOf() : -1;
  const keepRunningJob = async (
    name: string,
    { timeFromNowToNextInvocation, job }: typeof jobs[string],
  ) => {
    let start: Date | undefined = undefined;
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const timeToStartInMs = timeFromNowToNextInvocation();
      eventEmitter.emit("jobScheduled", { name, timeToStartInMs });
      const endedEvent: events.VirtualGlobalMutexSchedulerEvents["jobEnded"] = {
        name,
        durationInMs: -1,
      };
      try {
        await utils.sleep(timeToStartInMs);
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
    Object.entries(jobs).map(([name, job]) => keepRunningJob(name, job)),
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
