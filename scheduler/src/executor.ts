import * as utils from "@data-heaving/common";
import * as events from "./events";

export interface SchedulerRunOptions<TArg> {
  jobs: {
    [name: string]: utils.ItemOrFactory<{
      timeFromNowToNextInvocation: () => number;
      job: (arg: TArg) => Promise<unknown>;
    }>;
  };
  eventBuilder?: events.GlobalMutexSchedulerEventBuilder<TArg>;
  jobSetup: (options: JobArgumentCreationOptions<TArg>) => JobSetup<TArg>;
}

export interface JobArgumentCreationOptions<TArg> {
  schedulerEvents: events.GlobalMutexSchedulerJobSpecificEventAddition<TArg>;
  jobName: string;
}

export interface JobSetup<TArg> {
  jobArgument: TArg;
  jobRunner?: (runJob: () => Promise<unknown>) => Promise<unknown>;
}

export const runScheduler = <TArg>({
  jobs,
  eventBuilder,
  jobSetup,
}: SchedulerRunOptions<TArg>) => {
  const getDuration = (startTime: Date | undefined) =>
    startTime ? new Date().valueOf() - startTime.valueOf() : -1;

  const schedulerEvents = eventBuilder || events.createEventEmitterBuilder();
  const jobKeys = Object.keys(jobs);
  const allArgs = jobKeys.reduce<Record<string, JobSetup<TArg>>>(
    (argsDictionary, jobName) => {
      const nameMatcher = {
        name: jobName,
      } as const;
      argsDictionary[jobName] = jobSetup({
        jobName,
        schedulerEvents: schedulerEvents.createScopedEventBuilder({
          jobScheduled: nameMatcher,
          jobStarting: nameMatcher,
          jobEnded: nameMatcher,
        }),
      });
      return argsDictionary;
    },
    {},
  );

  const eventEmitter = schedulerEvents.createEventEmitter();
  const jobsDictionary = Object.fromEntries(
    jobKeys.map((jobName) => {
      const jobOrFactory = jobs[jobName];
      return [
        jobName,
        typeof jobOrFactory === "function" ? jobOrFactory() : jobOrFactory,
      ] as const;
    }),
  );

  const keepRunningJob = async (
    name: string,
    { timeFromNowToNextInvocation, job }: typeof jobsDictionary[string],
  ) => {
    // const api = mutexInfo?.mutexAPIFactory(name);
    let start: Date | undefined = undefined;
    const { jobArgument: arg, jobRunner } = allArgs[name];
    // eslint-disable-next-line no-constant-condition
    while (true) {
      const timeToStartInMs = timeFromNowToNextInvocation();
      eventEmitter.emit("jobScheduled", { name, arg, timeToStartInMs });
      const endedEvent: events.VirtualGlobalMutexSchedulerEvents<TArg>["jobEnded"] = {
        name,
        arg,
        durationInMs: -1,
      };
      try {
        await utils.sleep(timeToStartInMs);
        eventEmitter.emit("jobStarting", { name, arg });
        start = new Date();
        await (jobRunner ? jobRunner(() => job(arg)) : job(arg));
        //  mutex.executeWithMutexAndHeartBeat({
        //     api,
        //     usage: () => job(arg),
        //     heartBeatUploadPeriod: mutexInfo?.heartBeatUploadPeriod || 60000, // Upload heart beat every 1min by default
        //     heartBeatStaleTime: mutexInfo?.heartBeatStaleTime || 120000, // Stale time is 2min by default
        //     eventEmitter,
        //   })
        // : job(arg));
      } catch (e) {
        endedEvent.error = e as Error;
      } finally {
        endedEvent.durationInMs = getDuration(start);
        eventEmitter.emit("jobEnded", endedEvent);
      }
    }
  };
  return Promise.all(
    Object.entries(jobsDictionary).map(([name, job]) =>
      keepRunningJob(name, job),
    ),
  );
};
