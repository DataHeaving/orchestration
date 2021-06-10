import * as common from "@data-heaving/common";
import test from "ava";
import * as spec from "../executor";
import * as events from "../events";

test("Job-specific event invoking works", async (t) => {
  const globalEvents = createEventsTrackerObject();
  const job1Events = createEventsTrackerObject();
  const job2Events = createEventsTrackerObject();
  await spec.runScheduler(
    {
      job1: {
        job: () => common.sleep(100),
        timeFromNowToNextInvocation: timeFromNowToNextInvocation(1, 0), // Return value of 0 for 1 time, then undefined
        jobSpecificEvents: createJobSpecificEventBuilderForTrackerObject(
          job1Events,
        ),
      },
      job2: {
        job: () => common.sleep(100),
        timeFromNowToNextInvocation: timeFromNowToNextInvocation(1, 0), // Return value of 0 for 1 time, then undefined
        jobSpecificEvents: createJobSpecificEventBuilderForTrackerObject(
          job2Events,
        ),
      },
    },
    createJobSpecificEventBuilderForTrackerObject(globalEvents),
  );

  t.deepEqual(
    globalEvents,
    {
      jobScheduled: 2,
      jobStarting: 2,
      jobEnded: 2,
    },
    "Global events must all have been invoked twice: 1 full cycle for both jobs",
  );
  t.deepEqual(
    job1Events,
    {
      jobScheduled: 1,
      jobStarting: 1,
      jobEnded: 1,
    },
    "Job1-specific events must all have been invoked only once: 1 full cycle for this job.",
  );
  t.deepEqual(
    job2Events,
    {
      jobScheduled: 1,
      jobStarting: 1,
      jobEnded: 1,
    },
    "Job2-specific events must all have been invoked only once: 1 full cycle for this job.",
  );
});

test("After error, previous value is undefined", async (t) => {
  let counter = 0;
  const timeToNextRun = timeFromNowToNextInvocation(4, 0);
  const seenResults: Array<unknown> = [];
  await spec.runScheduler({
    job: {
      job: () => {
        ++counter;
        if (counter === 3) {
          throw new Error("Dummy");
        }
        return Promise.resolve(counter);
      },
      timeFromNowToNextInvocation: (val) => {
        seenResults.push(val);
        return timeToNextRun();
      },
    },
  });
  t.deepEqual(seenResults, [undefined, 1, 2, undefined, 4]);
});

function createEventsTrackerObject(): {
  [P in keyof events.VirtualSchedulerEvents]: number;
} {
  return {
    jobScheduled: 0,
    jobStarting: 0,
    jobEnded: 0,
  };
}

const createJobSpecificEventBuilderForTrackerObject = (
  trackerObject: ReturnType<typeof createEventsTrackerObject>,
) => {
  const retVal = new common.EventEmitterBuilder<events.VirtualSchedulerEvents>();
  for (const evtNameString of Object.keys(trackerObject)) {
    const evtName = evtNameString as keyof events.VirtualSchedulerEvents;
    retVal.addEventListener(evtName, () => {
      ++trackerObject[evtName];
    });
  }
  return retVal;
};

const timeFromNowToNextInvocation = (iterations: number, delay: number) => () =>
  --iterations < 0 ? undefined : delay;
