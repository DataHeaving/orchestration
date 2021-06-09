import * as common from "@data-heaving/common";
import test from "ava";
import * as spec from "../executor";
import * as events from "../events";

test("Job-specific event invoking works", async (t) => {
  const globalEvents = createEventsTrackerObject();
  const job1Events = createEventsTrackerObject();
  const job2Events = createEventsTrackerObject();
  let iterationCounter = 2;
  await spec.runScheduler(
    {
      job1: {
        job: () => common.sleep(100),
        timeFromNowToNextInvocation: () => 0,
        jobSpecificEvents: createJobSpecificEventBuilderForTrackerObject(
          job1Events,
        ),
      },
      job2: {
        job: () => common.sleep(100),
        timeFromNowToNextInvocation: () => 0,
        jobSpecificEvents: createJobSpecificEventBuilderForTrackerObject(
          job2Events,
        ),
      },
    },
    createJobSpecificEventBuilderForTrackerObject(globalEvents),
    () => {
      // Return true (to stop running) once counter goes to zero
      return --iterationCounter < 0;
    },
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
