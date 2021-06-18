import * as common from "@data-heaving/common";
import test from "ava";
import * as spec from "../builder";
import * as helpers from "../helpers";

test("Using transformations works as expected", async (t) => {
  const startArray = [0, 1];
  const endArray: typeof startArray = [];
  let sinkEndCalled = false;
  // pipeline input: start array, pipeline context: string, pipeline target: end array
  // transformation: increment each element by 1
  // transformation variations: both simple and complex
  let pipeline = helpers
    .from(
      helpers
        .arrayInputPassThroughSource<number>()
        .withContextFactory(() => "Context"),
    )
    .transformEveryDatum({
      transformer: "simple",
      factory: () => (item) => item + 1,
    })
    .storeTo(
      helpers.arrayDataSink(endArray, () => {
        sinkEndCalled = true;
      }),
    );

  await pipeline(startArray);
  t.deepEqual(startArray, [0, 1]);
  t.deepEqual(endArray, [1, 2]);
  t.true(sinkEndCalled);

  sinkEndCalled = false;
  endArray.length = 0;
  let transformerEndCalled = false;

  pipeline = helpers
    .from(
      helpers
        .arrayInputPassThroughSource<number>()
        .withContextFactory(() => "Context"),
    )
    .transformEveryDatum({
      transformer: "complex",
      factory: () => (next: common.DatumStoring<number>) => ({
        transformer: (item, controlFlow) => {
          next.processor(item + 1, controlFlow);
        },
        end: () => {
          next.end(); // NOTICE! It is responsibility of complex transformer to call end of the next-in-chain!
          transformerEndCalled = true;
        },
      }),
    })
    .storeTo(
      helpers.arrayDataSink(endArray, () => {
        sinkEndCalled = true;
      }),
    );
  await pipeline(startArray);
  t.deepEqual(startArray, [0, 1]);
  t.deepEqual(endArray, [1, 2]);
  t.true(transformerEndCalled);
  t.true(sinkEndCalled);
});

test("The supplyInputFromInMemoryResultOf works as expected", async (t) => {
  const startArray = [0, 1];
  const endArray: typeof startArray = [];
  // pipeline input: start array, pipeline context: string, pipeline target: end array
  // transformation: increment each element by 1
  // transformation variations: both simple and complex
  const pipelineSource = helpers
    .arrayInputPassThroughSource<number>()
    .withContextFactory(() => "Context");
  const pipeline = helpers
    .from(pipelineSource)
    .supplyInputFromInMemoryResultOf(
      helpers.from(pipelineSource),
      (theArray) => {
        return Promise.resolve([theArray.map((i) => i + 1)]);
      },
      1,
    )
    .transformEveryDatum({
      transformer: "simple",
      factory: () => (item) => {
        return item + 1;
      },
    })
    .storeTo(helpers.arrayDataSink(endArray));
  await pipeline(startArray);
  t.deepEqual(startArray, [0, 1]);
  t.deepEqual(endArray, [2, 3]); // Each element has been increased twice: by supplyInputFromInMemoryResultOf callback, and simple transformer
});

test("Throwing an error within complex behaves properly (e.g. does not become stuck)", async (t) => {
  const startArray = [0, 1];
  const endArray: typeof startArray = [];
  let sinkEndCalled = false;
  let transformerEndCalled = false;
  const pipeline = helpers
    .from(
      helpers
        .arrayInputPassThroughSource<number>()
        .withContextFactory(() => "Context"),
    )
    .transformEveryDatum<number>({
      transformer: "complex",
      factory: () => (next) => ({
        transformer: (datum) => {
          next.processor(datum);
        },
        end: async () => {
          transformerEndCalled = true;
          next.end();
          await common.sleep(100);
          throw new Error("Error");
        },
      }),
    })
    .storeTo(
      helpers.arrayDataSink(
        endArray,
        () => {
          sinkEndCalled = true;
        },
        async () => {
          await common.sleep(100);
          throw new Error("Error");
        },
      ),
    );

  await t.throwsAsync(() => pipeline(startArray));
  t.deepEqual(startArray, endArray);
  t.true(sinkEndCalled);
  t.true(transformerEndCalled);
});

test("Supplying wrong parameters to transform callback throws an error", (t) => {
  t.throws(
    () =>
      helpers
        .from(
          helpers
            .arrayInputPassThroughSource<number>()
            .withContextFactory(() => "Context"),
        )
        .transformEveryDatum<number>({
          transformer: "wrong" as "simple",
          factory: (undefined as unknown) as common.SimpleDatumTransformerFactory<
            string,
            number,
            number
          >["factory"],
        }),
    {
      instanceOf: spec.UnrecognizedTransformerFactoryKindError,
      message: `Unrecognized transformer factory kind "wrong".`,
    },
  );
});

test("The storeToNowhere doesn't crash", async (t) => {
  const startArray = [0, 1];
  const pipelineSource = helpers
    .arrayInputPassThroughSource<number>()
    .withContextFactory(() => "Context");
  const pipeline = helpers.from(pipelineSource).storeToNowhere();
  await pipeline(startArray);
  t.deepEqual(startArray, [0, 1]);
});
