import * as common from "@data-heaving/common";
import test from "ava";
import * as helpers from "../helpers";
import * as testHelpers from "../test-helpers";

test("Throwing an error within complex behaves properly", async (t) => {
  const startArray = [0, 1];
  const endArray: typeof startArray = [];
  let sinkEndCalled = false;
  let transformerEndCalled = false;
  const pipeline = helpers
    .from(
      helpers
        .arrayDataSource(startArray)
        .create<typeof startArray, typeof startArray>(() => endArray),
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
      testHelpers.arrayDataSink(
        async () => {
          await common.sleep(100);
          throw new Error("Error");
        },
        () => {
          sinkEndCalled = true;
        },
      ),
    )
    .finalizePipeline();

  await t.throwsAsync(() => pipeline(startArray));
  t.deepEqual(startArray, endArray);
  t.true(sinkEndCalled);
  t.true(transformerEndCalled);
});
