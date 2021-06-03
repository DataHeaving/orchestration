import test from "ava";
import * as helpers from "../helpers";
import * as testHelpers from "../test-helpers";

test("Splitting sink for array data source works as expected", async (t) => {
  const startArray = [0, 1];
  const endArray: typeof startArray = [];
  const transformerCalls: typeof startArray = [];
  let createCalls = 0;

  await helpers
    .from(
      helpers
        .arrayDataSource(startArray)
        .create<typeof startArray, typeof startArray>(() => endArray),
    )
    .transformEveryDatum<number>({
      transformer: "complex",
      factory: () => (next, context, recreateSignal) => {
        ++createCalls;
        return {
          transformer: (datum, controlFlow) => {
            transformerCalls.push(datum);
            next.processor(datum, controlFlow);
            recreateSignal();
          },
          end: () => {
            next.end();
            return Promise.resolve();
          },
        };
      },
    })
    .storeTo(
      testHelpers.arrayDataSink(
        (array) => Promise.resolve(array),
        () => {},
      ),
    )
    .finalizePipeline()(startArray);
  t.deepEqual(startArray, endArray);
  t.deepEqual(startArray, transformerCalls);
  t.is(createCalls, 2);
});
