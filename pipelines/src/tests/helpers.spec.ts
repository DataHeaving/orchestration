import test from "ava";
import * as helpers from "../helpers";

test("Splitting sink for array data source works as expected", async (t) => {
  const startArray = [0, 1];
  const endArray: typeof startArray = [];
  const transformerCalls: typeof startArray = [];
  let createCalls = 0;

  await helpers
    .from(
      helpers
        .arrayInputPassThroughSource<number>()
        .withContextFactory(() => "Context"),
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
      helpers.arrayDataSink(endArray, undefined, () => Promise.resolve()),
    )(startArray);
  t.deepEqual(startArray, endArray);
  t.deepEqual(startArray, transformerCalls);
  t.is(createCalls, 2);
});
