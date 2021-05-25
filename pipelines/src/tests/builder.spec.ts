import * as common from "@data-heaving/common";
import test from "ava";
import * as helpers from "../helpers";

test("Throwing an error within complex transform won't stuck pipeline", async (t) => {
  const startArray = [0, 1];
  const endArray: typeof startArray = [];
  const pipeline = helpers
    .from(
      arrayDataSource(startArray).create<typeof startArray, typeof startArray>(
        () => endArray,
      ),
    )
    .complexTransformEveryDatum<number>(
      () => (next, context, recreateSignal) => ({
        transformer: (datum, controlFlow) => {
          console.log('DATUMZ', datum); // eslint-disable-line
          next.processor(datum);
        },
        end: async () => {
          next.end();
          await common.sleep(100);
          // console.log('THROWING'); // eslint-disable-line
          // throw new Error("This error must not cause pipeline to hang");
        },
      }),
    )
    .storeTo(
      arrayDataSink(async () => {
        await common.sleep(100);
        console.log('THROWING'); // eslint-disable-line
        throw new Error("This error must not cause pipeline to hang");
      }),
    )
    .finalizePipeline();

  await pipeline(startArray);
  t.deepEqual(startArray, endArray);
});

const arrayDataSource = <T>(array: ReadonlyArray<T>) => {
  function create<TInput, TContext>(
    createContext: (input: TInput) => TContext,
  ): common.TPipelineFactory<TInput, TContext, T> {
    return (datumStoringFactory) => async (input) => {
      const datumStoring = datumStoringFactory();
      let currentStoring:
        | ReturnType<typeof datumStoring>
        | undefined = undefined;
      const recreate = () => {
        currentStoring?.storing.end();
        currentStoring = undefined;
      };
      const promises: Array<Promise<unknown>> = [];
      for (let i = 0; i < array.length; ++i) {
        await common.sleep(100);
        if (!currentStoring) {
          currentStoring = datumStoring(createContext(input), recreate);
          const promise = currentStoring.promise;
          if (promise) {
            promises.push(promise);
          }
        }
        currentStoring.storing.processor(array[i], undefined);
      }
      recreate();

      await Promise.all(promises);
    };
  }

  return {
    create,
  };
};

function arrayDataSink<TDatum>(
  getEndPromise: (ctx: Array<TDatum>) => Promise<Array<TDatum>>,
): () => common.DatumStoringFactory<Array<TDatum>, TDatum, Array<TDatum>> {
  return () => {
    return (context) => ({
      storing: {
        processor: (datum) => {
          context.push(datum);
        },
        end: () => {},
      },
      promise: getEndPromise(context),
    });
  };
}
