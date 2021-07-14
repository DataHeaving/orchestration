import * as common from "@data-heaving/common";
import * as builder from "./builder";

export const from = <TInput, TContext, TDatum>(
  source: common.TPipelineFactory<TInput, TContext, TDatum>,
) => new builder.DataPipelineBuilder<TInput, TContext, TDatum>(source);

export const arrayInputPassThroughSource = <TInput>(concurrencyLevel = 1) => {
  function withContextFactory<TContext>(
    createContext: (input: ReadonlyArray<TInput>) => TContext,
  ): common.TPipelineFactory<ReadonlyArray<TInput>, TContext, TInput> {
    return (datumStoringFactory) => async (input) => {
      await common.runPipelineWithBufferedData(
        createContext(input),
        datumStoringFactory(),
        input,
        concurrencyLevel,
      );
    };
  }

  return {
    withContextFactory,
  };
};

export function arrayDataSink<TContext, TDatum>(
  array: Array<TDatum>,
  end?: () => void,
  getEndPromise?: () => Promise<unknown>,
): () => common.DatumStoringFactory<TContext, TDatum, Array<TDatum>> {
  return () => {
    return () => ({
      storing: {
        processor: (datum) => {
          array.push(datum);
        },
        end: end ?? (() => {}),
      },
      promise: getEndPromise
        ? (async () => {
            await getEndPromise();
            return array;
          })()
        : undefined,
    });
  };
}

// export const arrayDataSource = <T>(
//   arrayOrFactory: common.ItemOrFactory<ReadonlyArray<T>>,
// ) => {
//   function create<TInput, TContext>(
//     createContext: (input: TInput) => TContext,
//   ): common.TPipelineFactory<TInput, TContext, T> {
//     return (datumStoringFactory) => async (input) => {
//       const datumStoring = datumStoringFactory();
//       let currentStoring:
//         | ReturnType<typeof datumStoring>
//         | undefined = undefined;
//       const recreate = () => {
//         currentStoring?.storing.end();
//         currentStoring = undefined;
//       };
//       const promises: Array<Promise<unknown>> = [];
//       const array =
//         typeof arrayOrFactory === "function"
//           ? arrayOrFactory()
//           : arrayOrFactory;
//       const ctx = createContext(input);
//       for (let i = 0; i < array.length; ++i) {
//         if (!currentStoring) {
//           currentStoring = datumStoring(ctx, recreate);
//           const promise = currentStoring.promise;
//           if (promise) {
//             promises.push(promise);
//           }
//         }
//         currentStoring.storing.processor(array[i], undefined);
//       }
//       recreate();

//       await Promise.all(promises);
//     };
//   }

//   return {
//     create,
//   };
// };
