import * as common from "@data-heaving/common";
import * as builder from "./builder";

export const from = <TInput, TContext, TDatum>(
  source: common.TPipelineFactory<TInput, TContext, TDatum>,
) => new builder.DataPipelineBuilder<TInput, TContext, TDatum>(source);

export const arrayDataSource = <T>(
  arrayOrFactory: common.ItemOrFactory<ReadonlyArray<T>>,
) => {
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
      const array =
        typeof arrayOrFactory === "function"
          ? arrayOrFactory()
          : arrayOrFactory;
      for (let i = 0; i < array.length; ++i) {
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
