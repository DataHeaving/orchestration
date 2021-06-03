import * as common from "@data-heaving/common";

export class DataPipeline<TInput, TContext, TDatum, TResult> {
  public constructor(
    private readonly _factory: common.TPipelineFactory<
      TInput,
      TContext,
      TDatum
    >,
    private readonly _datumStoringFactory:
      | (() => common.DatumStoringFactory<TContext, TDatum, TResult>)
      | undefined,
  ) {}

  public finalizePipeline() {
    return this._factory(
      this._datumStoringFactory ??
        (() => {
          return () => {
            return {
              storing: {
                processor: () => {},
                end: () => {},
              },
            };
          };
        }),
    );
  }

  public saveToMemoryAndContinueWithAsync<TNewResult>(
    processItems: (items: ReadonlyArray<TDatum>) => Promise<TNewResult>,
  ) {
    return async (input: TInput) => {
      const allValues: Array<TDatum> = [];
      const datumStoring: common.DatumStoringFactory<
        TContext,
        TDatum,
        TResult
      > = () => ({
        storing: {
          processor: (datum) => allValues.push(datum),
          end: () => {},
        },
        promise: undefined,
      });
      await this._factory(() => datumStoring)(input);
      return await processItems(allValues);
    };
  }
  // TODO: this doesn't properly work if top-level source e.g. creates datum storing multiple times. It can cause calling of dataTransform over carthesian product of data.
  // public saveToMemoryAndContinueWith<TNewDatum, TNewArg>(
  //   dataTransform: (
  //     data: ReadonlyArray<TResult>,
  //   ) => common.MaybePromise<ReadonlyArray<TNewDatum>>,
  //   getNewArg: (result: TNewDatum) => TNewArg,
  //   getConcurrenclyLevel?: (
  //     newData: ReadonlyArray<TNewDatum>,
  //     results: ReadonlyArray<TResult>,
  //   ) => number,
  // ) {
  //   return new DataPipelineBuilder<TNewArg, TNewDatum>(
  //     (datumStoringFactory) => {
  //       return this._factory(() => {
  //         const allSeenPromises: Array<Promise<TResult>> = [];
  //         const datumStoring =
  //           this._datumStoringFactory?.() ??
  //           (() => ({
  //             storing: {
  //               processor: (datum) =>
  //                 allSeenPromises.push(
  //                   Promise.resolve((datum as unknown) as TResult),
  //                 ),
  //               end: () => {},
  //             },
  //             promise: undefined,
  //           }));
  //         let endCalled = false;
  //         return (arg) => {
  //           let currentStoring:
  //             | {
  //                 storing: common.DatumStoring<TDatum>;
  //                 promise?: Promise<TResult>;
  //               }
  //             | undefined = undefined;
  //           return {
  //             storing: {
  //               processor: (datum, controlFlow) => {
  //                 if (!currentStoring) {
  //                   currentStoring = datumStoring(arg, () => {
  //                     currentStoring?.storing.end();
  //                     currentStoring = undefined;
  //                   });
  //                   const promise = currentStoring.promise;
  //                   if (promise) {
  //                     allSeenPromises.push(promise);
  //                   }
  //                 }
  //                 currentStoring.storing.processor(datum, controlFlow);
  //               },
  //               end: () => {
  //                 endCalled = true;
  //                 currentStoring?.storing.end();
  //               },
  //             },
  //             promise: (async () => {
  //               while (!endCalled) {
  //                 await common.sleep(100);
  //               }
  //               const data = await Promise.all(allSeenPromises);
  //               const newData = await dataTransform(data);
  //               await common.runPipelineWithBufferedData(
  //                 datumStoringFactory(),
  //                 newData,
  //                 getConcurrenclyLevel?.(newData, data) || 1,
  //                 getNewArg,
  //               );
  //             })(),
  //           };
  //         };
  //       });
  //     },
  //   );
  // }
}
