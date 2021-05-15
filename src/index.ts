import * as common from "@data-heaving/common";

export type BarrierFunctionalityFactory<TArg, TDatum, TTransformed> = () => (
  arg: TArg,
) => BarrierFunctionality<TDatum, TTransformed>;

export interface BarrierFunctionality<TDatum, TTransformed> {
  consumer: common.DatumProcessor<TDatum>;
  end: () => Array<Promise<TTransformed>>;
}

export class DataPipelineBuilder<TArg, TDatum> {
  public constructor(
    private readonly _factory: common.TPipelineFactory<TArg, TDatum>,
  ) {}

  public simpleTransformEveryDatum<TTransformed>(
    transformerFactory: common.SimpleDatumTransformerFactory<
      TArg,
      TDatum,
      TTransformed
    >,
  ) {
    return new DataPipelineBuilder<TArg, TTransformed>((datumStoringFactory) =>
      this._factory(() => {
        const processorFactory = datumStoringFactory();
        return (arg, resetSignal) => {
          const datumStoring = processorFactory(arg, resetSignal);
          const { processor, end } = datumStoring.storing;
          const transformer = transformerFactory(arg);
          return {
            storing: {
              processor: (datum, controlFlow) =>
                processor(transformer(datum), controlFlow),
              end,
            },
            promise: datumStoring.promise,
          };
        };
      }),
    );
  }

  // Please notice! This causes promises to accumulate 1 per datum into the top-level loop!!!
  // Use with care and only when you know there won't be much data!
  public asyncTransformEveryDatum<TTransformed>(
    transformer: (datum: TDatum, arg: TArg) => Promise<TTransformed>,
  ) {
    return this.complexTransformEveryDatum(() => {
      return (next: common.DatumStoring<TTransformed>, arg, recreateSignal) => {
        let promiseForTransform: Promise<TTransformed> | undefined = undefined;
        // let seenControlFlow: common.ControlFlow | undefined = undefined;
        return {
          transformer: (datum, controlFlow) => {
            controlFlow?.pause();
            promiseForTransform = (async () => {
              try {
                return await transformer(datum, arg);
              } finally {
                controlFlow?.resume();
              }
            })();
            // seenControlFlow = controlFlow;
            // Make top-level code call end immediately so that we can return next promise
            recreateSignal();
          },
          end: async () => {
            // TODO do we need to await for this here? This will be called only after control flow resumes, and at that point, we are done, as we pause the control flow before invoking promise.
            const transformed = await promiseForTransform!; // eslint-disable-line @typescript-eslint/no-non-null-assertion
            next.processor(transformed, undefined); // TODO what if underlying pipeline component will need control flow?
            next.end();
          },
        };
      };
    });
  }

  public complexTransformEveryDatum<TTransformed>(
    transformerFactory: common.ComplexDatumTransformerFactory<
      TArg,
      TDatum,
      TTransformed
    >,
  ) {
    return new DataPipelineBuilder<TArg, TTransformed>((datumStoringFactory) =>
      this._factory(() => {
        const datumFactory = transformerFactory();
        const processorFactory = datumStoringFactory();
        return (arg, resetSignal) => {
          // TODO Refactor: we don't need nextPart to be recreateable, as this whole scope will be recreated when resetSignal is called!
          let nextPart:
            | common.ComplexDatumTransformer<TDatum>
            | undefined = undefined;
          const endPromises: Array<Promise<unknown>> = [];
          let endCalled = false;
          return {
            storing: {
              processor: (datum, controlFlow) => {
                if (!nextPart) {
                  const { storing, promise } = processorFactory(
                    arg,
                    resetSignal,
                  );
                  if (promise) {
                    endPromises.push(promise);
                  }
                  nextPart = datumFactory(storing, arg, resetSignal);
                }
                nextPart.transformer(datum, controlFlow);
              },
              end: () => {
                if (nextPart) {
                  const maybePromise = nextPart.end();
                  if (maybePromise instanceof Promise) {
                    endPromises.push(maybePromise);
                  }
                  nextPart = undefined;
                }
                endCalled = true;
              },
            },
            promise: (async () => {
              while (!endCalled) {
                await common.sleep(500);
              }

              await Promise.all(endPromises);
            })(),
          };
        };
      }),
    );
  }

  public storeAsIs() {
    return new DataPipeline<TArg, TDatum, TDatum>(this._factory, undefined);
  }
  public storeTo<TResult>(
    datumStoringFactory: () => common.DatumStoringFactory<
      TArg,
      TDatum,
      TResult
    >,
  ) {
    return new DataPipeline<TArg, TDatum, TResult>(
      this._factory,
      datumStoringFactory,
    );
  }
}

export const from = <TArg, TDatum>(
  source: common.TPipelineFactory<TArg, TDatum>,
) => new DataPipelineBuilder<TArg, TDatum>(source);

export class DataPipeline<TArg, TDatum, TResult> {
  public constructor(
    private readonly _factory: common.TPipelineFactory<TArg, TDatum>,
    private readonly _datumStoringFactory:
      | (() => common.DatumStoringFactory<TArg, TDatum, TResult>)
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
    return async () => {
      const allValues: Array<TDatum> = [];
      const datumStoring: common.DatumStoringFactory<
        TArg,
        TDatum,
        TResult
      > = () => ({
        storing: {
          processor: (datum) => allValues.push(datum),
          end: () => {},
        },
        promise: undefined,
      });
      await this._factory(() => datumStoring)();
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
