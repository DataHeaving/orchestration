import * as common from "@data-heaving/common";
import * as pipeline from "./pipeline";

export class DataPipelineBuilder<TInput, TContext, TDatum> {
  public constructor(
    private readonly _factory: common.TPipelineFactory<
      TInput,
      TContext,
      TDatum
    >,
  ) {}

  public simpleTransformEveryDatum<TTransformed>(
    transformerFactory: common.SimpleDatumTransformerFactory<
      TContext,
      TDatum,
      TTransformed
    >,
  ) {
    return new DataPipelineBuilder<TInput, TContext, TTransformed>(
      (datumStoringFactory) =>
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

  // // Please notice! This causes promises to accumulate 1 per datum into the top-level loop!!!
  // // Use with care and only when you know there won't be much data!
  // public asyncTransformEveryDatum<TTransformed>(
  //   transformer: (datum: TDatum, context: TContext) => Promise<TTransformed>,
  // ) {
  //   return this.complexTransformEveryDatum(() => {
  //     return (
  //       next: common.DatumStoring<TTransformed>,
  //       context,
  //       recreateSignal,
  //     ) => {
  //       let promiseForTransform: Promise<TTransformed> | undefined = undefined;
  //       // let seenControlFlow: common.ControlFlow | undefined = undefined;
  //       return {
  //         transformer: (datum, controlFlow) => {
  //           controlFlow?.pause();
  //           promiseForTransform = (async () => {
  //             try {
  //               return await transformer(datum, context);
  //             } finally {
  //               controlFlow?.resume();
  //             }
  //           })();
  //           // seenControlFlow = controlFlow;
  //           // Make top-level code call end immediately so that we can return next promise
  //           recreateSignal();
  //         },
  //         end: async () => {
  //           // TODO do we need to await for this here? This will be called only after control flow resumes, and at that point, we are done, as we pause the control flow before invoking promise.
  //           // try {
  //           const transformed = await promiseForTransform!; // eslint-disable-line @typescript-eslint/no-non-null-assertion
  //           next.processor(transformed, undefined); // TODO what if underlying pipeline component will need control flow?
  //           // } finally {
  //           next.end();
  //           // }
  //         },
  //       };
  //     };
  //   });
  // }

  public complexTransformEveryDatum<TTransformed>(
    transformerFactory: common.ComplexDatumTransformerFactory<
      TContext,
      TDatum,
      TTransformed
    >,
  ) {
    return new DataPipelineBuilder<TInput, TContext, TTransformed>(
      (datumStoringFactory) =>
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
    return new pipeline.DataPipeline<TInput, TContext, TDatum, TDatum>(
      this._factory,
      undefined,
    );
  }
  public storeTo<TResult>(
    datumStoringFactory: () => common.DatumStoringFactory<
      TContext,
      TDatum,
      TResult
    >,
  ) {
    return new pipeline.DataPipeline<TInput, TContext, TDatum, TResult>(
      this._factory,
      datumStoringFactory,
    );
  }

  public supplyInputFromInMemoryResultOf<
    TPreviousInput,
    TPreviousContext,
    TPreviousDatum
  >(
    builder: DataPipelineBuilder<
      TPreviousInput,
      TPreviousContext,
      TPreviousDatum
    >,
    getInput: (
      data: ReadonlyArray<TPreviousDatum>,
    ) => Promise<ReadonlyArray<TInput>>,
    concurrencyLevel: number,
  ) {
    return new DataPipelineBuilder<TPreviousInput, TContext, TDatum>(
      (datumStoringFactory) => {
        return async (input) => {
          const allValues: Array<TPreviousDatum> = [];
          const datumStoring: common.DatumStoringFactory<
            TPreviousContext,
            TPreviousDatum
          > = () => ({
            storing: {
              processor: (datum) => allValues.push(datum),
              end: () => {},
            },
            promise: undefined,
          });
          await builder._factory(() => datumStoring)(input);
          const newInputs = await getInput(allValues);
          const thisPipeline = this._factory(datumStoringFactory);
          await common.iterateInParallel(
            newInputs,
            concurrencyLevel,
            async (newInput) => {
              await thisPipeline(newInput);
            },
            () => undefined,
          );
        };
      },
    );
  }
}
