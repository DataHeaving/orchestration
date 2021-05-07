import * as common from "@data-heaving/common";

export class DataPipelineBuilder<TArg, TDatum> {
  private readonly _factory: common.TPipelineFactory<TArg, TDatum>;

  public constructor(factory: common.TPipelineFactory<TArg, TDatum>) {
    this._factory = factory;
  }

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
            promises: datumStoring.promises,
          };
        };
      }),
    );
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
          // const { processor, end } = datumStoringFactory(arg, resetSignal);
          let nextPart:
            | common.ComplexDatumTransformer<TDatum>
            | undefined = undefined;
          const endPromises: Array<Promise<unknown>> = [];
          // const reset = () => {
          //   resetSignal();
          //   if (nextPart) {
          //     nextPart.transformer.end();
          //     endPromises.push(...nextPart.datumStoring.end());
          //   }
          //   nextPart = undefined;
          // };
          return {
            storing: {
              processor: (datum, controlFlow) => {
                if (!nextPart) {
                  const { storing, promises } = processorFactory(
                    arg,
                    resetSignal,
                  );
                  endPromises.push(...(promises || []));
                  nextPart = datumFactory(storing, arg, resetSignal);
                }
                nextPart.transformer(datum, controlFlow);
              },
              end: () => {
                if (nextPart) {
                  nextPart.end();
                  nextPart = undefined;
                }
              },
            },
            promises: endPromises,
          };
        };
      }),
    );
  }

  public store(
    datumStoringFactory: () => common.DatumStoringFactory<TArg, TDatum>,
  ) {
    return this._factory(datumStoringFactory);
  }
}

export const from = <TArg, TDatum>(
  source: common.TPipelineFactory<TArg, TDatum>,
) => new DataPipelineBuilder<TArg, TDatum>(source);
