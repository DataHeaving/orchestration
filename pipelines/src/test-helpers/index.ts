import * as common from "@data-heaving/common";

export function arrayDataSink<TDatum>(
  getEndPromise: (ctx: Array<TDatum>) => Promise<Array<TDatum>>,
  end: () => void,
): () => common.DatumStoringFactory<Array<TDatum>, TDatum, Array<TDatum>> {
  return () => {
    return (context) => ({
      storing: {
        processor: (datum) => {
          context.push(datum);
        },
        end,
      },
      promise: getEndPromise(context),
    });
  };
}
