import * as common from "@data-heaving/common";
import * as builder from "./builder";

export const from = <TInput, TContext, TDatum>(
  source: common.TPipelineFactory<TInput, TContext, TDatum>,
) => new builder.DataPipelineBuilder<TInput, TContext, TDatum>(source);
