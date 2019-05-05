package com.BeamTextIO.TextIO;

import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;

public class DEMO {

	public static void main(String[] args) {
		// 创建管道工厂
		PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

		// 设置运行的模型，现在一共有3种
		pipelineOptions.setRunner(DirectRunner.class);

		
		// DataflowPipelineOptions dataflowOptions =
		// pipelineOptions.as(DataflowPipelineOptions.class);
		// dataflowOptions.setRunner(DataflowRunner.class);

		// 设置相应的管道
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		// 根据文件路径读取文件内容
		pipeline.apply(TextIO.read().from(
				"/Users/xiaoliangchen/Documents/Workspace/Java/Apache-Beam-Example/TextIO/resource/text.txt")
			).apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				// 根据空格进行读取数据，里面可以用Luma 表达式写
				for (String word : c.element().split(" ")) {
					if (!word.isEmpty()) {
						c.output(word);
						System.out.println("读文件中的数据："+word);
					}
				}
			}
		})).apply(Count.<String>perElement())
		.apply("FormatResult", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
			@Override
			public String apply(KV<String, Long> input) {
				return input.getKey() + ": " + input.getValue();
			}
		})).apply(
				// 进行输出到文件夹下面
				TextIO.write().to("/Users/xiaoliangchen/Documents/Workspace/Java/Apache-Beam-Example/TextIO/result/textcount")
		);
		pipeline.run().waitUntilFinish();

	}

}
