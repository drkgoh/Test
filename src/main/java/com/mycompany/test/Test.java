package com.mycompany.test;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.TextualIntegerCoder;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.Combine;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.MapElements;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.Sum;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import java.io.File;
import java.util.List;
import java.util.Arrays;

/*
Name: Derrick Goh Jun Han
Email ID: derrick.goh.2013
 */
public class Test {
    public static void main(String [] args){
        PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
        Pipeline p = Pipeline.create(options);
        //Read the file and produce each line as a string
        PCollection<String> lines = p.apply(TextIO.Read.from("Lions-Tigers-Bears.csv"));
        
        //Split each line by commas and put into a PCollection of String Arrays
        PCollection<String []> eachLine = lines.apply(
            MapElements.via((String word) -> word.split(","))
            .withOutputType(new TypeDescriptor<String []>() {}));
        
        //Check if the element contains Lion, A and is larger than or equals to 5 – if yes +1
        PCollection<Integer> count = eachLine.apply(
            ParDo
              .named("LogicCheck")                      // the transform name
              .of(new DoFn<String[], Integer>() {       // a DoFn as an anonymous inner class instance
                @Override
                public void processElement(ProcessContext c) {
                    String[] eachLine = c.element();
                    if(eachLine[0].equals("Lion") && eachLine[1].equals("A") && (Integer.parseInt(eachLine[2]) >= 5)){
                        c.output(1);
                    }
                }
        }));
        
        //Sum everything up
        PCollection<Integer> sum = count.apply(Combine.globally(new Sum.SumIntegerFn()));
        
        sum.apply(TextIO.Write.to("Outfile.txt").withCoder(TextualIntegerCoder.of()));
        p.run();
    }
}
