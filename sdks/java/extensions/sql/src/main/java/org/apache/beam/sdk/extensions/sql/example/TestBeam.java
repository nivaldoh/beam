package org.apache.beam.sdk.extensions.sql.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;

import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;


class TestBeam {

  public static void main(String[] args) {
    PipelineOptions options = PipelineOptionsFactory.fromArgs(args).create();
    
    options
    .as(BeamSqlPipelineOptions.class)
    .setPlannerName("org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner");
    
    Pipeline p = Pipeline.create(options);


    // define the input row format
    Schema type =
        Schema.builder().addInt32Field("c1").addStringField("c2").addDoubleField("c3").build();

    Row row1 = Row.withSchema(type).addValues(1, "1_row", 1.0).build();
    Row row2 = Row.withSchema(type).addValues(2, "2_row", 2.0).build();
    Row row3 = Row.withSchema(type).addValues(3, "row", 3.0).build();

    // create a source PCollection with Create.of();
    PCollection<Row> inputTable =
        PBegin.in(p).apply(Create.of(row1, row2, row3).withRowSchema(type));

    // Case 1. run a simple SQL query over input PCollection with BeamSql.simpleQuery;
    PCollection<Row> outputStream =
        inputTable.apply(SqlTransform.query("select c2 PCOLLECTION where STARTS_WITH(c2, '2_')"));

    // print the output record of case 1;
    outputStream
        .apply(
            "log_result",
            MapElements.via(
                new SimpleFunction<Row, Row>() {
                  @Override
                  public Row apply(Row input) {
                    // expect output:
                    //  PCOLLECTION: [3, row, 3.0]
                    //  PCOLLECTION: [2, row, 2.0]
                    System.out.println("PCOLLECTION: " + input.getValues());
                    return input;
                  }
                }))
        .setRowSchema(type);

    // Case 2. run the query with SqlTransform.query over result PCollection of case 1.
    // PCollection<Row> outputStream2 =
    //     PCollectionTuple.of(new TupleTag<>("CASE1_RESULT"), outputStream)
    //         .apply(SqlTransform.query("select c2, sum(c3) from CASE1_RESULT group by c2"));

    // // print the output record of case 2;
    // outputStream2
    //     .apply(
    //         "log_result",
    //         MapElements.via(
    //             new SimpleFunction<Row, Row>() {
    //               @Override
    //               public Row apply(Row input) {
    //                 // expect output:
    //                 //  CASE1_RESULT: [row, 5.0]
    //                 System.out.println("CASE1_RESULT: " + input.getValues());
    //                 return input;
    //               }
    //             }))
    //     .setRowSchema(
    //         Schema.builder().addStringField("stringField").addDoubleField("doubleField").build());

    p.run().waitUntilFinish();
  }
}
