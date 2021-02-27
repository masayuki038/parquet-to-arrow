package net.wrap_trap.parquet_to_arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;

import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.Channels;

public class Test {
  public static void main(String[] args) throws IOException {
//    try (FileOutputStream out = new FileOutputStream("src/test/resources/test.arrow")) {
//      VectorSchemaRoot root = new ParquetToArrow().convert("src/test/resources/test.parquet");
//      try (ArrowWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))) {
//        writer.writeBatch();
//      }
//    }

    VectorSchemaRoot entireSchema = new ParquetToArrow().convert("src/test/resources/2000_6/2000_6_no_dictionary.parquet");
    try (FileWriter writer = new FileWriter("src/test/resources/2000_6/arrow/" + "schema.json")) {
      writer.write(entireSchema.getSchema().toJson());
    }

    VectorSchemaRoot[] roots = new ParquetToArrow().convertByColumnar("src/test/resources/2000_6/2000_6_no_dictionary.parquet");
    // VectorSchemaRoot[] roots = new ParquetToArrow().convertByColumnar("src/test/resources/test.parquet");
    for (VectorSchemaRoot root: roots) {
      try (FileOutputStream out = new FileOutputStream("src/test/resources/2000_6/arrow/" + root.getFieldVectors().get(0).getField().getName() + ".arrow")) {
        try (ArrowWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))) {
          writer.writeBatch();
        }
      }
    }
  }
}
