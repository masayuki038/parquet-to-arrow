package net.wrap_trap.parquet_to_arrow;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.ArrowWriter;

import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.channels.Channels;

public class Main {
  public static void main(String[] args) throws IOException {

    if (args.length != 1) {
      System.out.println("Test [dir]");
      System.exit(1);
    }
    String dir = args[0] + File.separator;

    String[] files = new File(args[0]).list((d, name) -> name.endsWith(".parquet"));
    if (files.length != 1) {
      throw new IllegalStateException("Multiple files found. " + files);
    }

    VectorSchemaRoot entireSchema = new ParquetToArrow().convert(dir + files[0]);
    try (FileWriter writer = new FileWriter(dir + "schema.json")) {
      writer.write(entireSchema.getSchema().toJson());
    }

    VectorSchemaRoot[] roots = new ParquetToArrow().convertByColumnar(dir + files[0]);
    // VectorSchemaRoot[] roots = new ParquetToArrow().convertByColumnar("src/test/resources/test.parquet");
    for (VectorSchemaRoot root: roots) {
      try (FileOutputStream out = new FileOutputStream(dir + root.getFieldVectors().get(0).getField().getName() + ".arrow")) {
        try (ArrowWriter writer = new ArrowFileWriter(root, null, Channels.newChannel(out))) {
          writer.writeBatch();
        }
      }
    }
  }
}
