package example.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by zhugb on 16-3-10.
 */
public class AvroTestUsingCodegen {

  public static void main(String[] args) {

    User user1 = new User();
    user1.setName("Alyssa");
    user1.setFavoriteNumber(256);
// Leave favorite color null

// Alternate constructor
    User user2 = new User("Ben", 7, "red");

// Construct via builder
    User user3 = User.newBuilder()
            .setName("Charlie")
            .setFavoriteColor("blue")
            .setFavoriteNumber(null)
            .build();

    // Serialize user1, user2 and user3 to disk
    DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
    DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);

    try {
      dataFileWriter.create(user1.getSchema(), new File("users.avro"));
      dataFileWriter.append(user1);
      dataFileWriter.append(user2);
      dataFileWriter.append(user3);
      dataFileWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    System.out.println();

    // Deserialize Users from disk
    Path workingDir = Paths.get(".").toAbsolutePath().normalize();
    File avroFile = new File(workingDir.toString(),"users.avro");
    DatumReader<User> userDatumReader = new SpecificDatumReader<User>(User.class);
    DataFileReader<User> dataFileReader = null;
    try {
      dataFileReader = new DataFileReader<User>(avroFile, userDatumReader);
    } catch (IOException e) {
      e.printStackTrace();
    }
    User user = null;
    while (dataFileReader.hasNext()) {
// Reuse user object by passing it to next(). This saves us from
// allocating and garbage collecting many objects for files with
// many items.
      try {
        user = dataFileReader.next(user);
      } catch (IOException e) {
        e.printStackTrace();
      }
      System.out.println(user);
    }


  }
}
