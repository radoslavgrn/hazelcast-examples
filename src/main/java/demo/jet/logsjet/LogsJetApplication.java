package demo.jet.logsjet;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class LogsJetApplication {

  JetInstance jetInstance;

  static final String FILE_LIST = "FILE-LIST";

  public LogsJetApplication(@Qualifier("jetInstance") JetInstance jetInstance) {
    this.jetInstance = jetInstance;
  }

  public static void main(String[] args) {
    SpringApplication.run(LogsJetApplication.class, args);
  }

  @EventListener(ApplicationReadyEvent.class)
  public void onStart() throws IOException {
    Path path = Paths.get("logs/spring-boot-logger.log");
    Pipeline p = Pipeline.create();

    byte[] bytes = Files.readAllBytes(path);

    p.readFrom(TestSources.items(Arrays.asList(ArrayUtils.toObject(bytes))))
        .writeTo(Sinks.list(FILE_LIST));

    jetInstance.newJob(p).join();
  }
}
