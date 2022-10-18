package demo.jet.logsjet;


import static com.hazelcast.jet.pipeline.Sinks.list;

import com.hazelcast.jet.JetService;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.test.TestSources;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.extern.apachecommons.CommonsLog;
import org.apache.commons.lang3.ArrayUtils;

@CommonsLog
public class SimpleMessageListener implements MessageListener<String> {

  private final JetService jetService;

  static final String FILE_LIST = "FILE-LIST";

  public SimpleMessageListener(JetService jetService) {
    this.jetService = jetService;
  }

  @SneakyThrows
  public void onMessage(Message<String> message) {
    log.info("Received message from: ".concat(message.getPublishingMember().toString()));
    log.info("Received message content: ".concat(message.getMessageObject()));

    String[] split = message.getMessageObject().split(";");

    if (split.length == 2) {
      Path path = Paths.get("logs/".concat(split[0]));
      Pipeline p = Pipeline.create();

      byte[] bytes = Files.readAllBytes(path);

      p.readFrom(TestSources.items(Arrays.asList(ArrayUtils.toObject(bytes))))
          .writeTo(Sinks.list(FILE_LIST + split[1]));

      jetService.newJob(p).join();
    }
  }
}
