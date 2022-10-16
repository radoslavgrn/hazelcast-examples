package demo.jet.logsjet;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.topic.ITopic;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;

@SpringBootApplication
public class LogsJetApplication {

  public static void main(String[] args) {
    SpringApplication.run(LogsJetApplication.class, args);
  }

  @EventListener(ApplicationReadyEvent.class)
  public void onStart() {

    HazelcastInstance hz = Hazelcast.newHazelcastInstance();
    ITopic<String> topic = hz.getReliableTopic("logs-file");
    topic.addMessageListener(new SimpleMessageListener(hz.getJet()));
  }
}
