package org.example.connectIbm;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MQSinkConnector extends SinkConnector {
    private Map<String, String> config;

    @Override
    public void start(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MQSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            taskConfigs.add(config);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        // Cerrar conexiones y recursos si es necesario
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("mq.queueManager", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "El nombre del Queue Manager de IBM MQ")
                .define("mq.queue", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "El nombre de la cola de IBM MQ")
                .define("kafka.topic", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "El nombre del tópico de Kafka")
                .define("mq.channel", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "El nombre del canal de IBM MQ")
                .define("mq.host", ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "El host de IBM MQ")
                .define("mq.port", ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "El puerto de IBM MQ")
                .define("mq.username", ConfigDef.Type.STRING, ConfigDef.Importance.MEDIUM, "El usuario para autenticación en IBM MQ")
                .define("mq.password", ConfigDef.Type.PASSWORD, ConfigDef.Importance.MEDIUM, "La contraseña para autenticación en IBM MQ");
    }

    @Override
    public String version() {
        return "1.0";
    }
}
