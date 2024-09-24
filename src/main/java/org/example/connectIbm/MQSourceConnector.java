package org.example.connectIbm;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class MQSourceConnector extends SourceConnector {
    private Map<String, String> config;

    @Override
    public void start(Map<String, String> config) {
        this.config = config;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MQSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        // Obtener la lista de colas desde la configuración
        String queuesStr = config.get("mq.queues");
        List<String> allQueues = Arrays.asList(queuesStr.split(","));
        int numQueues = allQueues.size();

        // Si el número de tareas es mayor que el número de colas, limitar el número de tareas
        int taskCount = Math.min(maxTasks, numQueues);

        List<Map<String, String>> taskConfigs = new ArrayList<>();

        // Dividir las colas entre las tareas
        for (int i = 0; i < taskCount; i++) {
            Map<String, String> taskConfig = new java.util.HashMap<>(config);
            List<String> assignedQueues = new ArrayList<>();

            for (int j = i; j < numQueues; j += taskCount) {
                assignedQueues.add(allQueues.get(j).trim());
            }

            // Reemplazar la configuración de colas con las asignadas a esta tarea
            taskConfig.put("mq.queues", String.join(",", assignedQueues));
            taskConfigs.add(taskConfig);
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
                .define("mq.queues", ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, "Los nombres de las colas de IBM MQ, separados por comas")
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
