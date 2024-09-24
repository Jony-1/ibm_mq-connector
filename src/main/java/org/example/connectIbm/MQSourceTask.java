package org.example.connectIbm;

import com.ibm.mq.*;
import com.ibm.mq.constants.MQConstants;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import java.util.*;

public class MQSourceTask extends SourceTask {
    private List<MQQueue> queues;
    private MQQueueManager queueManager;
    private String kafkaTopic;
    private Map<String, String> props;

    @Override
    public void start(Map<String, String> props) {
        this.props = props;
        queues = new ArrayList<>();
        kafkaTopic = props.get("kafka.topic");

        // Configurar conexión a IBM MQ
        String queueManagerName = props.get("mq.queueManager");
        String queuesStr = props.get("mq.queues");
        List<String> queueNames = Arrays.asList(queuesStr.split(","));
        String host = props.get("mq.host");
        int port = Integer.parseInt(props.get("mq.port"));
        String channel = props.get("mq.channel");
        String username = props.get("mq.username");
        String password = props.get("mq.password");

        try {
            MQEnvironment.hostname = host;
            MQEnvironment.port = port;
            MQEnvironment.channel = channel;

            if (username != null && password != null) {
                MQEnvironment.userID = username;
                MQEnvironment.password = password;
            }

            queueManager = new MQQueueManager(queueManagerName);

            for (String queueName : queueNames) {
                MQQueue queue = queueManager.accessQueue(queueName.trim(), MQConstants.MQOO_INPUT_AS_Q_DEF);
                queues.add(queue);
            }
        } catch (MQException e) {
            throw new ConnectException("Error al conectar con IBM MQ", e);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();

        for (MQQueue queue : queues) {
            try {
                // Obtener el mensaje desde IBM MQ
                MQGetMessageOptions gmo = new MQGetMessageOptions();
                gmo.options = MQConstants.MQGMO_WAIT | MQConstants.MQGMO_CONVERT;
                MQMessage message = new MQMessage();
                queue.get(message, gmo);

                String messageContent = message.readStringOfByteLength(message.getMessageLength());

                // Crear registro para enviar a Kafka
                Map<String, String> sourcePartition = new HashMap<>();
                sourcePartition.put("queueManager", props.get("mq.queueManager"));
                sourcePartition.put("queue", queue.getName());

                Map<String, Object> sourceOffset = new HashMap<>();
                // IBM MQ no tiene offsets como Kafka, pero podrías implementar un mecanismo si es necesario

                SourceRecord record = new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        kafkaTopic,
                        null,
                        messageContent
                );
                records.add(record);
            } catch (MQException e) {
                if (e.reasonCode == MQConstants.MQRC_NO_MSG_AVAILABLE) {
                    // No hay mensajes disponibles en esta cola
                    continue;   // Continuar con la siguiente cola

                } else {
                    throw new ConnectException("Error al obtener mensaje de IBM MQ", e);
                }
            } catch (Exception e) {
                throw new ConnectException("Error al procesar mensaje de IBM MQ", e);
            }
        }

        // Esperar un breve período si no se obtuvieron mensajes para evitar un ciclo apretado
        if (records.isEmpty()) {
            Thread.sleep(1000);
        }

        return records;
    }

    @Override
    public void stop() {
        try {
            for (MQQueue queue : queues) {
                queue.close();
            }
            if (queueManager != null) {
                queueManager.disconnect();
            }
        } catch (MQException e) {
            // Manejar el error de cierre de cola
            e.printStackTrace();
        }
    }

    @Override
    public String version() {
        return "1.0";
    }
}
