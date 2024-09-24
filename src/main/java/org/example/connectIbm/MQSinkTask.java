package org.example.connectIbm;

import com.ibm.mq.*;
import com.ibm.mq.constants.MQConstants;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class MQSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MQSinkTask.class);

    private MQQueueManager queueManager;
    private MQQueue queue;
    private String queueManagerName;
    private String queueName;

    @Override
    public void start(Map<String, String> props) {
        // Configurar conexión a IBM MQ
        this.queueManagerName = props.get("mq.queueManager");
        this.queueName = props.get("mq.queue");
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

            log.info("Conectando a IBM MQ QueueManager: {}, en la cola: {}", queueManagerName, queueName);

            queueManager = new MQQueueManager(queueManagerName);
            queue = queueManager.accessQueue(queueName, MQConstants.MQOO_OUTPUT);

        } catch (MQException e) {
            log.error("Error al conectar con IBM MQ", e);
            throw new ConnectException("No se pudo conectar a IBM MQ", e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        for (SinkRecord record : records) {
            try {
                MQMessage message = new MQMessage();
                message.writeString(record.value().toString());

                MQPutMessageOptions pmo = new MQPutMessageOptions();
                queue.put(message, pmo);

                log.info("Mensaje enviado a MQ: {}", record.value().toString());
            } catch (MQException e) {
                log.error("Error al enviar mensaje a IBM MQ", e);
                throw new ConnectException("Error al enviar mensaje a IBM MQ", e);
            } catch (Exception e) {
                log.error("Error general al procesar mensajes de Kafka", e);
                throw new ConnectException("Error al procesar mensajes", e);
            }
        }
    }

    @Override
    public void stop() {
        log.info("Cerrando conexión a IBM MQ");
        try {
            if (queue != null) queue.close();
            if (queueManager != null) queueManager.disconnect();
        } catch (MQException e) {
            log.error("Error al cerrar la conexión a IBM MQ", e);
        }
    }

    @Override
    public String version() {
        return "1.0";
    }
}
