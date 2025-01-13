package org.proiectsd;

import org.eclipse.paho.client.mqttv3.*;

import java.time.LocalDateTime;
import java.util.List;

public class MqttConnection {
    private List<String> brokers; // Lista de servere MQTT
    private String clientId = "JavaMqttClient";
    private MqttClient client;

    public MqttConnection(List<String> brokers) {
        this.brokers = brokers;
    }

    public void connect() {
        for (String broker : brokers) {
            try {
                log("Încerc conexiunea la broker-ul: " + broker);
                client = new MqttClient(broker, clientId);
                client.setCallback(new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        log("Conexiunea la broker-ul " + broker + " a fost pierdută! Cauza: " + cause.getMessage());
                        reconnect();
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) throws Exception {
                        log("Mesaj primit de la topic-ul: " + topic + ", Conținut: " + new String(message.getPayload()));
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {
                        log("Mesaj livrat complet.");
                    }
                });

                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                client.connect(options);

                log("Conexiunea la broker-ul " + broker + " a fost realizată cu succes!");
                return; // Dacă conexiunea reușește, ieșim din buclă
            } catch (MqttException e) {
                log("Eroare la conexiune cu broker-ul " + broker + ": " + e.getMessage());
            }
        }
        log("Nu s-a putut stabili conexiunea cu niciun broker!");
    }

    public void reconnect() {
        log("Încerc reconectarea...");
        connect();
    }

    public void publishMessage(String topic, String messageContent) {
        try {
            if (client == null || !client.isConnected()) {
                log("Clientul nu este conectat. Publicarea nu este posibilă.");
                return;
            }
            MqttMessage message = new MqttMessage(messageContent.getBytes());
            client.publish(topic, message);
            log("Mesaj publicat pe topic-ul: " + topic + ", Conținut: " + messageContent);
        } catch (MqttException e) {
            log("Eroare la publicare: " + e.getMessage());
        }
    }

    public void subscribeToTopic(String topic) {
        try {
            if (client == null || !client.isConnected()) {
                log("Clientul nu este conectat. Abonarea nu este posibilă.");
                return;
            }
            client.subscribe(topic);
            log("Abonat cu succes la topic-ul: " + topic);
        } catch (MqttException e) {
            log("Eroare la abonare: " + e.getMessage());
        }
    }

    public void disconnect() {
        try {
            if (client != null && client.isConnected()) {
                client.disconnect();
                log("Clientul a fost deconectat.");
            }
        } catch (MqttException e) {
            log("Eroare la deconectare: " + e.getMessage());
        }
    }

    private void log(String message) {
        String timestamp = LocalDateTime.now().toString();
        System.out.println("[" + timestamp + "] " + message);
    }
}


