package org.proiectsd;

import org.eclipse.paho.client.mqttv3.*;

import java.util.ArrayList;
import java.util.List;

public class FaultTolerantMQTTClient {
    private List<String> brokers; // Lista brokerilor
    private String clientId;
    private MqttClient client;
    private List<String> subscribedTopics; // Lista topicurilor abonate

    public FaultTolerantMQTTClient(List<String> brokers, String clientId) {
        this.brokers = brokers;
        this.clientId = clientId;
        this.subscribedTopics = new ArrayList<>();
    }

    public void connect() {
        for (String broker : brokers) {
            try {
                System.out.println("Încerc conexiunea la broker-ul: " + broker);
                client = new MqttClient(broker, clientId);
                client.setCallback(new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable cause) {
                        System.out.println("Conexiunea la broker-ul " + broker + " a fost pierdută! Cauza: " + cause.getMessage());
                        reconnect();
                    }

                    @Override
                    public void messageArrived(String topic, MqttMessage message) {
                        System.out.println("Mesaj primit de la topic-ul: " + topic + " | Conținut: " + new String(message.getPayload()));
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken token) {
                        System.out.println("Mesaj livrat complet.");
                    }
                });

                MqttConnectOptions options = new MqttConnectOptions();
                options.setCleanSession(true);
                client.connect(options);

                System.out.println("Conectat la broker-ul " + broker + " cu succes!");

                // Reabonează la toate topicurile după reconectare
                reSubscribeAllTopics();

                return; // Dacă conexiunea reușește, ieșim din buclă
            } catch (MqttException e) {
                System.out.println("Eroare la conexiune cu broker-ul: " + broker + " | Detalii: " + e.getMessage());
            }
        }
        System.err.println("Nu s-a putut stabili conexiunea cu niciun broker din listă!");
    }

    public void reconnect() {
        System.out.println("Încerc reconectarea...");
        connect();
    }

    public void publish(String topic, String message) {
        try {
            if (client == null || !client.isConnected()) {
                System.out.println("Clientul nu este conectat. Publicarea nu este posibilă.");
                return;
            }
            MqttMessage mqttMessage = new MqttMessage(message.getBytes());
            client.publish(topic, mqttMessage);
            System.out.println("Mesaj publicat pe topic-ul: " + topic + " | Conținut: " + message);
        } catch (MqttException e) {
            System.out.println("Eroare la publicare: " + e.getMessage());
        }
    }

    public void subscribe(String topic) {
        try {
            if (client == null || !client.isConnected()) {
                System.out.println("Clientul nu este conectat. Abonarea nu este posibilă.");
                return;
            }
            client.subscribe(topic);
            subscribedTopics.add(topic); // Adaugă topic-ul la lista de abonări
            System.out.println("Abonat cu succes la topic-ul: " + topic);
        } catch (MqttException e) {
            System.out.println("Eroare la abonare: " + e.getMessage());
        }
    }

    private void reSubscribeAllTopics() {
        for (String topic : subscribedTopics) {
            try {
                client.subscribe(topic);
                System.out.println("Reabonat cu succes la topic-ul: " + topic);
            } catch (MqttException e) {
                System.out.println("Eroare la reabonare la topic-ul: " + topic + " | Detalii: " + e.getMessage());
            }
        }
    }

    public void disconnect() {
        try {
            if (client != null && client.isConnected()) {
                client.disconnect();
                System.out.println("Clientul a fost deconectat.");
            }
        } catch (MqttException e) {
            System.out.println("Eroare la deconectare: " + e.getMessage());
        }
    }
}
