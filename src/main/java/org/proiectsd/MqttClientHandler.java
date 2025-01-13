package org.proiectsd;

import org.eclipse.paho.client.mqttv3.*;

public class MqttClientHandler {
    private String brokerUrl;
    private String clientId;
    private MqttClient client;

    public MqttClientHandler(String brokerUrl, String clientId) {
        this.brokerUrl = brokerUrl;
        this.clientId = clientId;
    }

    public void connect() {
        try {
            client = new MqttClient(brokerUrl, clientId);
            client.setCallback(new MqttCallback() {

                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Conexiunea a fost pierdută: " + cause.getMessage());
                    boolean reconnected = false;

                    while (!reconnected) {
                        try {
                            System.out.println("Încerc reconectarea...");
                            client.connect();
                            System.out.println("Reconectare reușită!");
                            reconnected = true;
                        } catch (MqttException e) {
                            System.out.println("Eroare la reconectare: " + e.getMessage());
                            try {
                                Thread.sleep(5000); // Așteaptă 5 secunde înainte de o nouă încercare
                            } catch (InterruptedException ie) {
                                Thread.currentThread().interrupt();
                                System.out.println("Reconectarea a fost întreruptă.");
                                break;
                            }
                        }
                    }
                }


                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    Logger.log("[" + clientId + "] Mesaj primit de la topic-ul " + topic + ": " + new String(message.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    Logger.log("[" + clientId + "] Mesaj livrat complet.");
                }
            });

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            client.connect(options);
            Logger.log("[" + clientId + "] Conexiunea la broker-ul " + brokerUrl + " a fost realizată cu succes!");
        } catch (MqttException e) {
            Logger.log("[" + clientId + "] Eroare la conexiune: " + e.getMessage());
        }
    }

    public void subscribe(String topic) {
        try {
            client.subscribe(topic);
            Logger.log("[" + clientId + "] Abonat la topic-ul: " + topic);
        } catch (MqttException e) {
            Logger.log("[" + clientId + "] Eroare la abonare: " + e.getMessage());
        }
    }

    public void publish(String topic, String message) {
        try {
            MqttMessage mqttMessage = new MqttMessage(message.getBytes());
            client.publish(topic, mqttMessage);
            Logger.log("[" + clientId + "] Mesaj publicat pe topic-ul " + topic + ": " + message);
        } catch (MqttException e) {
            Logger.log("[" + clientId + "] Eroare la publicare: " + e.getMessage());
        }
    }

    public void disconnect() {
        try {
            client.disconnect();
            Logger.log("[" + clientId + "] Deconectat.");
        } catch (MqttException e) {
            Logger.log("[" + clientId + "] Eroare la deconectare: " + e.getMessage());
        }
    }
}
