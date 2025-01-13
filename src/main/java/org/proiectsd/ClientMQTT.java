package org.proiectsd;

import org.eclipse.paho.client.mqttv3.*;

import java.util.Scanner;

public class ClientMQTT {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        // Generare clientId unic
        String clientId = args.length > 0 ? args[0] : "Client-" + System.currentTimeMillis();

        System.out.println("Broker-ul implicit este tcp://broker.hivemq.com:1883");
        String brokerUrl = "tcp://broker.hivemq.com:1883";

        // Inițializare client
        try {
            MqttClient client = new MqttClient(brokerUrl, clientId);
            client.setCallback(new MqttCallback() {
                @Override
                public void connectionLost(Throwable cause) {
                    System.out.println("Conexiunea a fost pierdută: " + cause.getMessage());
                }

                @Override
                public void messageArrived(String topic, MqttMessage message) {
                    System.out.println("Mesaj primit de la topic-ul " + topic + ": " + new String(message.getPayload()));
                }

                @Override
                public void deliveryComplete(IMqttDeliveryToken token) {
                    System.out.println("Mesaj livrat complet.");
                }
            });

            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            client.connect(options);

            System.out.println("Conectat la broker-ul: " + brokerUrl + " cu clientId-ul: " + clientId);

            // Meniu pentru abonare și publicare
            while (true) {
                System.out.println("\nSelectați o opțiune:");
                System.out.println("1. Abonare la un topic");
                System.out.println("2. Publicare mesaj");
                System.out.println("3. Deconectare și ieșire");
                int choice = scanner.nextInt();
                scanner.nextLine(); // Consumă linia

                if (choice == 1) {
                    System.out.println("Introduceți topic-ul:");
                    String topic = scanner.nextLine();
                    client.subscribe(topic);
                    System.out.println("Abonat la topic-ul: " + topic);
                } else if (choice == 2) {
                    System.out.println("Introduceți topic-ul:");
                    String topic = scanner.nextLine();
                    System.out.println("Introduceți mesajul:");
                    String message = scanner.nextLine();
                    client.publish(topic, new MqttMessage(message.getBytes()));
                    System.out.println("Mesaj publicat pe topic-ul: " + topic);
                } else if (choice == 3) {
                    client.disconnect();
                    System.out.println("Deconectat de la broker. La revedere!");
                    break;
                } else {
                    System.out.println("Opțiune invalidă. Încercați din nou.");
                }
            }
        } catch (MqttException e) {
            System.err.println("Eroare: " + e.getMessage());
            e.printStackTrace();
        }

        scanner.close();
    }
}


