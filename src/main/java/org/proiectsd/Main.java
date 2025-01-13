package org.proiectsd;

import java.util.Arrays;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        // Configurare lista de brokeri
        var brokers = Arrays.asList(
                "tcp://192.168.1.6:1883",
                "tcp://192.168.1.2:1883"
                /*"tcp://broker.hivemq.com:1883",
                "tcp://test.mosquitto.org:1883"*/

        );

        // Generare clientId unic
        String clientId = "Client-" + System.currentTimeMillis();

        // Inițializare client MQTT fault-tolerant
        FaultTolerantMQTTClient mqttClient = new FaultTolerantMQTTClient(brokers, clientId);

        // Conectare la broker
        mqttClient.connect();

        // Interfață CLI
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("\nSelectați o opțiune:");
            System.out.println("1. Abonare la un topic");
            System.out.println("2. Publicare mesaj");
            System.out.println("3. Deconectare și ieșire");
            System.out.print("Alegerea dvs.: ");

            int choice;
            try {
                choice = Integer.parseInt(scanner.nextLine());
            } catch (NumberFormatException e) {
                System.out.println("Opțiune invalidă. Încercați din nou.");
                continue;
            }

            switch (choice) {
                case 1:
                    System.out.print("Introduceți topic-ul: ");
                    String topicToSubscribe = scanner.nextLine();
                    mqttClient.subscribe(topicToSubscribe);
                    break;
                case 2:
                    System.out.print("Introduceți topic-ul: ");
                    String topicToPublish = scanner.nextLine();
                    System.out.print("Introduceți mesajul: ");
                    String message = scanner.nextLine();
                    mqttClient.publish(topicToPublish, message);
                    break;
                case 3:
                    mqttClient.disconnect();
                    System.out.println("Aplicația a fost oprită. La revedere!");
                    scanner.close();
                    return;
                default:
                    System.out.println("Opțiune invalidă. Încercați din nou.");
            }
        }
    }
}






