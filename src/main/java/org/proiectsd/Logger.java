package org.proiectsd;

import java.io.FileWriter;
import java.io.IOException;
import java.time.LocalDateTime;

public class Logger {
    private static final String LOG_FILE = "mqtt_logs.txt";

    // Metodă statică pentru a scrie loguri
    public static void log(String message) {
        String timestamp = LocalDateTime.now().toString();
        String logMessage = "[" + timestamp + "] " + message;

        // Log în consolă
        System.out.println(logMessage);

        // Scriere în fișier
        FileWriter writer = null;
        try {
            writer = new FileWriter(LOG_FILE, true); // Mod de adăugare
            writer.write(logMessage + System.lineSeparator());
        } catch (IOException e) {
            System.err.println("Eroare la scrierea în fișierul de log: " + e.getMessage());
        } finally {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    System.err.println("Eroare la închiderea fișierului de log: " + e.getMessage());
                }
            }
        }
    }
}



