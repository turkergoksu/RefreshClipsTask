import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;

import java.io.*;
import java.util.*;

public class Main {

    private static final long REFRESH_TIME = 900000; // 900000ms = 15 minutes.

    private static FileInputStream serviceAccount;
    private static FirebaseOptions options;

    private static String databaseURL;

    public static void main(String[] args) {

        File apiCredentialsFile = new File("api_credentials.txt");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(apiCredentialsFile));
            databaseURL = bufferedReader.readLine(); // Just need first line
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            serviceAccount = new FileInputStream("serviceAccount.json");
            options = new FirebaseOptions.Builder()
                    .setCredentials(GoogleCredentials.fromStream(serviceAccount))
                    .setDatabaseUrl(databaseURL)
                    .build();
            FirebaseApp.initializeApp(options);
        } catch (IOException e) {
            e.printStackTrace();
        }

        Timer timer = new Timer();
        RefreshClipsTask task = new RefreshClipsTask();
        timer.scheduleAtFixedRate(task, 0, REFRESH_TIME);

//        Scanner scanner = new Scanner(System.in);
//        while(!scanner.nextLine().equals("Quit")){}
//        System.exit(0);
    }

}
