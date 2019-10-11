import com.google.firebase.database.*;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.*;

public class RefreshClipsTask extends TimerTask {

    // Sacrificed memory rather than time x(
    private HashMap<Clip, Long> oneDayNotSortedHashMap;
    private HashMap<Clip, Long> oneDaySortedHashMap;
    private ArrayList<Clip> oneDayClipArrayList;

    private HashMap<Clip, Long> oneWeekNotSortedHashMap;
    private HashMap<Clip, Long> oneWeekSortedHashMap;
    private ArrayList<Clip> oneWeekClipArrayList;

    private HashMap<Clip, Long> oneMonthNotSortedHashMap;
    private HashMap<Clip, Long> oneMonthSortedHashMap;
    private ArrayList<Clip> oneMonthClipArrayList;

    private ArrayList<String> streamerIDList;

    private ArrayList<String> apiCredentials;

    private HttpURLConnection connection;

    public RefreshClipsTask() {
    }

    @Override
    public void run() {
        apiCredentials = new ArrayList<>();
        File apiCredentialsFile = new File("api_credentials.txt");
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(apiCredentialsFile));
            String line;
            while ((line = bufferedReader.readLine()) != null){
                apiCredentials.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        oneDayNotSortedHashMap = new HashMap<Clip, Long>();
        oneDaySortedHashMap = new HashMap<Clip, Long>();
        oneDayClipArrayList = new ArrayList<>();

        oneWeekNotSortedHashMap = new HashMap<Clip, Long>();
        oneWeekSortedHashMap = new HashMap<Clip, Long>();
        oneWeekClipArrayList = new ArrayList<>();

        oneMonthNotSortedHashMap = new HashMap<Clip, Long>();
        oneMonthSortedHashMap = new HashMap<Clip, Long>();
        oneMonthClipArrayList = new ArrayList<>();

        streamerIDList = new ArrayList<>();

        FirebaseDatabase.getInstance().getReference()
                .child("StreamerList").addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot snapshot) {
                for (DataSnapshot childSnapshot : snapshot.getChildren()){
                    streamerIDList.add((String) childSnapshot.getValue());
                }

                Calendar calendar = Calendar.getInstance();
                calendar.setTimeZone(TimeZone.getTimeZone("Europe/Istanbul"));
                Date now = calendar.getTime();

                calendar.add(Calendar.DAY_OF_MONTH, -1);
                Date oneDay = calendar.getTime();

                calendar.add(Calendar.DAY_OF_MONTH, -6);
                Date oneWeek = calendar.getTime();

                calendar.add(Calendar.DAY_OF_MONTH, -23);
                Date oneMonth = calendar.getTime();

                oneDay(oneDay, now);
                oneWeek(oneWeek, now);
                oneMonth(oneMonth, now);
            }

            @Override
            public void onCancelled(DatabaseError error) {

            }
        });
    }


    private JsonObject getUserAsJsonObject(String userID){
        JsonObject jsonObject = null;
        try {
            URL url = new URL("https://api.twitch.tv/helix/users?login=" + userID);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Client-ID", apiCredentials.get(1));
            connection.setRequestProperty("Authorization", "Bearer " + apiCredentials.get(2));

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
//            System.out.println(content.toString());

            jsonObject = new Gson().fromJson(content.toString(), JsonObject.class);

//            System.out.println(jsonObject.toString());
//            String a = jsonObject.getAsJsonArray("data").get(0).getAsJsonObject().getAsJsonPrimitive("login").getAsString();
//            System.out.println(a);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonObject.getAsJsonArray("data").get(0).getAsJsonObject();
    }

    private JsonArray getClipsAsJsonArray(String broadcasterID, int first, Date startedAt, Date endedAt){
        SimpleDateFormat dateSDF = new SimpleDateFormat("yyyy-MM-dd");
        SimpleDateFormat timeSDF = new SimpleDateFormat("HH:mm:ss");

        JsonObject jsonObject = null;
        StringBuilder startDate = new StringBuilder();
        startDate.append(dateSDF.format(startedAt)).append("T").append(timeSDF.format(startedAt)).append("Z");
        StringBuilder endDate = new StringBuilder();
        endDate.append(dateSDF.format(endedAt)).append("T").append(timeSDF.format(endedAt)).append("Z");

        try {
            URL url = new URL("https://api.twitch.tv/helix/clips?broadcaster_id=" + broadcasterID
                    + "&first=" + first + "&started_at=" + startDate + "&ended_at=" + endDate);
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Client-ID", apiCredentials.get(1));
            connection.setRequestProperty("Authorization", "Bearer " + apiCredentials.get(2));
            connection.setUseCaches(true);

            BufferedReader in = new BufferedReader(
                    new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
            String inputLine;
            StringBuilder content = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                content.append(inputLine);
            }
            in.close();
//            System.out.println(content.toString());

            jsonObject = new Gson().fromJson(content.toString(), JsonObject.class);

        } catch (IOException e) {
            e.printStackTrace();
        }

        return jsonObject.getAsJsonArray("data");
    }

    private HashMap<Clip, Long> sortByValue(HashMap<Clip, Long> hm) {
        // Create a list from elements of HashMap
        List<Map.Entry<Clip, Long> > list =
                new LinkedList<Map.Entry<Clip, Long> >(hm.entrySet());

        // Sort the list
        Collections.sort(list, new Comparator<Map.Entry<Clip, Long> >() {
            public int compare(Map.Entry<Clip, Long> o1,
                               Map.Entry<Clip, Long> o2)
            {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });

        // put data from sorted list to hashmap
        HashMap<Clip, Long> temp = new LinkedHashMap<Clip, Long>();
        for (Map.Entry<Clip, Long> aa : list) {
            temp.put(aa.getKey(), aa.getValue());
        }
        return temp;
    }

    private void oneDay(Date firstTimeInterval, Date now){
        System.out.println("--------------------------------------------------");
        System.out.println("Getting oneDay clips from Twitch API");

        for (String streamerID : streamerIDList){
            JsonArray jsonArray = getClipsAsJsonArray(streamerID, 20, firstTimeInterval, now);
            for (JsonElement jsonElement : jsonArray){
                JsonObject clipAsJsonObject = jsonElement.getAsJsonObject();
                Clip clip = new Clip(
                        clipAsJsonObject.getAsJsonPrimitive("id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("url").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("embed_url").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("broadcaster_id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("broadcaster_name").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("creator_id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("creator_name").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("game_id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("title").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("view_count").getAsLong(),
                        clipAsJsonObject.getAsJsonPrimitive("created_at").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("thumbnail_url").getAsString());
                oneDayNotSortedHashMap.put(clip, clip.getViewCount());
            }
        }

        oneDaySortedHashMap = sortByValue(oneDayNotSortedHashMap);
        for (Map.Entry<Clip, Long> clip : oneDaySortedHashMap.entrySet()){
            if (oneDayClipArrayList.size() >= 100){
                break;
            } else {
                oneDayClipArrayList.add(clip.getKey());
            }
        }

        DatabaseReference oneDayReference = FirebaseDatabase.getInstance()
                .getReference().child("TR").child("oneDay");

        oneDayReference.child("clips").setValue(oneDayClipArrayList, new DatabaseReference.CompletionListener() {
            @Override
            public void onComplete(DatabaseError error, DatabaseReference ref) {
                System.out.println("Successfully pushed oneDay clips!");
                System.out.println("--------------------------------------------------");
                oneDayNotSortedHashMap = null;
                oneDaySortedHashMap = null;
                oneDayClipArrayList = null;
                System.gc();
            }
        });

//        oneDayReference.removeValue(new DatabaseReference.CompletionListener() {
//            @Override
//            public void onComplete(DatabaseError error, DatabaseReference ref) {
//                System.out.println("Successfully removed oneDay clips!");
//                oneDayReference.child("clips").setValue(oneDayClipArrayList, new DatabaseReference.CompletionListener() {
//                    @Override
//                    public void onComplete(DatabaseError error, DatabaseReference ref) {
//                        System.out.println("Successfully pushed oneDay clips!");
//                        oneDayNotSortedHashMap = null;
//                        oneDaySortedHashMap = null;
//                        oneDayClipArrayList = null;
//                        System.gc();
//                    }
//                });
//            }
//        });
    }

    private void oneWeek(Date firstTimeInterval, Date now){
        System.out.println("--------------------------------------------------");
        System.out.println("Getting oneWeek clips from Twitch API");

        for (String streamerID : streamerIDList){
            JsonArray jsonArray = getClipsAsJsonArray(streamerID, 20, firstTimeInterval, now);
            for (JsonElement jsonElement : jsonArray){
                JsonObject clipAsJsonObject = jsonElement.getAsJsonObject();
                Clip clip = new Clip(
                        clipAsJsonObject.getAsJsonPrimitive("id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("url").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("embed_url").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("broadcaster_id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("broadcaster_name").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("creator_id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("creator_name").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("game_id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("title").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("view_count").getAsLong(),
                        clipAsJsonObject.getAsJsonPrimitive("created_at").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("thumbnail_url").getAsString());
                oneWeekNotSortedHashMap.put(clip, clip.getViewCount());
            }
        }

        oneWeekSortedHashMap = sortByValue(oneWeekNotSortedHashMap);
        for (Map.Entry<Clip, Long> clip : oneWeekSortedHashMap.entrySet()){
            if (oneWeekClipArrayList.size() >= 100){
                break;
            } else {
                oneWeekClipArrayList.add(clip.getKey());
            }
        }

        DatabaseReference oneWeekReference = FirebaseDatabase.getInstance()
                .getReference().child("TR").child("oneWeek");

        oneWeekReference.child("clips").setValue(oneWeekClipArrayList, new DatabaseReference.CompletionListener() {
            @Override
            public void onComplete(DatabaseError error, DatabaseReference ref) {
                System.out.println("Successfully pushed oneWeek clips!");
                System.out.println("--------------------------------------------------");
                oneWeekNotSortedHashMap = null;
                oneWeekSortedHashMap = null;
                oneWeekClipArrayList = null;
                System.gc();
            }
        });

//        oneWeekReference.removeValue(new DatabaseReference.CompletionListener() {
//            @Override
//            public void onComplete(DatabaseError error, DatabaseReference ref) {
//                System.out.println("Successfully removed oneWeek clips!");
//                oneWeekReference.child("clips").setValue(oneWeekClipArrayList, new DatabaseReference.CompletionListener() {
//                    @Override
//                    public void onComplete(DatabaseError error, DatabaseReference ref) {
//                        System.out.println("Successfully pushed oneWeek clips!");
//                        oneWeekNotSortedHashMap = null;
//                        oneWeekSortedHashMap = null;
//                        oneWeekClipArrayList = null;
//                        System.gc();
//                    }
//                });
//            }
//        });
    }

    private void oneMonth(Date firstTimeInterval, Date now){
        System.out.println("--------------------------------------------------");
        System.out.println("Getting oneMonth clips from Twitch API");

        for (String streamerID : streamerIDList){
            JsonArray jsonArray = getClipsAsJsonArray(streamerID, 20, firstTimeInterval, now);
            for (JsonElement jsonElement : jsonArray){
                JsonObject clipAsJsonObject = jsonElement.getAsJsonObject();
                Clip clip = new Clip(
                        clipAsJsonObject.getAsJsonPrimitive("id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("url").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("embed_url").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("broadcaster_id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("broadcaster_name").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("creator_id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("creator_name").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("game_id").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("title").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("view_count").getAsLong(),
                        clipAsJsonObject.getAsJsonPrimitive("created_at").getAsString(),
                        clipAsJsonObject.getAsJsonPrimitive("thumbnail_url").getAsString());
                oneMonthNotSortedHashMap.put(clip, clip.getViewCount());
            }
        }

        oneMonthSortedHashMap = sortByValue(oneMonthNotSortedHashMap);
        for (Map.Entry<Clip, Long> clip : oneMonthSortedHashMap.entrySet()){
            if (oneMonthClipArrayList.size() >= 100){
                break;
            } else {
                oneMonthClipArrayList.add(clip.getKey());
            }
        }

        DatabaseReference oneMonthReference = FirebaseDatabase.getInstance()
                .getReference().child("TR").child("oneMonth");

        oneMonthReference.child("clips").setValue(oneMonthClipArrayList, new DatabaseReference.CompletionListener() {
            @Override
            public void onComplete(DatabaseError error, DatabaseReference ref) {
                System.out.println("Successfully pushed oneMonth clips!");
                System.out.println("--------------------------------------------------");
                oneMonthNotSortedHashMap = null;
                oneMonthSortedHashMap = null;
                oneMonthClipArrayList = null;
                System.gc();
//                        System.exit(0);
            }
        });

//        oneMonthReference.removeValue(new DatabaseReference.CompletionListener() {
//            @Override
//            public void onComplete(DatabaseError error, DatabaseReference ref) {
//                System.out.println("Successfully removed oneMonth clips!");
//                oneMonthReference.child("clips").setValue(oneMonthClipArrayList, new DatabaseReference.CompletionListener() {
//                    @Override
//                    public void onComplete(DatabaseError error, DatabaseReference ref) {
//                        System.out.println("Successfully pushed oneMonth clips!");
//                        oneMonthNotSortedHashMap = null;
//                        oneMonthSortedHashMap = null;
//                        oneMonthClipArrayList = null;
//                        System.gc();
//
////                        System.exit(0);
//                    }
//                });
//            }
//        });
    }

}
