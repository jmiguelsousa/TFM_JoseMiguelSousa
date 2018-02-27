import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class ClienteServicioBicimad {

    private static KafkaBicimadProductor kafkaBicimadProductor = new KafkaBicimadProductor();
    private static KafkaProducer<Integer, byte[]> kafkaProductor;

    public static void main(String[] args) throws Exception {

        String user = args[0];

        String password = args[1];

        ClienteServicioBicimad http = new ClienteServicioBicimad();

        final String servers = "localhost:9092";
        final String topic = "bicimad";
        int nVeces = 4;

        kafkaProductor = kafkaBicimadProductor.create(servers, topic);

        while (true)
        {
            http.sendGet(user, password);
            Thread.sleep(5000);
        }
    }

    // HTTP GET request
    private void sendGet(String user, String password) throws Exception {

        String url = "https://rbdata.emtmadrid.es:8443/BiciMad/get_stations/" +
                      user + "/" + password;

        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();

        // optional default is GET
        con.setRequestMethod("GET");

        int responseCode = con.getResponseCode();

        if (responseCode == 200) {
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(con.getInputStream()));
            String inputLine;
            StringBuffer response = new StringBuffer();

            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            con.disconnect();

            JSONObject jsonObj = new JSONObject(response.toString());

            try {
                JSONObject jsonObject = new JSONObject(jsonObj.get("data").toString());

                JSONArray jsonArray = jsonObject.getJSONArray("stations");

                for (int i = 0; i < jsonArray.length(); i++) {
                    JSONObject station = jsonArray.getJSONObject(i);
                    String name = station.getString("name");

                    Station stationAvro = Station.newBuilder()
                            .setTime(jsonObj.getString("time"))
                            .setId(station.getInt("id"))
                            .setLatitude(station.getString("latitude"))
                            .setLongitude(station.getString("longitude"))
                            .setName(station.getString("name"))
                            .setLight(station.getString("light"))
                            .setNumber(station.getString("number"))
                            .setAddress(station.getString("address"))
                            .setActivate(station.getString("activate"))
                            .setNoAvailable(station.getString("no_available"))
                            .setTotalBases(station.getInt("total_bases"))
                            .setDockBikes(station.getInt("dock_bikes"))
                            .setFreeBases(station.getInt("free_bases"))
                            .setReservationsCount(station.getInt("reservations_count"))
                            .build();

                    kafkaBicimadProductor.send(kafkaProductor, stationAvro);
                }
            }
            catch (JSONException jse)
            {
                System.out.println(response.toString());
            }
        }
    }

}