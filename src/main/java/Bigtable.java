import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.ServerStream;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminClient;
import com.google.cloud.bigtable.admin.v2.BigtableTableAdminSettings;
import com.google.cloud.bigtable.admin.v2.models.CreateTableRequest;
import com.google.cloud.bigtable.data.v2.BigtableDataClient;
import com.google.cloud.bigtable.data.v2.BigtableDataSettings;
import com.google.cloud.bigtable.data.v2.models.BulkMutation;
import com.google.cloud.bigtable.data.v2.models.Query;
import com.google.cloud.bigtable.data.v2.models.Row;
import com.google.cloud.bigtable.data.v2.models.RowCell;
import com.google.cloud.bigtable.data.v2.models.RowMutationEntry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.google.auth.oauth2.GoogleCredentials;

/*
 * Use Google Bigtable to store and analyze sensor data.
 */
public class Bigtable {
        public final String projectId = System.getenv().getOrDefault("BT_PROJECT_ID",
                        System.getProperty("bt.projectId", "capstone-473802"));
        public final String instanceId = System.getenv().getOrDefault("BT_INSTANCE_ID",
                        System.getProperty("bt.instanceId", "assignment-4"));
        public final String COLUMN_FAMILY = "sensor";
        public final String tableId = "weather_g24ai2066"; // Changed to make it unique
        public BigtableDataClient dataClient;
        public BigtableTableAdminClient adminClient;

        public static void main(String[] args) throws Exception {
                Bigtable bt = new Bigtable();
                bt.run();
        }

        public void connect() throws IOException {
                GoogleCredentials credentials = resolveCredentials();
                BigtableTableAdminSettings.Builder adminBuilder = BigtableTableAdminSettings.newBuilder()
                        .setProjectId(projectId)
                        .setInstanceId(instanceId);
                if (credentials != null) {
                        adminBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
                }
                adminClient = BigtableTableAdminClient.create(adminBuilder.build());
                BigtableDataSettings.Builder dataBuilder = BigtableDataSettings.newBuilder()
                        .setProjectId(projectId)
                        .setInstanceId(instanceId);
                if (credentials != null) {
                        dataBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
                }
                dataClient = BigtableDataClient.create(dataBuilder.build());
                System.out.println("Connected to Bigtable successfully! Project=" + projectId + ", Instance=" + instanceId);
        }

        private GoogleCredentials resolveCredentials() throws IOException {
                try {
                        return GoogleCredentials.getApplicationDefault();
                } catch (IOException e) {}
                String envPath = System.getenv("GOOGLE_APPLICATION_CREDENTIALS");
                if (envPath != null && !envPath.isEmpty() && new File(envPath).isFile()) {
                        return GoogleCredentials.fromStream(new FileInputStream(envPath));
                }
                String home = System.getProperty("user.home");
                File defaultAdc = new File(home + "/.config/gcloud/application_default_credentials.json");
                if (defaultAdc.isFile()) {
                        return GoogleCredentials.fromStream(new FileInputStream(defaultAdc));
                }
                File legacyDir = new File(home + "/.config/gcloud/legacy_credentials");
                if (legacyDir.isDirectory()) {
                        File[] users = legacyDir.listFiles();
                        if (users != null) {
                                for (File userDir : users) {
                                        File adc = new File(userDir, "adc.json");
                                        if (adc.isFile()) {
                                                return GoogleCredentials.fromStream(new FileInputStream(adc));
                                        }
                                }
                        }
                }
                throw new IOException("Application Default Credentials not found. Please run 'gcloud auth application-default login' or set GOOGLE_APPLICATION_CREDENTIALS.");
        }

        public void run() throws Exception {
                connect();
                deleteTable();
                createTable();
                loadData();
                int temp = query1();
                System.out.println("Temperature: " + temp);
                int windspeed = query2();
                System.out.println("Windspeed: " + windspeed);
                ArrayList<Object[]> data = query3();
                StringBuilder buf = new StringBuilder();
                for (Object[] vals : data) {
                        for (Object v : vals) buf.append(v).append(" ");
                        buf.append("\n");
                }
                System.out.println(buf.toString());
                temp = query4();
                System.out.println("Temperature: " + temp);
                close();
        }

        public void close() {
                dataClient.close();
                adminClient.close();
        }

        public void createTable() {
                if (!adminClient.exists(tableId)) {
                        System.out.println("Creating table: " + tableId);
                        CreateTableRequest createTableRequest = CreateTableRequest.of(tableId)
                                .addFamily(COLUMN_FAMILY);
                        adminClient.createTable(createTableRequest);
                        System.out.printf("Table %s created successfully%n", tableId);
                } else {
                        System.out.println("Table already exists: " + tableId);
                }
        }

        public void loadData() throws Exception {
                try {
                        System.out.println("Load data for SeaTac");
                        loadStationData("seatac.csv", "SEA");
                        System.out.println("Loading data for Vancouver");
                        loadStationData("vancouver.csv", "YVR");
                        System.out.println("Loading data for Portland");
                        loadStationData("portland.csv", "PDX");
                } catch (Exception e) {
                        throw new Exception(e);
                }
        }

        private void loadStationData(String filename, String stationId) throws Exception {
                BufferedReader br = openReaderFromData(filename);
                String line;
                int lineCount = 0;
                String lastHourProcessed = "";
                BulkMutation bulkMutation = BulkMutation.create(tableId);
                int pendingInBatch = 0;
                br.readLine();
                br.readLine();
                while ((line = br.readLine()) != null) {
                        lineCount++;
                        String[] parts = line.split(",");
                        if (parts.length < 9) continue;
                        String date = parts[1].trim();
                        String time = parts[2].trim();
                        String temperature = parts[3].trim();
                        String dewpoint = parts[4].trim();
                        String humidity = parts[5].trim();
                        String windspeed = parts[6].trim();
                        String gust = parts[7].trim();
                        String pressure = parts[8].trim();
                        String hour = time.split(":")[0];
                        String hourKey = date + "#" + hour;
                        if (hourKey.equals(lastHourProcessed)) continue;
                        lastHourProcessed = hourKey;
                        String rowKey = stationId + "#" + date + "#" + String.format("%02d", Integer.parseInt(hour));
                        RowMutationEntry entry = RowMutationEntry.create(rowKey)
                                .setCell(COLUMN_FAMILY, "temperature", temperature)
                                .setCell(COLUMN_FAMILY, "dewpoint", dewpoint)
                                .setCell(COLUMN_FAMILY, "humidity", humidity)
                                .setCell(COLUMN_FAMILY, "windspeed", windspeed)
                                .setCell(COLUMN_FAMILY, "gust", gust)
                                .setCell(COLUMN_FAMILY, "pressure", pressure)
                                .setCell(COLUMN_FAMILY, "date", date)
                                .setCell(COLUMN_FAMILY, "hour", hour);
                        bulkMutation.add(entry);
                        pendingInBatch++;
                        if (pendingInBatch >= 1000) {
                                dataClient.bulkMutateRows(bulkMutation);
                                bulkMutation = BulkMutation.create(tableId);
                                pendingInBatch = 0;
                                System.out.println("Loaded " + lineCount + " rows for " + stationId);
                        }
                }
                if (pendingInBatch > 0) {
                        dataClient.bulkMutateRows(bulkMutation);
                }
                br.close();
                System.out.println("Finished loading data for " + stationId + ": " + lineCount + " total rows processed");
        }

        private BufferedReader openReaderFromData(String filename) throws IOException {
                String[] candidates = new String[] {
                        "data/" + filename,
                        "./data/" + filename,
                        "../data/" + filename
                };
                for (String c : candidates) {
                        File f = new File(c);
                        if (f.isFile()) {
                                System.out.println("Reading: " + f.getPath());
                                return new BufferedReader(new FileReader(f));
                        }
                }
                InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("data/" + filename);
                if (in != null) {
                        System.out.println("Reading from classpath: data/" + filename);
                        return new BufferedReader(new InputStreamReader(in));
                }
                throw new IOException("Could not locate data file: " + filename + " in data/ or classpath.");
        }

        public int query1() throws Exception {
                System.out.println("Executing query #1.");
                String rowKey = "YVR#2022-10-01#10";
                try {
                        Row row = dataClient.readRow(tableId, rowKey);
                        if (row == null) {
                                System.out.println("Row not found for: " + rowKey);
                                return 0;
                        }
                        List<RowCell> cells = row.getCells(COLUMN_FAMILY, "temperature");
                        if (cells.isEmpty()) return 0;
                        String tempStr = cells.get(0).getValue().toStringUtf8();
                        return Integer.parseInt(tempStr);
                } catch (Exception e) {
                        System.err.println("Error in query1: " + e.getMessage());
                        return 0;
                }
        }

        public int query2() throws Exception {
                System.out.println("Executing query #2.");
                int maxWindSpeed = 0;
                Query query = Query.create(tableId)
                        .range("PDX#2022-09-01", "PDX#2022-09-30#99");
                try {
                        ServerStream<Row> rows = dataClient.readRows(query);
                        for (Row row : rows) {
                                List<RowCell> cells = row.getCells(COLUMN_FAMILY, "windspeed");
                                if (!cells.isEmpty()) {
                                        String windspeedStr = cells.get(0).getValue().toStringUtf8();
                                        if (!windspeedStr.equals("M")) {
                                                int windspeed = Integer.parseInt(windspeedStr);
                                                if (windspeed > maxWindSpeed) {
                                                        maxWindSpeed = windspeed;
                                                }
                                        }
                                }
                        }
                } catch (Exception e) {
                        System.err.println("Error in query2: " + e.getMessage());
                }
                return maxWindSpeed;
        }

        public ArrayList<Object[]> query3() throws Exception {
                System.out.println("Executing query #3.");
                ArrayList<Object[]> data = new ArrayList<>();
                Query query = Query.create(tableId)
                        .range("SEA#2022-10-02#00", "SEA#2022-10-02#99");
                try {
                        ServerStream<Row> rows = dataClient.readRows(query);
                        for (Row row : rows) {
                                String date = getCellValue(row, "date");
                                String hour = getCellValue(row, "hour");
                                String tempStr = getCellValue(row, "temperature");
                                String dewpointStr = getCellValue(row, "dewpoint");
                                String humidity = getCellValue(row, "humidity");
                                String windspeed = getCellValue(row, "windspeed");
                                String pressure = getCellValue(row, "pressure");
                                int temp = 0, dewpoint = 0;
                                try {
                                        temp = Integer.parseInt(tempStr);
                                        dewpoint = Integer.parseInt(dewpointStr);
                                } catch (NumberFormatException e) {}
                                Object[] rowData = { date, hour, temp, dewpoint, humidity, windspeed, pressure };
                                data.add(rowData);
                        }
                } catch (Exception e) {
                        System.err.println("Error in query3: " + e.getMessage());
                }
                return data;
        }

        public int query4() throws Exception {
                System.out.println("Executing query #4.");
                int maxTemp = -100;
                String[] stations = { "PDX", "SEA", "YVR" };
                String[] months = { "2022-07", "2022-08" };
                try {
                        for (String station : stations) {
                                for (String month : months) {
                                        String startKey = station + "#" + month + "-01";
                                        String endKey = station + "#" + month + "-31#99";
                                        Query query = Query.create(tableId).range(startKey, endKey);
                                        ServerStream<Row> rows = dataClient.readRows(query);
                                        for (Row row : rows) {
                                                List<RowCell> cells = row.getCells(COLUMN_FAMILY, "temperature");
                                                if (!cells.isEmpty()) {
                                                        String tempStr = cells.get(0).getValue().toStringUtf8();
                                                        try {
                                                                int temp = Integer.parseInt(tempStr);
                                                                if (temp > maxTemp) {
                                                                        maxTemp = temp;
                                                                }
                                                        } catch (NumberFormatException e) {}
                                                }
                                        }
                                }
                        }
                } catch (Exception e) {
                        System.err.println("Error in query4: " + e.getMessage());
                }
                return maxTemp;
        }

        // ...existing code...

        public void deleteTable() {
                if (adminClient.exists(tableId)) {
                        System.out.println("\nDeleting table: " + tableId);
                        try {
                                adminClient.deleteTable(tableId);
                                System.out.printf("Table %s deleted successfully%n", tableId);
                        } catch (NotFoundException e) {
                                System.err.println("Failed to delete table: " + e.getMessage());
                        }
                } else {
                        System.out.println("\nTable does not exist, skipping deletion: " + tableId);
                }
        }

        private String getCellValue(Row row, String columnQualifier) {
                List<RowCell> cells = row.getCells(COLUMN_FAMILY, columnQualifier);
                if (cells.isEmpty()) return "";
                return cells.get(0).getValue().toStringUtf8();
        }
}
