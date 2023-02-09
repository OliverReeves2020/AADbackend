package org.example.functions;

import com.google.api.core.ApiFuture;
import com.google.cloud.Timestamp;
import com.google.cloud.firestore.*;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import com.microsoft.azure.functions.annotation.*;
import com.microsoft.azure.functions.*;

import java.io.*;
import java.sql.Time;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;



/**
 * Azure Functions with Timer trigger.
 */
public class TimerTriggerFunction {
    @FunctionName("timerFunction")
    public void run(@TimerTrigger(name = "timer", schedule = "0 0 0 * * *") String timerInfo,
                    final ExecutionContext context) {

        Timestamp TodaysTime= Timestamp.now();


        String jsonString = "{\n" +
                "  \"type\": \"service_account\",\n" +
                "  \"project_id\": \"advanced-analysis-and-design\",\n" +
                "  \"private_key_id\": \"97927e5daddcfd298a90ed60d5ed4d4df89ea2d7\",\n" +
                "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC5tuS6mK6LdPx2\\nmW9H5ql9xh02BXyhlHRUhihY4yetSYzPi+wUcR1n1bNnItuzKFZSjfWCb5STw6pa\\nd9G8fYcERrViqOFDc7a5GLMp+BpvS578gU/x20ofs5wSsRLMg52thLxPTa4k2cD7\\nVSTHiJgvzOmH4EhqVPY5FNEqbfRDa6VZX1MmztOW5cx+4mAaqcokoP4x9itWkvtC\\nYhwRsy0rA4V4ScscRwATzhbp3E+hcgrAKTZ/I9kIj7wkYlMbijPyrb9tOk2OomrO\\n8Js+0e8zolMtPuhsaewkZaTvZFfbcVZ86XjjzvUhuG9tJy8stH0GAqhyVYceDOYd\\nVMwD6RG9AgMBAAECggEAQVyKxNd+uutJ/Y3D2hjm9zK2TjCBLDcc+DCdW6da7ASy\\nT8uNo6Yp9MUEFpzwmO3t77Exwp31u0nGl3+LLB072zftl9MbMQu1OBDm4bMFzrzd\\nMD5svhDxVhX3LKbwmFUMNik8w2ZyD1sqLerJ/oXnFg+281Du+6l6XR2j/hxjtyQ+\\nEjw/z83B6/2cf9J807dZkx1uUoGVtCUSgmJXSMmo5eskhKpx7pWG8RfnBKjIo7i8\\nPgffS0pz3clm74vCDd/WBT/hQy+NcDCGI4kn4KRnQRhCyokjlisdvN7+zyTgIY/f\\n3NQuUwHWutyRi2ynNaI4RMPqS5WtjsffNMHrgIHsSwKBgQDvNHdHSuN16NOCdsja\\nu2VxMXVsS7EBAnDcJaa4bQ9vZTzGc7JsgV3aC+C/UEuTtffTaU9bCrTJJVgtKiIC\\nxAF0MlBdQQVp75MijcyGQ996cte3ZUiS/xxIMzpnuPzCojJTPgXHCr/tQzdaanwe\\nY2e61094PMPF9F8hX2H7/drLuwKBgQDGwPl+P5/8WgIwwTpwPkYUyW1s7hRIpel0\\naxX4br0gt73Oh9obEBkg5XvPoKBdCpxq6DHF41M1MvyjTj1MpQFVWgBbyNoZb9Wo\\ntrUiLGfpGiRzAZrSkBWyoxYIZa7+yQadZR/ZQfrLgYaqc157b/R/BgmQc+aR+Jbm\\nR/0e2pf05wKBgHFp/dfmkPxZvNcRvN/AlDNcEOGc7bqdzqcCjw7IXtYhOPSa7JD3\\nsWjt0szCpGLPl0Y8myx+YgMw2qLnZCLNjibqqCHe71MwDSIH+135ll73ToIc2pEf\\nteghqeJ92hHqV1QShyi9VwyGnxsWe+zokskKxxoqLWSRHf2zvMB+txt7AoGAT5P0\\nURgodmGTfvHe9BH6/Veo+HRaqrvDPVmi8lVkZDUtSgQaz7siVImNiRRHIPmepy9Q\\nvmrGZgaEIN6yrH1ki0f93MIOkSC161c8LvKYqDxPW7zIwIXwKhYiPDp6VokmnN5g\\nYhyIzfKOeLC4o7OvsSw5qBy/tLJTuOAZ+MCNFwECgYEAuSGCwAaNC6tkujfjh+aK\\nZlmpK6Hv+T/+QeL6XA7wiyPujpne/SK2uM8jE7YSgMYUnz60CKFqvK4mvEegMkOX\\nogfd3vzfNPAScz9U9tEtxdhqJeYC19oNQHVu1mMwz55fqaxJDZew+10Cp0mNx//Q\\njP1LujglUcSE5gCio18MPfw=\\n-----END PRIVATE KEY-----\\n\",\n" +
                "  \"client_email\": \"firebase-adminsdk-gaf97@advanced-analysis-and-design.iam.gserviceaccount.com\",\n" +
                "  \"client_id\": \"118120485446522159564\",\n" +
                "  \"auth_uri\": \"https://accounts.google.com/o/oauth2/auth\",\n" +
                "  \"token_uri\": \"https://oauth2.googleapis.com/token\",\n" +
                "  \"auth_provider_x509_cert_url\": \"https://www.googleapis.com/oauth2/v1/certs\",\n" +
                "  \"client_x509_cert_url\": \"https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-gaf97%40advanced-analysis-and-design.iam.gserviceaccount.com\"\n" +
                "}";
        GoogleCredentials credentials;
        try {
            credentials = GoogleCredentials.fromStream(new ByteArrayInputStream(jsonString.getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }




        FirebaseOptions options = null;

            options = new FirebaseOptions.Builder()
                    .setCredentials(credentials)
                    .setDatabaseUrl("https://advanced-analysis-and-design-default-rtdb.europe-west1.firebasedatabase.app")
                    .build();
            context.getLogger().info("works");
        if(FirebaseApp.getApps().isEmpty()){FirebaseApp.initializeApp(options);}
        Firestore db = FirestoreClient.getFirestore();

        //Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
        CollectionReference collection =db.collection("Fridges");
        List<String> documentNames;
        try {
            documentNames = collection.get().get().getDocuments().stream()
                    .map(document -> document.getId()).collect(Collectors.toList());
        } catch (InterruptedException e) {
            return;
        } catch (ExecutionException e) {
            return;
        }

        //we now have all fridges stored in our database
        //cycle through each one with for loop

        for (String Fridge : documentNames) {

            System.out.println(Fridge);
            CollectionReference fridgecollection = db.collection("Fridges/"+Fridge+"/Items");
            List<String> items = null;

            HashMap<String, String> towrite = new HashMap<>();
            try {
                items = fridgecollection.get().get().getDocuments().stream()
                        .map(document -> document.getId()).collect(Collectors.toList());
            } catch (InterruptedException e) {
                return;
            } catch (ExecutionException e) {
                return;
            }
            //for each barcode get items where condition met
            for(String barcodes: items){


                CollectionReference barcodecollection = db.collection("Fridges/"+Fridge+"/Items/"+barcodes+"/Items");
                //define query to be expired items

                Query query=barcodecollection.whereLessThan("ExpiryDate",TodaysTime);

                try {
                    Long counter=query.count().get().get().getCount();
                    if(counter>0){
                        towrite.put(barcodes,counter.toString());
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }



            }

            //write to database doc
            Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            String s="Expired Items collected on"+ TodaysTime+"\n";
            try {
                out.write(s.getBytes());

                for (HashMap.Entry<String, String> entry : towrite.entrySet()) {
                    String r;
                    r= "Item:"+entry.getKey()+"Qauntity:"+entry.getValue()+"\n";

                    out.write(r.getBytes());
                }

                // Get existing data from file
                BlobId blobId = BlobId.of("advanced-analysis-and-design.appspot.com", "Reports/"+Fridge);
                Blob blob = storage.get(blobId);
                if(blob!=null){
                    byte[] existingData = blob.getContent();

                    // Combine existing data and new data
                    byte[] combinedData = new byte[existingData.length + out.toByteArray().length];
                    System.arraycopy(existingData, 0, combinedData, 0, existingData.length);
                    System.arraycopy(out.toByteArray(), 0, combinedData, existingData.length, out.toByteArray().length);

                    BlobInfo blobInfo = BlobInfo.newBuilder(blobId).setContentType("text/plain").build();
                    storage.create(blobInfo, combinedData);
                    context.getLogger().info("Data added to Google Cloud Storage file.");

                }
                else{
                   storage.create(BlobInfo.newBuilder(blobId).setContentType("text/plain").build(), out.toByteArray());
                    context.getLogger().info("new Data Written to Google Cloud Storage file.");
                }






                //BlobInfo blobInfo = storage.update(BlobInfo.newBuilder("Reports", Fridge).build(), out.toByteArray());

                //context.getLogger().info("Data written to Google Cloud Storage: " + blobInfo.getSelfLink());



            } catch (IOException e) {
                throw new RuntimeException(e);
            }


        }









        context.getLogger().info("Hello World!");
    }
}
