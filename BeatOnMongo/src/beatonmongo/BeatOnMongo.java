/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package beatonmongo;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

/**
 *
 * @author jdguerrera
 */
public class BeatOnMongo {
    
    private static  String mongoAddress0;
    private static  String mongoAddress1;
    private static String mongoDB;
    private static SimpleDateFormat dateFormatter;
    private static DB db;

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws ParseException {
        // TODO code application logic here
        mongoAddress0 = "stgs-mongodb01.jfk.ad.radio.com";
        mongoAddress1 = "stgs-mongodb02.jfk.ad.radio.com";
        mongoDB = "Stations";
        
        dateFormatter = new SimpleDateFormat("MM/dd/yy H:m:s Z");
        Date startDate = dateFormatter.parse("10/15/12 00:00:00 0000");
        Date endDate = dateFormatter.parse("10/16/12 00:00:00 0000");
        
        getOnAirList(151, startDate, endDate);
        
    }
    
    private static void mongoConnection() throws UnknownHostException {
        Mongo m = new Mongo(Arrays.asList(
                                      new ServerAddress(mongoAddress0), 
                                      new ServerAddress(mongoAddress1)));
        m.s
        db = m.getDB(mongoDB);
    }
    
    private static void getOnAirList(int r20ID, Date startDate, Date endDate) {
        DBCollection coll = db.getCollection("OnAirInfo");
        BasicDBObject query = new BasicDBObject();
        
        query.put("r20id", r20ID);
        query.put("playtime", new BasicDBObject("$gt", startDate).append("$lte", endDate));
        
        DBCursor cursor = coll.find(query).sort(new BasicDBObject("playtime", -1));
        try {
            while(cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }
    }            
}
