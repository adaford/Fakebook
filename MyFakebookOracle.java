package project2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MyFakebookOracle extends FakebookOracle {

    static String prefix = "jiaqni.";

    // You must use the following variable as the JDBC connection
    Connection oracleConnection = null;

    // You must refer to the following variables for the corresponding tables in your database
    String cityTableName = null;
    String userTableName = null;
    String friendsTableName = null;
    String currentCityTableName = null;
    String hometownCityTableName = null;
    String programTableName = null;
    String educationTableName = null;
    String eventTableName = null;
    String participantTableName = null;
    String albumTableName = null;
    String photoTableName = null;
    String coverPhotoTableName = null;
    String tagTableName = null;


    // DO NOT modify this constructor
    public MyFakebookOracle(String dataType, Connection c) {
        super();
        oracleConnection = c;
        // You will use the following tables in your Java code
        cityTableName = prefix + dataType + "_CITIES";
        userTableName = prefix + dataType + "_USERS";
        friendsTableName = prefix + dataType + "_FRIENDS";
        currentCityTableName = prefix + dataType + "_USER_CURRENT_CITY";
        hometownCityTableName = prefix + dataType + "_USER_HOMETOWN_CITY";
        programTableName = prefix + dataType + "_PROGRAMS";
        educationTableName = prefix + dataType + "_EDUCATION";
        eventTableName = prefix + dataType + "_USER_EVENTS";
        albumTableName = prefix + dataType + "_ALBUMS";
        photoTableName = prefix + dataType + "_PHOTOS";
        tagTableName = prefix + dataType + "_TAGS";
    }


    @Override
    // ***** Query 0 *****
    public void findMonthOfBirthInfo() {

        // Scrollable result set allows us to read forward (using next())
        // and also backward.
        // This is needed here to support the user of isFirst() and isLast() methods,
        // but in many cases you will not need it.
        // To create a "normal" (unscrollable) statement, you would simply call
        // Statement stmt = oracleConnection.createStatement();
        //
        try (Statement stmt =
                     oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                             ResultSet.CONCUR_READ_ONLY)) {

            // For each month, find the number of users born that month
            // Sort them in descending order of count
            ResultSet rst = stmt.executeQuery("select count(*), month_of_birth from " +
                    userTableName +
                    " where month_of_birth is not null group by month_of_birth order by 1 desc");

            this.monthOfMostUsers = 0;
            this.monthOfLeastUsers = 0;
            this.totalUsersWithMonthOfBirth = 0;

            // Get the month with most users, and the month with least users.
            // (Notice that this only considers months for which the number of users is > 0)
            // Also, count how many total users have listed month of birth (i.e., month_of_birth not null)
            //
            while (rst.next()) {
                int count = rst.getInt(1);
                int month = rst.getInt(2);
                if (rst.isFirst())
                    this.monthOfMostUsers = month;
                if (rst.isLast())
                    this.monthOfLeastUsers = month;
                this.totalUsersWithMonthOfBirth += count;
            }

            // Get the names of users born in the "most" month
            rst = stmt.executeQuery("select user_id, first_name, last_name from " +
                    userTableName + " where month_of_birth=" + this.monthOfMostUsers);
            while (rst.next()) {
                Long uid = rst.getLong(1);
                String firstName = rst.getString(2);
                String lastName = rst.getString(3);
                this.usersInMonthOfMost.add(new UserInfo(uid, firstName, lastName));
            }

            // Get the names of users born in the "least" month
            rst = stmt.executeQuery("select first_name, last_name, user_id from " +
                    userTableName + " where month_of_birth=" + this.monthOfLeastUsers);
            while (rst.next()) {
                String firstName = rst.getString(1);
                String lastName = rst.getString(2);
                Long uid = rst.getLong(3);
                this.usersInMonthOfLeast.add(new UserInfo(uid, firstName, lastName));
            }

            // Close statement and result set
            rst.close();
            stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // ***** Query 1 *****
    // Find information about users' names:
    // (1) The longest first name (if there is a tie, include all in result)
    // (2) The shortest first name (if there is a tie, include all in result)
    // (3) The most common first name, and the number of times it appears (if there
    //      is a tie, include all in result)
    //
    public void findNameInfo() { // Query1
        // Find the following information from your database and store the information as shown
        try (Statement stmt =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
        
            ResultSet rst = stmt.executeQuery("select first_name, LENGTH(first_name) from  " +
                    userTableName + " order by 2");
                
                int longest_length = 0;
                int shortest_length = 0;
                while (rst.next()) {
                if(rst.isFirst()) {
                        String first_name_short = rst.getString(1);
                        shortest_length = rst.getInt(2);
                        this.shortestFirstNames.add(first_name_short);
           
                } 
                else if (!rst.isFirst() && rst.getInt(2) == shortest_length) {
                    String first_name_short = rst.getString(1);
                    this.shortestFirstNames.add(first_name_short);
                }
                
                else if (rst.isLast()) {
                        String first_name_long = rst.getString(1);
                        longest_length = rst.getInt(2);
                        this.longestFirstNames.add(first_name_long);
                    }
                }
            
                while(rst.previous()) {
                    if(rst.getInt(2) == longest_length && !rst.isLast()) {
                        String first_name_long = rst.getString(1);
                        this.longestFirstNames.add(first_name_long);
                    }
                }
                
                rst = stmt.executeQuery("select first_name, count(*) from  " +
                        userTableName + " group by first_name order by 2 DESC");
                
                int first_count = 0;
                while(rst.next()) {
                    int current_count = rst.getInt(2);
                    if(rst.isFirst()) {
                        first_count = rst.getInt(2);
                        String first_name_common = rst.getString(1);
                        this.mostCommonFirstNames.add(first_name_common);
                        this.mostCommonFirstNamesCount = first_count;
                    } else if (current_count == first_count) {
                        String first_name_common = rst.getString(1);
                        this.mostCommonFirstNames.add(first_name_common);
                    }
                    
                }
                
                 // Close statement and result set
                rst.close();
                stmt.close();
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    
    }

    @Override
    // ***** Query 2 *****
    // Find the user(s) who have no friends in the network
    //
    // Be careful on this query!
    // Remember that if two users are friends, the friends table
    // only contains the pair of user ids once, subject to
    // the constraint that user1_id < user2_id
    //
    public void lonelyUsers() {
        // Find the following information from your database and store the information as shown
        
        
        try (Statement stmt =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            
            ResultSet rst = stmt.executeQuery("select user_id, first_name, last_name from " 
            + userTableName + " where user_id not in (select user1_id from " + friendsTableName + ") and user_id not in (select user2_id from " + friendsTableName + ")");
            
            while(rst.next()) {
                Long id = rst.getLong(1);
                String first = rst.getString(2);
                String last = rst.getString(3);
                this.lonelyUsers.add(new UserInfo(id, first, last));
            }

            rst.close();
            stmt.close();
        
            
        } catch (SQLException err) {
            System.err.println(err.getMessage());
    }
    
 
 }

    @Override
    // ***** Query 3 *****
    // Find the users who do not live in their hometowns
    // (I.e., current_city != hometown_city)
    //
    public void liveAwayFromHome() {
        
        try (Statement stmt =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            
            ResultSet rst = stmt.executeQuery("select u.user_id, u.first_name, u.last_name from " + userTableName + " u inner join "
                    + hometownCityTableName + " h on u.user_id = h.user_id inner join " +
                    currentCityTableName + " c on u.user_id = c.user_id where h.hometown_city_id != c.current_city_id");
            
            while(rst.next()) {
                Long id = rst.getLong(1);
                String first = rst.getString(2);
                String last = rst.getString(3);
                this.liveAwayFromHome.add(new UserInfo(id, first, last));
            }
            
            rst.close();
            stmt.close();
            
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
        
        
  }

    @Override
    // **** Query 4 ****
    // Find the top-n photos based on the number of tagged users
    // If there are ties, choose the photo with the smaller numeric PhotoID first
    //
    public void findPhotosWithMostTags(int n) {
        try (Statement stmt =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst = stmt.executeQuery("select tag_photo_id, count(*) from " + tagTableName + " group by tag_photo_id order by 2 desc, 1 asc");

            int count = 0;
            while(rst.next() && count < n) {
                String photoId = rst.getString(1);

                try (Statement stmt2 =
                    oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

                ResultSet rst2 = stmt2.executeQuery("select p.album_id, a.album_name, p.photo_caption, p.photo_link from " + 
                    photoTableName + " p, " + albumTableName + " a where p.photo_id = " + photoId + " and p.album_id = a.album_id"); 
                
                rst2.next();

                String albumId = rst2.getString(1);
                String albumName = rst2.getString(2);
                String photoCaption = rst2.getString(3);
                String photoLink = rst2.getString(4);
                PhotoInfo p = new PhotoInfo(photoId, albumId, albumName, photoCaption, photoLink);
                TaggedPhotoInfo tp = new TaggedPhotoInfo(p);

                try (Statement stmt3 =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

                ResultSet rst3 = stmt3.executeQuery("select u.user_id, u.first_name, u.last_name from " + userTableName + " u, " +
                        tagTableName + " t where t.tag_subject_id=u.user_id and t.tag_photo_id=" + photoId);

                while(rst3.next()) {
                    tp.addTaggedUser(new UserInfo(rst3.getLong(1), rst3.getString(2), rst3.getString(3)));
                }
                this.photosWithMostTags.add(tp);
                ++count;
                rst2.close();
                rst3.close();
            }
            }}

            rst.close();
            stmt.close();
            
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
    }

    @Override
    // **** Query 5 ****
    // Find suggested "match pairs" of users, using the following criteria:
    // (1) Both users should be of the same gender
    // (2) They should be tagged together in at least one photo (They do not have to be friends of the same person)
    // (3) Their age difference is <= yearDiff (just compare the years of birth for this)
    // (4) They are not friends with one another
    //
    // You should return up to n "match pairs"
    // If there are more than n match pairs, you should break ties as follows:
    // (i) First choose the pairs with the largest number of shared photos
    // (ii) If there are still ties, choose the pair with the smaller user1_id
    // (iii) If there are still ties, choose the pair with the smaller user2_id
    //
    public void matchMaker(int n, int yearDiff) {

        try (Statement stmt =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

            stmt.executeQuery("create view users2(u1, u2) as select u1.user_id, u2.user_id from " +
                                              userTableName + " u1, " + userTableName +
                                              " u2 where u1.user_id != u2.user_id and " +
                                              "(u1.gender = u2.gender) and " +
                                              "abs(u1.year_of_birth - u2.year_of_birth) <= " + yearDiff);


            try (Statement stmt2 = 
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)) {

            stmt2.executeQuery("create view not_friends2(u1,u2) as select u.u1, u.u2 from users2 u minus " +
                "(select u.user_id, u2.user_id from " + friendsTableName + " f inner join users2 u3 on f.user1_id=u3.u1 inner join " + userTableName + 
                " u on f.user1_id=u.user_id inner join " + userTableName + " u2 on f.user2_id=u2.user_id where f.user2_id=u3.u2)");

            try (Statement stmt7 =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

            stmt7.executeQuery("drop view users2");

            try (Statement stmt3 = 
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst3 = stmt3.executeQuery("select t1.tag_photo_id, t1.tag_subject_id, t2.tag_subject_id, count(*) from " +
                                 tagTableName + " t1, " + tagTableName +
                                " t2, not_friends2 f where t1.tag_subject_id = f.u1 and t2.tag_subject_id = " +
                                "f.u2 and t1.tag_photo_id = t2.tag_photo_id group by t1.tag_photo_id, t1.tag_subject_id, t2.tag_subject_id order by 4 desc");
            int count = 0;
            while(rst3.next() && count++ < n) {
                Long u1UserId = rst3.getLong(2);
                Long u2UserId = rst3.getLong(3);
                String sharedPhotoId = rst3.getString(1);

                try (Statement stmt4 = 
                    oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

                ResultSet rst4 = stmt4.executeQuery("select a.album_id, a.album_name, p.photo_caption, p.photo_link from " +
                                albumTableName + " a, " + photoTableName + " p where p.photo_id = " + sharedPhotoId + " and p.album_id = a.album_id");
                rst4.next();

                try (Statement stmt5 = 
                    oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

                ResultSet rst5 = stmt5.executeQuery("select u.first_name, u.last_name, u.year_of_birth, u2.first_name, u2.last_name, u2.year_of_birth from " + 
                    userTableName + " u, " + userTableName + " u2 where u.user_id=" + u1UserId + " and u2.user_id=" + u2UserId);
                rst5.next();
                
                String u1FirstName = rst5.getString(1);
                String u1LastName = rst5.getString(2);
                int u1Year = rst5.getInt(3);
                String u2FirstName = rst5.getString(4);
                String u2LastName = rst5.getString(5);
                int u2Year = rst5.getInt(6);

                MatchPair mp = new MatchPair(u1UserId, u1FirstName, u1LastName,
                    u1Year, u2UserId, u2FirstName, u2LastName, u2Year);

                String sharedPhotoAlbumId = rst4.getString(1);
                String sharedPhotoAlbumName = rst4.getString(2);
                String sharedPhotoCaption = rst4.getString(3);
                String sharedPhotoLink = rst4.getString(4);

                mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId,
                sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));

                this.bestMatches.add(mp);
            }}}

            try (Statement stmt6 =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

            stmt6.executeQuery("drop view not_friends2");

            stmt7.close();
            stmt6.close();
            stmt3.close();
            stmt2.close();
            stmt.close();
            rst3.close();
            
        }}}}} catch (SQLException err) {
            System.err.println(err.getMessage());
        }

        /*
        Long u1UserId = 123L;
        String u1FirstName = "u1FirstName";
        String u1LastName = "u1LastName";
        int u1Year = 1988;
        Long u2UserId = 456L;
        String u2FirstName = "u2FirstName";
        String u2LastName = "u2LastName";
        int u2Year = 1986;
        MatchPair mp = new MatchPair(u1UserId, u1FirstName, u1LastName,
                u1Year, u2UserId, u2FirstName, u2LastName, u2Year);
        String sharedPhotoId = "12345678";
        String sharedPhotoAlbumId = "123456789";
        String sharedPhotoAlbumName = "albumName";
        String sharedPhotoCaption = "caption";
        String sharedPhotoLink = "link";
        mp.addSharedPhoto(new PhotoInfo(sharedPhotoId, sharedPhotoAlbumId,
                sharedPhotoAlbumName, sharedPhotoCaption, sharedPhotoLink));
        this.bestMatches.add(mp);
        
*/
    }

    // **** Query 6 ****
    // Suggest users based on mutual friends
    //
    // Find the top n pairs of users in the database who have the most
    // common friends, but are not friends themselves.
    //
    // Your output will consist of a set of pairs (user1_id, user2_id)
    // No pair should appear in the result twice; you should always order the pairs so that
    // user1_id < user2_id
    //
    // If there are ties, you should give priority to the pair with the smaller user1_id.
    // If there are still ties, give priority to the pair with the smaller user2_id.
    //
    @Override
    public void suggestFriendsByMutualFriends(int n) {
   
        try (Statement stmt =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

        stmt.executeQuery("create view mutual(u1, u2, u3) as (select f.user1_id, f2.user1_id, f2.user2_id from " + 
            friendsTableName + " f inner join " + friendsTableName + " f2 on f.user2_id=f2.user2_id where f.user1_id < f2.user1_id) union " +
            "(select f.user1_id, f2.user2_id, f2.user1_id from " + friendsTableName + " f inner join " + 
            friendsTableName + " f2 on f2.user1_id=f.user2_id where f.user1_id < f2.user2_id) union " +
            "(select f.user2_id, f2.user1_id, f2.user2_id from " + friendsTableName + " f inner join " + 
            friendsTableName + " f2 on f2.user2_id=f.user1_id where f.user2_id < f2.user1_id) union " +
            "(select f.user2_id, f2.user2_id, f2.user1_id from " + friendsTableName + " f inner join " + 
            friendsTableName + " f2 on f2.user1_id=f.user1_id where f.user2_id < f2.user2_id)");

        try (Statement stmt2 =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

        ResultSet rst = stmt2.executeQuery("select m.u1, m.u2, count(*) from mutual m group by m.u1, m.u2 order by 3 desc");

        int count = 0;
        while (rst.next() && count++ < n) {
            Long user1_id = rst.getLong(1);
            Long user2_id = rst.getLong(2);

            try (Statement stmt3 =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst2 = stmt3.executeQuery("select u.first_name, u.last_name from " + 
                userTableName + " u where " + user1_id + "=u.user_id");
            rst2.next();

            String user1FirstName = rst2.getString(1);
            String user1LastName = rst2.getString(2);

            try (Statement stmt4 =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst3 = stmt4.executeQuery("select u.first_name, u.last_name from " + 
                userTableName + " u where " + user2_id + "=u.user_id");
            rst3.next();
            
            String user2FirstName = rst3.getString(1);
            String user2LastName = rst3.getString(2);

            UsersPair p = new UsersPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

            try (Statement stmt5 =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst4 = stmt5.executeQuery("select u.user_id, u.first_name, u.last_name from " + 
                userTableName + " u, mutual m where m.u3=u.user_id and m.u1=" + user1_id + " and m.u2=" + user2_id);

            while (rst4.next()) {
                p.addSharedFriend(rst4.getLong(1), rst4.getString(2), rst4.getString(3));
            }
            this.suggestedUsersPairs.add(p);
        }}}}

        try (Statement stmt6 =
            oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY)) {

        stmt6.executeQuery("drop view mutual");

        stmt6.close();
        stmt.close();
        stmt2.close();
        rst.close();

        }}} catch (SQLException err) {
            System.err.println(err.getMessage());
        }
        /*Long user1_id = 123L;
        String user1FirstName = "User1FirstName";
        String user1LastName = "User1LastName";
        Long user2_id = 456L;
        String user2FirstName = "User2FirstName";
        String user2LastName = "User2LastName";
        UsersPair p = new UsersPair(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);

        p.addSharedFriend(567L, "sharedFriend1FirstName", "sharedFriend1LastName");
        p.addSharedFriend(678L, "sharedFriend2FirstName", "sharedFriend2LastName");
        p.addSharedFriend(789L, "sharedFriend3FirstName", "sharedFriend3LastName");
        this.suggestedUsersPairs.add(p);*/
    }

    @Override
    // ***** Query 7 *****
    //
    // Find the name of the state with the most events, as well as the number of
    // events in that state.  If there is a tie, return the names of all of the (tied) states.
    //
    public void findEventStates() {
        
        
        try (Statement stmt =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            
            ResultSet rst = stmt.executeQuery("select c.state_name, count(*) from " + cityTableName + " c inner join "
                    + eventTableName + " e on c.city_id = e.event_city_id group by c.state_name order by 2 DESC");
            
            int first_count = 0;
            while(rst.next()) {
                int current_count = rst.getInt(2);
                String state = rst.getString(1);
                if(rst.isFirst()) {
                    first_count = rst.getInt(2);
                    this.eventCount = first_count;
                    this.popularStateNames.add(state);
                } else if(current_count == first_count) {
                    this.popularStateNames.add(state);
                }
            }
            
            rst.close();
            stmt.close();
            
            
        } catch (SQLException err) {
            System.err.println(err.getMessage());
        }
        
            
    
}

    //@Override
    // ***** Query 8 *****
    // Given the ID of a user, find information about that
    // user's oldest friend and youngest friend
    //
    // If two users have exactly the same age, meaning that they were born
    // on the same day, then assume that the one with the larger user_id is older
    //
    public void findAgeInfo(Long user_id) {
        
          
        try (Statement stmt =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
            
            ResultSet rst = stmt.executeQuery("select u.user_id, u.first_name, u.last_name from " +
                                             userTableName + " u, " + friendsTableName + " f " +
                                             "where ((f.user1_id = " + user_id + " and f.user2_id = u.user_id) or (f.user2_id = " + user_id + " and f.user1_id = u.user_id)) order by u.year_of_birth, u.month_of_birth, u.day_of_birth, u.user_id DESC");

            //oldest
            while(rst.next()) {
                if(rst.isFirst()) {
                    Long first = rst.getLong(1);
                    String name = rst.getString(2);
                    String last = rst.getString(3);
                    this.oldestFriend = new UserInfo(first, name, last);
                }

            }
        
            rst = stmt.executeQuery("select u.user_id, u.first_name, u.last_name from " +
                                             userTableName + " u, " + friendsTableName + " f " +
                                             "where ((f.user1_id = " + user_id + " and f.user2_id = u.user_id) or (f.user2_id = " + user_id + " and f.user1_id = u.user_id)) order by u.year_of_birth DESC, u.month_of_birth DESC, u.day_of_birth DESC, u.user_id");


            while(rst.next()) {
                if(rst.isFirst()) {
                    Long first = rst.getLong(1);
                    String name = rst.getString(2);
                    String last = rst.getString(3);
                    this.youngestFriend = new UserInfo(first, name, last);
                }
            

            }

            
            rst.close();
            stmt.close();
            
        }
        catch (SQLException err) {
            System.err.println(err.getMessage());
        }
        
            
 }
    @Override
    //   ***** Query 9 *****
    //
    // Find pairs of potential siblings.
    //
    // A pair of users are potential siblings if they have the same last name and hometown, if they are friends, and
    // if they are less than 10 years apart in age.  Pairs of siblings are returned with the lower user_id user first
    // on the line.  They are ordered based on the first user_id and in the event of a tie, the second user_id.
    //
    //
    public void findPotentialSiblings() {

        try (Statement stmt =
                oracleConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {

            ResultSet rst = stmt.executeQuery("select u1.user_id, u1.first_name, u1.last_name, u2.user_id, u2.first_name, u2.last_name from " +
                                              userTableName + " u1, " + userTableName + " u2, " + hometownCityTableName + " h1, " + 
                                              hometownCityTableName + " h2, " + friendsTableName + " f where (u1.user_id = h1.user_id and u2.user_id = h2.user_id and h1.hometown_city_id = h2.hometown_city_id and u1.last_name = u2.last_name and (abs(u1.year_of_birth - u2.year_of_birth) < 10) and ((f.user1_id = u1.user_id and f.user2_id = u2.user_id))) order by u1.user_id, u2.user_id");
                                 
            while(rst.next()) {
                Long user1_id = rst.getLong(1);
                String user1FirstName = rst.getString(2);
                String user1LastName = rst.getString(3);
                Long user2_id = rst.getLong(4);
                String user2FirstName = rst.getString(5);
                String user2LastName = rst.getString(6);
                SiblingInfo s;
                if (user1_id < user2_id) {
                    s = new SiblingInfo(user1_id, user1FirstName, user1LastName, user2_id, user2FirstName, user2LastName);
                } else {
                    s = new SiblingInfo(user2_id, user2FirstName, user2LastName, user1_id, user1FirstName, user1LastName);
                }
            
                this.siblings.add(s);
            }


            rst.close();
            stmt.close();

        }
        catch (SQLException err) {
            System.err.println(err.getMessage());
        }

    }

}
