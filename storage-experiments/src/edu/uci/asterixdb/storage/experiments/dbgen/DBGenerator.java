package edu.uci.asterixdb.storage.experiments.dbgen;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import com.github.javafaker.Faker;

class Course {
    String dept;
    String cno;
    String title;
    String level;

    List<String> classNos = new ArrayList<>();

    public Course(String dept, String cno, String title, String level) {
        super();
        this.dept = dept;
        this.cno = cno;
        this.title = title;
        this.level = level;
    }

}

public class DBGenerator {

    private static final String USER = "User";
    private static final String PHONE = "Phone";
    private static final String STUDENT = "Student";
    private static final String INSTRUCTOR = "Instructor";
    private static final String CLASS = "Class";
    private static final String OFFERS = "Offers";
    private static final String TAKES = "Takes";
    private static final String ASSIGNMENT = "Assignment";
    private static final String DOES = "Does";
    private static final String POST = "Post";
    private static final String LIKES = "Likes";

    private static final String Topic = "Topic";

    private final Faker faker = new Faker();
    private final Connection conn;
    private final Statement stmt;

    private final List<String> userIds = new ArrayList<>();
    private final Set<String> studentIds = new HashSet<>();
    private final Set<String> instructorIds = new HashSet<>();
    private final Map<String, List<String>> instructorOffers = new HashMap<>();
    private final Map<String, List<String>> studentTakes = new HashMap<>();
    private final Map<String, List<String>> classAssignments = new HashMap<>();
    private final Map<String, List<String>> classPosts = new HashMap<>();

    private final String[] majors = new String[] { "CS", "EE", "ART", "MATH" };

    private final String[] instructorTitles = new String[] { "Professor", "Reader", "TA" };

    private final Random rand = new Random(17);

    private final List<Course> courses = new ArrayList<>();

    public DBGenerator() throws Exception {
        conn = connectToDB();
        stmt = conn.createStatement();
    }

    public void close() throws Exception {
        conn.close();
        stmt.close();
    }

    public Connection connectToDB() throws Exception {
        Class.forName("com.mysql.jdbc.Driver").newInstance();
        return DriverManager.getConnection("jdbc:mysql://localhost/cs122a?user=root&password=root");
    }

    public void generateUsers(int num) throws SQLException {
        delete(USER);

        for (int i = 0; i < num; i++) {
            String id = String.valueOf(i);
            String fname = faker.name().firstName().replaceAll("'", "");
            String lname = faker.name().lastName().replaceAll("'", "");
            String email = faker.internet().emailAddress(fname.toLowerCase() + "." + lname.toLowerCase());
            String insert = String.format("INSERT INTO User VALUES('%s','%s','%s','%s')", id, email, fname, lname);
            System.out.println(insert);
            stmt.executeUpdate(insert);
            userIds.add(id);
        }
    }

    public void generatePhones(int phones) throws SQLException {
        delete(PHONE);

        String[] phoneTypes = new String[] { "Mobile", "Office", "Home" };
        for (int i = 0; i < phones; i++) {
            String userid = userIds.get(rand.nextInt(userIds.size()));
            String type = phoneTypes[rand.nextInt(phoneTypes.length)];
            String phone = faker.phoneNumber().cellPhone();

            String insert = String.format("INSERT INTO Phone VALUES ('%s', '%s', '%s')", userid, type, phone);
            System.out.println(insert);
            try {
                stmt.executeUpdate(insert);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void generateStudents(int numStudents) throws SQLException {
        delete(STUDENT);
        for (int i = 0; i < numStudents; i++) {
            String userId = userIds.get(rand.nextInt(userIds.size()));
            while (studentIds.contains(userId)) {
                userId = userIds.get(rand.nextInt(userIds.size()));
            }
            String major = majors[rand.nextInt(majors.length)];

            String insert = String.format("INSERT INTO Student VALUES ('%s', '%s')", userId, major);
            System.out.println(insert);
            stmt.executeUpdate(insert);
            studentIds.add(userId);
        }
    }

    public void generateInstructors(int numInstructors) throws SQLException {
        delete(INSTRUCTOR);

        instructorIds.addAll(userIds);
        instructorIds.removeAll(studentIds);
        Iterator<String> it = instructorIds.iterator();
        int i = 0;
        while (it.hasNext() && i++ < numInstructors) {
            String id = it.next();
            String title = instructorTitles[rand.nextInt(instructorTitles.length)];
            String insert = String.format("INSERT INTO Instructor VALUES ('%s', '%s')", id, title);
            System.out.println(insert);
            stmt.executeUpdate(insert);
        }
    }

    public void loadCourses() throws SQLException {
        ResultSet rs = stmt.executeQuery("select * from Course");
        while (rs.next()) {
            String dept = rs.getString(1);
            String cno = rs.getString(2);
            String title = rs.getString(3);
            String level = rs.getString(4);
            courses.add(new Course(dept, cno, title, level));
        }
        rs.close();
    }

    public void generateClasses(int numClasses) throws Exception {
        delete(CLASS);
        String[] quarters = new String[] { "Spring", "Fall", "Winter", "Summer" };
        int[] maxStudents = new int[] { 40, 50, 100, 150, 200 };
        int[] years = new int[] { 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018 };
        Set<String> set = new HashSet<>();
        for (int i = 0; i < numClasses;) {
            Course course = courses.get(rand.nextInt(courses.size()));
            int maxStudent = maxStudents[rand.nextInt(maxStudents.length)];
            String quarter = quarters[rand.nextInt(quarters.length)];
            int year = years[rand.nextInt(years.length)];
            String id = course.cno + course.dept + year + quarter;
            if (!set.add(id)) {
                continue;
            }
            String class_no = String.valueOf(i);
            course.classNos.add(class_no);
            Date begin = getBeginTime(year, quarter);
            Date end = getEndTime(year, quarter);
            String insert = String.format("INSERT INTO Class VALUES ('%s', '%s', '%s', '%s', '%s', '%d', '%s', '%d')",
                    class_no, course.dept, course.cno, begin, end, maxStudent, quarter, year);
            System.out.println(insert);
            stmt.executeUpdate(insert);
            i++;
        }
    }

    public void generateOffers(int maxInstructorsPerCourse, int maxCoursesPerInstructor) throws SQLException {
        delete(OFFERS);
        Map<String, Integer> map = new HashMap<>();
        for (String id : instructorIds) {
            int n = rand.nextInt(maxCoursesPerInstructor);
            if (n == 0) {
                continue;
            }
            Set<Course> teachCourses = sample(courses, n);
            for (Course course : teachCourses) {
                for (String classno : course.classNos) {
                    if (addToMap(map, classno) <= maxInstructorsPerCourse) {
                        String insert = String.format("INSERT INTO Offers VALUES ('%s', '%s', '%s', '%s')", id, classno,
                                course.dept, course.cno);
                        System.out.println(insert);
                        stmt.executeUpdate(insert);
                        addToMap(instructorOffers, id, classno);
                    }
                }
            }
        }
    }

    public void generateTakes(int maxCoursesPerStudent) throws SQLException {
        delete(TAKES);
        for (String id : userIds) {
            int numCourses = rand.nextInt(maxCoursesPerStudent);
            Set<Course> courses = sample(this.courses, numCourses);
            for (Course course : courses) {
                if (course.classNos.size() > 0) {
                    String classNo = sample(course.classNos);
                    int grade = rand.nextInt(40) + 60;
                    String insert = String.format("INSERT INTO Takes VALUES ('%s', '%s', '%s', '%s', '%d')", id,
                            classNo, course.dept, course.cno, grade);
                    System.out.println(insert);
                    stmt.executeUpdate(insert);

                    addToMap(studentTakes, id, classNo);
                }
            }
        }
    }

    public void generateAssignments(int maxAssignmentsPerClass) throws SQLException {
        delete(ASSIGNMENT);
        int globalId = 0;
        for (Course course : courses) {
            for (String classNo : course.classNos) {
                int numAssignments = rand.nextInt(maxAssignmentsPerClass);
                for (int i = 0; i < numAssignments; i++) {
                    String assignId = String.valueOf(globalId++);
                    String name = "Assignment " + i;
                    String insert = String.format("INSERT INTO Assignment VALUES ('%s', '%s', '%s', '%s', '%s', NULL)",
                            assignId, classNo, course.dept, course.cno, name);
                    System.out.println(insert);
                    stmt.executeUpdate(insert);

                    addToMap(classAssignments, classNo, assignId);
                }
            }
        }
    }

    public void generateDoes(int maxAssignmentsPerStudent) throws SQLException {
        delete(DOES);
        for (String student : studentIds) {
            List<String> classes = studentTakes.get(student);
            if (classes == null || classes.isEmpty()) {
                continue;
            }
            for (String classno : classes) {
                List<String> assignments = classAssignments.get(classno);
                if (assignments == null || assignments.isEmpty()) {
                    continue;
                }
                int numAssignments = Math.min(assignments.size(), rand.nextInt(maxAssignmentsPerStudent));
                Set<String> didAssignments = sample(assignments, numAssignments);
                for (String assignmentNo : didAssignments) {
                    int grade = rand.nextInt(50) + 50;
                    String insert =
                            String.format("INSERT INTO Does VALUES ('%s', '%s', '%d')", student, assignmentNo, grade);
                    System.out.println(insert);
                    stmt.executeUpdate(insert);
                }

            }
        }
    }

    public void generatePost(int maxPostsPerClass, int maxLikesPerClass, int maxTopicsPerPost) throws SQLException {
        delete(POST);
        delete(LIKES);
        delete(Topic);
        int globalId = 0;
        String[] categories = new String[] { "Announcement", "Question", "Comment" };
        List<String> topics = Arrays.asList(new String[] { "Homework", "Logistics", "Exams", "Lectures", "Other" });
        for (Course course : courses) {
            for (String classNo : course.classNos) {
                List<String> students = searchValue(studentTakes, classNo);
                List<String> instructors = searchValue(instructorOffers, classNo);
                List<String> assignments = classAssignments.get(classNo);
                if (students.isEmpty() || instructors.isEmpty() || assignments == null || assignments.isEmpty()) {
                    continue;
                }
                int numPosts = rand.nextInt(maxPostsPerClass);
                if (numPosts == 0) {
                    continue;
                }
                List<String> postIds = new ArrayList<>();
                for (int i = 0; i < numPosts; i++) {
                    String postId = String.valueOf(globalId++);
                    String category = categories[rand.nextInt(categories.length)];
                    String replyId = null;
                    String userId = null;
                    if (category.equals("Announcement")) {
                        userId = sample(instructors);
                    } else if (category.equals("Question")) {
                        userId = sample(students);
                    } else if (category.equals("Comment")) {
                        userId = rand.nextBoolean() ? sample(instructors) : sample(students);
                        if (!postIds.isEmpty()) {
                            replyId = sample(postIds);
                        }
                    }
                    String assignId = sample(assignments);
                    String content = faker.lorem().sentence(10, 20);
                    int popularity = rand.nextInt(100);
                    String insert =
                            String.format("INSERT INTO Post VALUES ('%s', '%s', '%s', '%s', '%s', '%s', NULL, '%d')",
                                    postId, replyId, userId, assignId, category, content, popularity);
                    System.out.println(insert);
                    stmt.executeUpdate(insert);

                    postIds.add(postId);
                    addToMap(classPosts, classNo, postId);

                    int numTopics = rand.nextInt(maxTopicsPerPost);
                    if (numTopics > 0) {
                        Set<String> postTopics = sample(topics, numTopics);
                        for (String topic : postTopics) {
                            insert = String.format("INSERT INTO Topic VALUES ('%s', '%s')", postId, topic);
                            System.out.println(insert);
                            stmt.executeUpdate(insert);
                        }
                    }
                }

                int numLikes = rand.nextInt(maxLikesPerClass);
                List<String> userIds = new ArrayList<>();
                userIds.addAll(studentIds);
                userIds.addAll(instructorIds);
                for (int i = 0; i < numLikes; i++) {
                    String postId = sample(postIds);
                    String userId = sample(userIds);
                    try {
                        String insert = String.format("INSERT INTO Likes VALUES ('%s', '%s')", userId, postId);
                        System.out.println(insert);
                        stmt.executeUpdate(insert);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            }
        }
    }

    private List<String> searchValue(Map<String, List<String>> map, String value) {
        List<String> list = new ArrayList<>();
        for (Entry<String, List<String>> e : map.entrySet()) {
            if (e.getValue().contains(value)) {
                list.add(e.getKey());
            }
        }
        return list;
    }

    private int addToMap(Map<String, Integer> map, String key) {
        if (map.containsKey(key)) {
            int count = map.get(key);
            count++;
            map.put(key, count);
            return count;
        } else {
            map.put(key, 1);
            return 1;
        }
    }

    private <T> Set<T> sample(List<T> objects, int n) {
        Set<T> results = new HashSet<>();
        while (results.size() < n) {
            results.add(sample(objects));
        }
        return results;
    }

    private <T> T sample(List<T> list) {
        return list.get(rand.nextInt(list.size()));
    }

    private Date getBeginTime(int year, String quarter) {
        year = year - 1900;
        switch (quarter) {
            case "Spring":
                return new Date(year, 3, 15);
            case "Summer":
                return new Date(year, 7, 1);
            case "Fall":
                return new Date(year, 9, 1);
            case "Winter":
                return new Date(year, 1, 1);
            default:
                return null;
        }
    }

    private Date getEndTime(int year, String quarter) {
        year -= 1900;
        switch (quarter) {
            case "Spring":
                return new Date(year, 6, 15);
            case "Summer":
                return new Date(year, 8, 25);
            case "Fall":
                return new Date(year, 12, 15);
            case "Winter":
                return new Date(year, 3, 1);
            default:
                return null;
        }
    }

    private void addToMap(Map<String, List<String>> map, String key, String value) {
        List<String> list = map.get(key);
        if (list == null) {
            list = new ArrayList<>();
            map.put(key, list);
        }
        list.add(value);
    }

    private void delete(String table) throws SQLException {
        stmt.execute("SET FOREIGN_KEY_CHECKS=0;");
        stmt.execute("delete from " + table);
    }

    public static void main(String[] agrs) throws Exception {
        DBGenerator gen = new DBGenerator();
        gen.generateUsers(2000);
        gen.generatePhones(3000);
        gen.generateStudents(1800);
        gen.generateInstructors(180);
        // courses already exist in db
        gen.loadCourses();
        gen.generateClasses(400);
        gen.generateOffers(5, 3);
        gen.generateTakes(5);
        gen.generateAssignments(5);
        gen.generateDoes(5);
        gen.generatePost(100, 300, 3);
    }
}
