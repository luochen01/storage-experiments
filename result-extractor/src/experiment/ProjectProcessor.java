package experiment;

import java.io.File;

public class ProjectProcessor {

    public static String[] names = new String[] { ".txt", ".pdf", ".inc", ".cc", ".c", ".cpp", ".h", "makefile" };

    public static void main(String[] args) {
        String path = "/Users/luochen/Desktop/cs222p";

        File dir = new File(path);
        check(dir);

    }

    public static void check(File file) {
        if (file.isDirectory()) {
            File[] files = file.listFiles();
            for (File sub : files) {
               // System.out.println("Enter " + sub.getAbsolutePath());
                check(sub);
            }
            if (files.length == 0) {
                System.out.println("Delete folder " + file.getPath());
                file.delete();

            }
        } else {
            String name = file.getName();
            boolean keep = false;
            for (String suffix : names) {
                if (name.endsWith(suffix)) {
                    keep = true;
                    break;
                }
            }
            if (!keep) {
                file.delete();
                System.out.println("Deleted " + name);
            }
        }

    }

}
