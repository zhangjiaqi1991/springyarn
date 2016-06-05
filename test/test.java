import org.springframework.beans.factory.annotation.Value;

import java.io.File;

/**
 * Created by zjq on 16/5/19.
 */
public class test {

    public static void main(String args[]){
        File file=new File("/data/tweets.dat");
        System.out.println(file.getAbsolutePath());
    }
}
