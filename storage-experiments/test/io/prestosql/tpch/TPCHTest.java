package io.prestosql.tpch;

import java.util.Iterator;

import org.junit.Test;

import edu.uci.asterixdb.storage.tpch.gen.LineItem;
import edu.uci.asterixdb.storage.tpch.gen.LineItemGenerator;

public class TPCHTest {

    @Test
    public void test() {
        LineItemGenerator gen = new LineItemGenerator(1, 1, 1000);
        Iterator<LineItem> it = gen.iterator();

        while (it.hasNext()) {
            LineItem obj = it.next();
            System.out.println(obj.toLine());
        }
    }
}
