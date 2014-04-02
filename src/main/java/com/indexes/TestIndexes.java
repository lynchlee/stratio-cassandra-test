package com.indexes;

/**
 * Created by Jcalderin on 20/03/14.
 */

import com.indexes.TestCases.TestsConsults.TestsWildcards;
import com.indexes.Utils.MainUtils;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

public class TestIndexes {
    public static String cassandra = "stratiojon.stratio.com";
    public static boolean debug = false;
    public static boolean cassOps = true;


    public static void main(String args[])
    {
        MainUtils mu = new MainUtils();
        try {
            report(JUnitCore.runClasses(TestsWildcards.class));
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (MainUtils.opsToCassandra != null && !MainUtils.opsToCassandra.isEmpty())
        {
            System.out.println("********************************Operaciones a Cassaandra*************************************************");
            for (String s : MainUtils.opsToCassandra)
            {
                if (cassOps)
                {
                    System.out.println(s);
                }
            }
        }
        System.exit(0);
    }

    public static void report(Result result)
    {
        int totalRun = result.getRunCount();
        int totalFail = result.getFailureCount();
        int totalPass = totalRun - totalFail;
        System.out.println("************Report***************");
        System.out.println("Número de casos: " + totalRun);
        System.out.println("Número de casos ok: " + totalPass);
        System.out.println("Número de casos fallidos: " + totalFail);

        for (Failure failure: result.getFailures()){
            System.out.println(failure.toString());
        }
    }

}
