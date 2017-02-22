package funv;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

import model.CmdResult;

/**
 * Created by kingdee on 2017/1/12.
 */

    public class SupportTest {

        public CmdResult execute(ArrayList<String> cmdList)  {
            CmdResult result = new CmdResult();
            //DataImportCount dataImportCount = new DataImportCount();
            StringBuffer sb = new StringBuffer();
            boolean flag = false;
            try {
                ProcessBuilder pb = new ProcessBuilder();

                //System.out.println(cmd);
                //Process process = Runtime.getRuntime().exec( cmd);
                Process process = pb.command(cmdList).start();
                InputStream is = process.getInputStream();
                InputStream errIs = process.getErrorStream();
                BufferedReader bis = new BufferedReader( new InputStreamReader( is));
                BufferedReader errBis = new BufferedReader( new InputStreamReader(errIs));
                String line = null;
                String count = null;
                String size = null;
                while( (line = errBis.readLine()) != null) {
                    sb.append(line).append( "\n");
                    System.out.println( line);
                }
                int isExit = process.waitFor();
                int excode = process.exitValue();
                if ( isExit == 0 && excode == 0) {
                    result.setExitCode( 0);
                    result.setOutput( sb.toString());
                }
            } catch ( Exception e) {
                e.printStackTrace();
                result.setExitCode( 1);
                result.setOutput( e.getMessage());
            }
            return result;
        }

    }


