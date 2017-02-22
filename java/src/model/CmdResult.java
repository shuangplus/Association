package model;

/**
 * Created by kingdee on 2017/1/12.
 */
public class CmdResult {
    private int exitCode;
    private String output;
    private String errOutput;

    public int getExitCode() {
        return exitCode;
    }

    public void setExitCode(int exitCode) {
        this.exitCode = exitCode;
    }

    public String getOutput() {
        return output;
    }

    public void setOutput(String output) {
        this.output = output;
    }

    public String getErrOutput() {
        return errOutput;
    }

    public void setErrOutput(String errOutput) {
        this.errOutput = errOutput;
    }
}
