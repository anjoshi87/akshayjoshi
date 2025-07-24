package com.kantar.sessionsjob;


public class Session {

    //HomeNo|Channel|Starttime|Activity
    private String homeNo;
    private String channel;
    private String StartTime;
    private String activity;

    public String getHomeNo() {
        return homeNo;
    }

    public void setHomeNo(String homeNo) {
        this.homeNo = homeNo;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getStartTime() {
        return StartTime;
    }

    public void setStartTime(String startTime) {
        StartTime = startTime;
    }

    public String getActivity() {
        return activity;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }


    @Override
    public String toString() {
        return "Session{" +
                "homeNo='" + homeNo + '\'' +
                ", channel='" + channel + '\'' +
                ", StartTime='" + StartTime + '\'' +
                ", activity='" + activity + '\'' +
                '}';
    }
}
