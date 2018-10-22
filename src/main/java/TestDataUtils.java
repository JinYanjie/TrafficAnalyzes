import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TestDataUtils {
    static double DEF_PI = 3.14159265359; // PI
    static double DEF_2PI= 6.28318530712; // 2*PI
    static double DEF_PI180= 0.01745329252; // PI/180.0
    static double DEF_R =6370693.5; // radius of earth

    public static double getSubHour(String t1, String t2) {
        long subTimes = Math.abs(getTimeMills(t1) - getTimeMills(t2));
//        转换成小时
        float subHour = (float)subTimes / (1000 * 60 * 60);
        DecimalFormat decimalFormat=new DecimalFormat("#0.0000");
        String format = decimalFormat.format(subHour);

        return Double.valueOf(format);
    }

    public static long getTimeMills(String time) {
        long times = 0;
        try {
            DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            Date parse = dateFormat.parse(time);
            times = parse.getTime();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return times;
    }

    //适用于远距离
    public static double getLongDistance(double lon1, double lat1, double lon2, double lat2)
    {
        double ew1, ns1, ew2, ns2;
        double distance;
        // 角度转换为弧度
        ew1 = lon1 * DEF_PI180;
        ns1 = lat1 * DEF_PI180;
        ew2 = lon2 * DEF_PI180;
        ns2 = lat2 * DEF_PI180;
        // 求大圆劣弧与球心所夹的角(弧度)
        distance = Math.sin(ns1) * Math.sin(ns2) + Math.cos(ns1) * Math.cos(ns2) * Math.cos(ew1 - ew2);
        // 调整到[-1..1]范围内，避免溢出
        if (distance > 1.0)
            distance = 1.0;
        else if (distance < -1.0)
            distance = -1.0;
        // 求大圆劣弧长度
        distance = DEF_R * Math.acos(distance);
        //四舍五入：单位KM
        return Math.round(distance/1000);
    }

    public static Integer getSpeed(double distance,double hour){
        if(hour==0){
            return 0;
        }
        return (int)(distance/hour);
    }

    public static double getSubminutes(String t1,String t2){
        long abs = Math.abs(getTimeMills(t1) - getTimeMills(t2));
        float minutes=(float) abs/(1000*60);
        DecimalFormat decimalFormat=new DecimalFormat("#0.00");
        String format = decimalFormat.format(minutes);
        return Double.valueOf(format);
    }

}
